"""Batch layer: one Airflow DAG run reconciles one completed simulated day.

determine_target_date -> wait_for_cost_file (FileSensor) -> load_and_validate
-> compute_profitability_and_report -> mark_run_success
                                    \-> (any upstream failure) on_failure_alert

Idempotent: compute_profitability_and_report upserts daily_profitability on
(vehicle_id, cost_date), so re-running the DAG for a day that was already
processed produces the same rows, not duplicates.

Import note: this file runs inside the Airflow containers, which mount the
repo's common/ package at /opt/airflow/common (see docker-compose.yml) with
PYTHONPATH=/opt/airflow, so `from common...` resolves exactly like it does
for the simulators and the Spark job -- one shared implementation of
validation, zones and profit math across every layer.
"""
import glob
import math
import os
import re
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule

from common import config
from common.logging_utils import get_logger
from common.revenue import compute_margin, compute_profit, compute_profit_per_km, is_unprofitable
from common.validation import validate_cost_row

log = get_logger("batch.airflow_dag")


def _conn():
    return psycopg2.connect(config.postgres_dsn())


def _safe_float(value):
    """float(value), or None if it isn't a finite number. Used when writing to
    a NUMERIC column so a validation-rejected row's raw text (or NaN/Infinity
    -- Postgres NUMERIC accepts those, jsonable_encoder later does not) never
    reaches the database as anything other than NULL."""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def determine_target_date(**context):
    """Pick the oldest cost file sitting in the landing folder that has not
    yet been successfully processed. Deliberately does NOT assume the first
    target date is SIM_BASE_DATE: because the simulated clock is anchored to
    wall-clock time (see common/sim_clock.py), the very first cost file the
    simulator ever writes can already be many simulated "days" past the
    base date (whatever simulated day-cycle was in progress when the
    simulator started) -- so we look at what files actually exist instead.
    """
    pattern = os.path.join(config.LANDING_DIR, "costs_*.csv")
    available_dates = set()
    for path in glob.glob(pattern):
        m = re.search(r"costs_(\d{4}-\d{2}-\d{2})\.csv$", os.path.basename(path))
        if m:
            available_dates.add(m.group(1))

    with _conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT cost_date FROM batch_job_runs WHERE status = 'success'")
        processed = {row[0].isoformat() for row in cur.fetchall()}

    pending = sorted(available_dates - processed)
    if not pending:
        log.info("no_pending_cost_files", available=len(available_dates), processed=len(processed))
        raise AirflowSkipException("No new cost file to process yet.")

    target_str = pending[0]

    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO batch_job_runs (cost_date, status, started_at) VALUES (%s, 'running', now())",
            (target_str,),
        )
        conn.commit()

    log.info("target_date_determined", cost_date=target_str)
    context["ti"].xcom_push(key="target_date", value=target_str)
    return target_str


def cost_file_exists(**context) -> bool:
    """Sensor predicate: is today's target cost file sitting in the landing
    folder yet? Plain os.path check (not FileSensor) so this doesn't depend
    on an Airflow Connection object being pre-registered."""
    target_date = context["ti"].xcom_pull(task_ids="determine_target_date", key="target_date")
    path = os.path.join(config.LANDING_DIR, f"costs_{target_date}.csv")
    found = os.path.isfile(path)
    log.info("polling_for_cost_file", path=path, found=found)
    return found


def load_and_validate(**context):
    target_date = context["ti"].xcom_pull(task_ids="determine_target_date", key="target_date")
    file_path = os.path.join(config.LANDING_DIR, f"costs_{target_date}.csv")

    # keep_default_na=False: pandas' default NA-token list includes "N/A", "NULL",
    # "nan", etc. -- exactly the kind of text cost_simulator.py's corruption
    # injects to test validation. Without this, pandas silently turns that text
    # into a real NaN *before* validate_cost_row ever sees it, and float(nan)
    # doesn't raise, so a poisoned row would sail through as "valid" (see
    # common/validation.py's _is_bad_float for the matching defensive guard).
    df = pd.read_csv(file_path, keep_default_na=False, na_values=[])
    rows_loaded, rows_rejected = 0, 0

    with _conn() as conn, conn.cursor() as cur:
        for _, row in df.iterrows():
            record = row.to_dict()
            ok, reason = validate_cost_row(record)
            if ok:
                rows_loaded += 1
            else:
                rows_rejected += 1
                log.warning("cost_row_rejected", vehicle_id=record.get("vehicle_id"),
                            reason=reason, raw_row=record)

            # cost_staging's numeric columns are NUMERIC in Postgres -- an invalid
            # row's raw value (e.g. the literal text "N/A" a corrupted CSV cell
            # can contain) is not castable to NUMERIC and would raise a hard DB
            # error rather than a graceful validation rejection, so store NULL
            # for whichever field(s) don't actually parse as a number instead of
            # the raw un-parseable value. The reason/is_valid columns already
            # record *why* the row was rejected; the raw value is in the log line
            # above for debugging.
            cur.execute(
                """
                INSERT INTO cost_staging (vehicle_id, fuel_cost, maintenance_cost, distance_covered,
                                           service_flag, cost_date, is_valid, reject_reason)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    record.get("vehicle_id"),
                    _safe_float(record.get("fuel_cost")),
                    _safe_float(record.get("maintenance_cost")),
                    _safe_float(record.get("distance_covered")),
                    bool(record.get("service_flag")) if ok else None,
                    target_date,
                    ok,
                    reason,
                ),
            )
        conn.commit()

    log.info("cost_file_loaded", cost_date=target_date, rows_loaded=rows_loaded, rows_rejected=rows_rejected)
    context["ti"].xcom_push(key="rows_loaded", value=rows_loaded)
    context["ti"].xcom_push(key="rows_rejected", value=rows_rejected)
    return rows_loaded


def _read_trips_for_day(target_date: str) -> pd.DataFrame:
    partition_dir = os.path.join(config.PARQUET_DIR, "trips", f"sim_date={target_date}")
    if not os.path.isdir(partition_dir):
        return pd.DataFrame(columns=["vehicle_id", "trip_id", "fare"])
    return pd.read_parquet(partition_dir, engine="pyarrow")


def compute_profitability_and_report(**context):
    target_date = context["ti"].xcom_pull(task_ids="determine_target_date", key="target_date")

    trips = _read_trips_for_day(target_date)
    if not trips.empty:
        trip_starts = trips[trips["fare"] > 0]
        earnings_by_vehicle = trip_starts.groupby("vehicle_id").agg(
            earnings=("fare", "sum"), trips_count=("trip_id", "nunique")
        ).reset_index()
    else:
        earnings_by_vehicle = pd.DataFrame(columns=["vehicle_id", "earnings", "trips_count"])

    with _conn() as conn:
        costs = pd.read_sql(
            "SELECT vehicle_id, fuel_cost, maintenance_cost, distance_covered, service_flag "
            "FROM cost_staging WHERE cost_date = %(d)s AND is_valid = true",
            conn, params={"d": target_date},
        )

    merged = costs.merge(earnings_by_vehicle, on="vehicle_id", how="left")
    merged["earnings"] = merged["earnings"].fillna(0.0)
    merged["trips_count"] = merged["trips_count"].fillna(0).astype(int)

    records = []
    for _, r in merged.iterrows():
        profit = compute_profit(float(r["earnings"]), float(r["fuel_cost"]), float(r["maintenance_cost"]))
        records.append({
            "vehicle_id": r["vehicle_id"],
            "cost_date": target_date,
            "trips_count": int(r["trips_count"]),
            "earnings": round(float(r["earnings"]), 2),
            "fuel_cost": float(r["fuel_cost"]),
            "maintenance_cost": float(r["maintenance_cost"]),
            "distance_covered": float(r["distance_covered"]),
            "profit": profit,
            "profit_per_km": compute_profit_per_km(profit, float(r["distance_covered"])),
            "margin": compute_margin(profit, float(r["earnings"])),
            "is_unprofitable": is_unprofitable(profit),
            "needs_service": bool(r["service_flag"]),
        })

    with _conn() as conn, conn.cursor() as cur:
        for rec in records:
            cur.execute(
                """
                INSERT INTO daily_profitability
                    (vehicle_id, cost_date, trips_count, earnings, fuel_cost, maintenance_cost,
                     distance_covered, profit, profit_per_km, margin, is_unprofitable, needs_service, computed_at)
                VALUES (%(vehicle_id)s, %(cost_date)s, %(trips_count)s, %(earnings)s, %(fuel_cost)s,
                        %(maintenance_cost)s, %(distance_covered)s, %(profit)s, %(profit_per_km)s,
                        %(margin)s, %(is_unprofitable)s, %(needs_service)s, now())
                ON CONFLICT (vehicle_id, cost_date) DO UPDATE SET
                    trips_count = EXCLUDED.trips_count,
                    earnings = EXCLUDED.earnings,
                    fuel_cost = EXCLUDED.fuel_cost,
                    maintenance_cost = EXCLUDED.maintenance_cost,
                    distance_covered = EXCLUDED.distance_covered,
                    profit = EXCLUDED.profit,
                    profit_per_km = EXCLUDED.profit_per_km,
                    margin = EXCLUDED.margin,
                    is_unprofitable = EXCLUDED.is_unprofitable,
                    needs_service = EXCLUDED.needs_service,
                    computed_at = now()
                """,
                rec,
            )
        conn.commit()

    _write_report_files(target_date, records)
    log.info("profitability_computed", cost_date=target_date, vehicles=len(records))
    return len(records)


def _write_report_files(target_date: str, records: list):
    os.makedirs(config.REPORTS_DIR, exist_ok=True)
    df = pd.DataFrame(records).sort_values("profit")  # unprofitable vehicles first

    csv_path = os.path.join(config.REPORTS_DIR, f"profitability_{target_date}.csv")
    df.to_csv(csv_path, index=False)

    rows_html = "\n".join(
        f"<tr style='background:{'#fdd' if r['is_unprofitable'] else '#fff'}'>"
        f"<td>{r['vehicle_id']}</td><td>{r['trips_count']}</td><td>{r['earnings']:.2f}</td>"
        f"<td>{r['fuel_cost']:.2f}</td><td>{r['maintenance_cost']:.2f}</td>"
        f"<td>{r['profit']:.2f}</td><td>{'YES' if r['needs_service'] else ''}</td></tr>"
        for r in df.to_dict("records")
    )
    html = f"""<html><head><title>Daily Profitability - {target_date}</title>
<style>table{{border-collapse:collapse;font-family:sans-serif}}
td,th{{border:1px solid #ccc;padding:6px 10px;text-align:right}}
th{{background:#333;color:#fff}}td:first-child,th:first-child{{text-align:left}}</style></head>
<body><h2>Daily Profitability Report &mdash; {target_date}</h2>
<table><tr><th>Vehicle</th><th>Trips</th><th>Earnings</th><th>Fuel</th><th>Maintenance</th>
<th>Profit</th><th>Needs Service</th></tr>{rows_html}</table></body></html>"""

    html_path = os.path.join(config.REPORTS_DIR, f"profitability_{target_date}.html")
    with open(html_path, "w") as f:
        f.write(html)


def mark_run_success(**context):
    target_date = context["ti"].xcom_pull(task_ids="determine_target_date", key="target_date")
    rows_loaded = context["ti"].xcom_pull(task_ids="load_and_validate", key="rows_loaded") or 0
    rows_rejected = context["ti"].xcom_pull(task_ids="load_and_validate", key="rows_rejected") or 0
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE batch_job_runs SET status = 'success', rows_loaded = %s, rows_rejected = %s,
                                       finished_at = now()
            WHERE cost_date = %s AND status = 'running'
            """,
            (rows_loaded, rows_rejected, target_date),
        )
        conn.commit()
    log.info("batch_run_marked_success", cost_date=target_date)


def on_failure_alert(**context):
    target_date = context["ti"].xcom_pull(task_ids="determine_target_date", key="target_date")
    sensor_state = context["ti"].xcom_pull(task_ids="wait_for_cost_file")
    failed_task_ids = [
        t.task_id for t in context["dag_run"].get_task_instances() if t.state == "failed"
    ]
    alert_type = "missing_cost_file" if "wait_for_cost_file" in failed_task_ids else "batch_job_failed"
    message = f"Batch job for {target_date} failed at task(s): {failed_task_ids or 'unknown'}"

    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO alerts (alert_type, severity, message) VALUES (%s, 'critical', %s)",
            (alert_type, message),
        )
        cur.execute(
            "UPDATE batch_job_runs SET status = %s, finished_at = now(), error_message = %s "
            "WHERE cost_date = %s AND status = 'running'",
            ("missing_file" if alert_type == "missing_cost_file" else "failed", message, target_date),
        )
        conn.commit()
    log.error("batch_run_failed", cost_date=target_date, alert_type=alert_type)


default_args = {
    "owner": "fleet-pipeline",
    "retries": 1,
    "retry_delay": timedelta(seconds=20),
}

with DAG(
    dag_id="daily_profitability",
    description="Batch layer: reconcile one simulated day's trips against fuel/maintenance costs.",
    schedule="*/1 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["fleet-pipeline", "batch", "lambda-batch-layer"],
) as dag:

    t_target_date = PythonOperator(
        task_id="determine_target_date",
        python_callable=determine_target_date,
    )

    t_wait_for_file = PythonSensor(
        task_id="wait_for_cost_file",
        python_callable=cost_file_exists,
        poke_interval=15,
        timeout=200,
        mode="reschedule",
    )

    t_load = PythonOperator(
        task_id="load_and_validate",
        python_callable=load_and_validate,
    )

    t_compute = PythonOperator(
        task_id="compute_profitability_and_report",
        python_callable=compute_profitability_and_report,
    )

    t_success = PythonOperator(
        task_id="mark_run_success",
        python_callable=mark_run_success,
    )

    t_failure_alert = PythonOperator(
        task_id="on_failure_alert",
        python_callable=on_failure_alert,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    t_target_date >> t_wait_for_file >> t_load >> t_compute >> t_success
    [t_wait_for_file, t_load, t_compute] >> t_failure_alert
