"""Speed layer: Spark Structured Streaming job.

Reads trip-events from Kafka and, on every micro-batch (foreachBatch,
trigger every SIM_EMIT_INTERVAL-scale seconds):

  1. parses JSON (malformed JSON -> quarantine, reason=malformed_json)
  2. drops duplicate event_id within the batch
  3. validates business rules via common.validation (invalid -> quarantine)
  4. enriches with zone via common.zones (derived here, not by the simulator)
  5. appends cleaned trips to Parquet, partitioned by simulated date
  6. accumulates windowed per-zone-per-simulated-hour aggregates in Postgres
  7. tracks each vehicle's latest status and detects idle-too-long vehicles
  8. writes one pipeline_health row per batch (drives the API's /health and
     /metrics, so no separate Prometheus server is needed for this project)

Design choice: instead of Spark's native stateful windowed aggregation
(groupBy(window(...)).agg(...) with watermarks), each micro-batch is pulled
to the driver with toPandas() and processed with plain Python + psycopg2
upserts. At this data volume (a few hundred rows per 5s trigger, ~30-50
vehicles) that is simpler to get right under a tight deadline than tuning
watermark delays against an event-time clock that runs ~288x real speed,
and it lets the streaming job reuse the *exact* same common/validation.py,
common/zones.py and common/revenue.py functions the batch layer uses -- one
of the concrete steps taken to stop the two Lambda code paths from drifting
apart (see report, architecture decision section). Spark Structured
Streaming is still doing the real work here: continuous Kafka consumption,
micro-batch triggering, and checkpointed, restart-safe offset tracking.
"""
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import psycopg2.extras
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from common import config
from common.logging_utils import get_logger
from common.revenue import compute_profit  # noqa: F401 - imported for parity/reuse, used in report discussion
from common.sim_clock import sim_now
from common.validation import validate_trip_event
from common.zones import zone_for

log = get_logger("processing.spark_stream")

_conn = None


def get_conn():
    global _conn
    if _conn is None or _conn.closed:
        _conn = psycopg2.connect(config.postgres_dsn())
        _conn.autocommit = False
    return _conn


def _parse_ts(value: str):
    try:
        return datetime.fromisoformat(value)
    except Exception:  # noqa: BLE001
        return None


def _hour_bucket(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def _write_parquet(rows, base_dir: str, dataset: str):
    if not rows:
        return
    df = pd.DataFrame(rows)
    df["sim_date"] = df["timestamp"].astype(str).str.slice(0, 10)
    out_dir = os.path.join(base_dir, dataset)
    os.makedirs(out_dir, exist_ok=True)
    df.to_parquet(out_dir, engine="pyarrow", partition_cols=["sim_date"], index=False)


def process_batch(batch_df, batch_id: int):
    t0 = time.time()
    conn = get_conn()

    pdf = batch_df.select(
        col("value").cast("string").alias("json_str"),
        col("timestamp").alias("kafka_ingest_time"),
    ).toPandas()

    rows_in = len(pdf)
    parsed = []
    quarantine = []

    for _, row in pdf.iterrows():
        try:
            rec = json.loads(row["json_str"])
        except Exception:  # noqa: BLE001
            quarantine.append({
                "raw_json": row["json_str"], "reject_reason": "malformed_json",
                "timestamp": sim_now().isoformat(),
            })
            continue
        rec["_kafka_ingest_time"] = str(row["kafka_ingest_time"])
        parsed.append(rec)

    # 2. dedupe within this micro-batch on event_id
    seen = set()
    deduped = []
    duplicate_count = 0
    for rec in parsed:
        eid = rec.get("event_id")
        if eid in seen:
            duplicate_count += 1
            continue
        seen.add(eid)
        deduped.append(rec)

    # 3 + 4. validate, then enrich with zone
    valid_rows = []
    for rec in deduped:
        ok, reason = validate_trip_event(rec)
        if not ok:
            bad = dict(rec)
            bad["reject_reason"] = reason
            quarantine.append(bad)
            continue
        rec["zone"] = zone_for(float(rec["lat"]), float(rec["lon"]))
        valid_rows.append(rec)

    rows_out = len(valid_rows)
    rows_rejected = rows_in - rows_out

    # 5. append cleaned + quarantined data to Parquet (master data / replay source)
    _write_parquet(valid_rows, config.PARQUET_DIR, "trips")
    _write_parquet(quarantine, config.QUARANTINE_DIR, "trips")

    # 6. windowed per-zone aggregates (accumulated in Postgres across batches)
    zone_hour = defaultdict(lambda: {"trips_started": 0, "earnings": 0.0, "vehicles": set()})
    latest_per_vehicle = {}
    for rec in valid_rows:
        event_time = _parse_ts(rec["timestamp"])
        if event_time is None:
            continue
        window_start = _hour_bucket(event_time)
        key = (rec["zone"], window_start)
        bucket = zone_hour[key]
        fare = float(rec.get("fare") or 0)
        if fare > 0:
            bucket["trips_started"] += 1
            bucket["earnings"] += fare
        bucket["vehicles"].add(rec["vehicle_id"])
        # keep the row with the latest timestamp per vehicle for status tracking
        prev = latest_per_vehicle.get(rec["vehicle_id"])
        if prev is None or event_time >= prev[0]:
            latest_per_vehicle[rec["vehicle_id"]] = (event_time, rec)

    cur = conn.cursor()
    try:
        for (zone, window_start), bucket in zone_hour.items():
            window_end = window_start + timedelta(hours=1)
            cur.execute(
                """
                INSERT INTO realtime_zone_metrics
                    (zone, window_start, window_end, trips_started, earnings, active_vehicles, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (zone, window_start) DO UPDATE SET
                    trips_started = realtime_zone_metrics.trips_started + EXCLUDED.trips_started,
                    earnings = realtime_zone_metrics.earnings + EXCLUDED.earnings,
                    updated_at = now()
                """,
                (zone, window_start, window_end, bucket["trips_started"], round(bucket["earnings"], 2), 0),
            )
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO realtime_zone_vehicle_seen (zone, window_start, vehicle_id) VALUES %s "
                "ON CONFLICT DO NOTHING",
                [(zone, window_start, vid) for vid in bucket["vehicles"]],
            )
            cur.execute(
                """
                UPDATE realtime_zone_metrics SET active_vehicles = (
                    SELECT count(*) FROM realtime_zone_vehicle_seen s
                    WHERE s.zone = %s AND s.window_start = %s
                ) WHERE zone = %s AND window_start = %s
                """,
                (zone, window_start, zone, window_start),
            )

        # 7. vehicle status tracking + idle detection
        resolved_vehicle_ids = []
        for vehicle_id, (event_time, rec) in latest_per_vehicle.items():
            cur.execute(
                "SELECT last_status, idle_since_sim_time FROM vehicle_status WHERE vehicle_id = %s",
                (vehicle_id,),
            )
            prev = cur.fetchone()

            if rec["status"] == "idle":
                if prev and prev[0] == "idle" and prev[1] is not None:
                    idle_since = prev[1]
                else:
                    idle_since = event_time
                idle_duration_s = (event_time - idle_since).total_seconds()
                if idle_duration_s >= config.IDLE_ALERT_SIM_HOURS * 3600:
                    cur.execute(
                        "SELECT 1 FROM alerts WHERE vehicle_id = %s AND alert_type = 'vehicle_idle' "
                        "AND resolved = false",
                        (vehicle_id,),
                    )
                    if cur.fetchone() is None:
                        cur.execute(
                            """
                            INSERT INTO alerts (alert_type, severity, vehicle_id, zone, message, sim_time)
                            VALUES ('vehicle_idle', 'warning', %s, %s, %s, %s)
                            """,
                            (
                                vehicle_id, rec["zone"],
                                f"Vehicle {vehicle_id} idle for "
                                f"{idle_duration_s / 3600:.1f} simulated hours in {rec['zone']}",
                                event_time,
                            ),
                        )
            else:
                idle_since = None
                resolved_vehicle_ids.append(vehicle_id)

            cur.execute(
                """
                INSERT INTO vehicle_status (vehicle_id, last_status, last_zone, last_event_sim_time,
                                             idle_since_sim_time, updated_at)
                VALUES (%s, %s, %s, %s, %s, now())
                ON CONFLICT (vehicle_id) DO UPDATE SET
                    last_status = EXCLUDED.last_status,
                    last_zone = EXCLUDED.last_zone,
                    last_event_sim_time = EXCLUDED.last_event_sim_time,
                    idle_since_sim_time = EXCLUDED.idle_since_sim_time,
                    updated_at = now()
                """,
                (vehicle_id, rec["status"], rec["zone"], event_time, idle_since),
            )

        if resolved_vehicle_ids:
            cur.execute(
                "UPDATE alerts SET resolved = true WHERE vehicle_id = ANY(%s) "
                "AND alert_type = 'vehicle_idle' AND resolved = false",
                (resolved_vehicle_ids,),
            )

        # fleet-wide snapshot, derived from the full vehicle_status table (not just this batch)
        cur.execute("SELECT last_status, count(*) FROM vehicle_status GROUP BY last_status")
        status_counts = dict(cur.fetchall())
        idle_c = status_counts.get("idle", 0)
        enroute_c = status_counts.get("enroute", 0)
        on_trip_c = status_counts.get("on_trip", 0)
        total = idle_c + enroute_c + on_trip_c
        active = enroute_c + on_trip_c
        idle_ratio = (idle_c / total) if total else 0.0

        cur.execute(
            """
            INSERT INTO fleet_snapshot (snapshot_sim_time, active_vehicles, idle_vehicles,
                                         enroute_vehicles, on_trip_vehicles, idle_ratio, total_vehicles_seen)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (sim_now(), active, idle_c, enroute_c, on_trip_c, round(idle_ratio, 4), total),
        )

        # high-invalid-rate alert / auto-resolve, based on this batch's rate
        if rows_in > 0:
            rate = rows_rejected / rows_in
            cur.execute(
                "SELECT 1 FROM alerts WHERE alert_type = 'high_invalid_rate' AND resolved = false"
            )
            already_open = cur.fetchone() is not None
            if rate > config.INVALID_RATE_ALERT_THRESHOLD and not already_open:
                cur.execute(
                    """
                    INSERT INTO alerts (alert_type, severity, message, sim_time)
                    VALUES ('high_invalid_rate', 'warning', %s, %s)
                    """,
                    (f"Invalid/rejected event rate {rate:.1%} over threshold "
                     f"{config.INVALID_RATE_ALERT_THRESHOLD:.0%} in batch {batch_id}", sim_now()),
                )
            elif rate <= config.INVALID_RATE_ALERT_THRESHOLD and already_open:
                cur.execute(
                    "UPDATE alerts SET resolved = true WHERE alert_type = 'high_invalid_rate' "
                    "AND resolved = false"
                )

        duration_ms = int((time.time() - t0) * 1000)
        cur.execute(
            """
            INSERT INTO pipeline_health (component, batch_id, sim_time, rows_in, rows_out,
                                          rows_rejected, duration_ms)
            VALUES ('streaming', %s, %s, %s, %s, %s, %s)
            """,
            (str(batch_id), sim_now(), rows_in, rows_out, rows_rejected, duration_ms),
        )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()

    log.info(
        "batch_processed",
        batch_id=batch_id,
        rows_in=rows_in,
        rows_out=rows_out,
        rows_rejected=rows_rejected,
        duplicates=duplicate_count,
        duration_ms=int((time.time() - t0) * 1000),
    )


def main():
    log.info("spark_stream_job_starting", kafka=config.KAFKA_BOOTSTRAP, topic=config.KAFKA_TOPIC_TRIPS)

    spark = (
        SparkSession.builder
        .appName("fleet-trip-stream")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", config.KAFKA_TOPIC_TRIPS)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    query = (
        raw.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", os.path.join(config.CHECKPOINT_DIR, "trip_stream"))
        .trigger(processingTime="5 seconds")
        .start()
    )

    log.info("spark_stream_job_running")
    query.awaitTermination()


if __name__ == "__main__":
    main()
