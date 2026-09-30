"""Serving layer: FastAPI.

Endpoints (see report/README for full list):
  GET /fleet/metrics[?zone=]      live utilization: active vehicles, idle ratio, earnings by zone
  GET /alerts[?resolved=]         recent alerts (idle vehicles, high invalid rate, batch failures)
  GET /reports/profitability[?date=]  daily per-vehicle reconciliation, unprofitable vehicles first
  GET /health                     liveness/health check: is the streaming layer still producing data?
  GET /metrics                    Prometheus text-exposition format, computed on read from Postgres
  GET /                           a small live HTML dashboard (auto-refreshing) over the endpoints above

No separate Prometheus/Grafana server is used for this project (see report,
Observability section, for the trade-off): /metrics is scraped-compatible
text format even though nothing is scraping it here, and the numbers behind
it live in pipeline_health / fleet_snapshot, written every streaming
micro-batch -- see streaming/spark_stream_job.py.
"""
import os
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse
from psycopg2.pool import SimpleConnectionPool

from common import config
from common.logging_utils import get_logger

log = get_logger("api")

app = FastAPI(title="Fleet Pipeline API", version="1.0")
pool = SimpleConnectionPool(1, 10, config.postgres_dsn())

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


def query(sql, params=None, one=False):
    conn = pool.getconn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or ())
            if cur.description is None:
                return None
            rows = cur.fetchall()
            return (rows[0] if rows else None) if one else rows
    finally:
        pool.putconn(conn)


@app.get("/fleet/metrics")
def fleet_metrics(zone: Optional[str] = None):
    snapshot = query("SELECT * FROM fleet_snapshot ORDER BY created_at DESC LIMIT 1", one=True)

    if zone:
        zone_rows = query(
            "SELECT zone, window_start, window_end, trips_started, earnings, active_vehicles "
            "FROM realtime_zone_metrics WHERE zone = %s ORDER BY window_start DESC LIMIT 24",
            (zone,),
        )
    else:
        zone_rows = query(
            """
            SELECT DISTINCT ON (zone) zone, window_start, window_end, trips_started, earnings, active_vehicles
            FROM realtime_zone_metrics
            ORDER BY zone, window_start DESC
            """
        )

    trips_last_hour = query(
        "SELECT COALESCE(SUM(trips_started), 0) AS trips FROM realtime_zone_metrics "
        "WHERE window_start >= now() - interval '1 hour'",
        one=True,
    )

    return {
        "snapshot": snapshot,
        "zones": zone_rows,
        "trips_last_hour": trips_last_hour["trips"] if trips_last_hour else 0,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/alerts")
def alerts(resolved: Optional[bool] = None, limit: int = 50):
    if resolved is None:
        rows = query("SELECT * FROM alerts ORDER BY real_time DESC LIMIT %s", (limit,))
    else:
        rows = query(
            "SELECT * FROM alerts WHERE resolved = %s ORDER BY real_time DESC LIMIT %s",
            (resolved, limit),
        )
    return {"alerts": rows}


@app.get("/reports/profitability")
def profitability(date: Optional[str] = Query(None)):
    if date is None:
        row = query("SELECT max(cost_date) AS d FROM daily_profitability", one=True)
        date = row["d"].isoformat() if row and row["d"] else None
        if date is None:
            return {"date": None, "vehicles": []}
    rows = query(
        "SELECT * FROM daily_profitability WHERE cost_date = %s ORDER BY profit ASC",
        (date,),
    )
    return {"date": date, "vehicles": rows}


@app.get("/health")
def health():
    last = query(
        "SELECT max(real_time) AS t FROM pipeline_health WHERE component = 'streaming'", one=True
    )
    last_t = last["t"] if last else None

    if last_t is None:
        return JSONResponse(
            {"status": "unknown", "reason": "no streaming batches recorded yet"}, status_code=503
        )

    age_s = (datetime.now(timezone.utc) - last_t).total_seconds()
    healthy = age_s <= config.NO_DATA_ALERT_SECONDS
    body = {
        "status": "ok" if healthy else "unhealthy",
        "reason": None if healthy else "no_data_received",
        "seconds_since_last_batch": round(age_s, 1),
        "threshold_seconds": config.NO_DATA_ALERT_SECONDS,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }
    return JSONResponse(body, status_code=200 if healthy else 503)


@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    last = query(
        "SELECT * FROM pipeline_health WHERE component = 'streaming' ORDER BY real_time DESC LIMIT 1",
        one=True,
    )
    snapshot = query("SELECT * FROM fleet_snapshot ORDER BY created_at DESC LIMIT 1", one=True)
    totals = query(
        "SELECT COALESCE(SUM(rows_in),0) ri, COALESCE(SUM(rows_out),0) ro, "
        "COALESCE(SUM(rows_rejected),0) rr FROM pipeline_health WHERE component = 'streaming'",
        one=True,
    )
    open_alerts = query("SELECT count(*) c FROM alerts WHERE resolved = false", one=True)

    lines = []

    def emit(name, value, help_text, mtype="gauge"):
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {mtype}")
        lines.append(f"{name} {value}")

    if last:
        age_s = (datetime.now(timezone.utc) - last["real_time"]).total_seconds()
        emit("fleet_seconds_since_last_batch", round(age_s, 2),
             "Real seconds since the last streaming micro-batch was recorded.")
        emit("fleet_last_batch_duration_ms", last["duration_ms"] or 0,
             "Duration of the last streaming micro-batch in milliseconds.")

    emit("fleet_events_ingested_total", totals["ri"] if totals else 0,
         "Total raw events seen by the streaming job.", "counter")
    emit("fleet_events_valid_total", totals["ro"] if totals else 0,
         "Total events that passed validation.", "counter")
    emit("fleet_events_rejected_total", totals["rr"] if totals else 0,
         "Total events rejected or quarantined.", "counter")

    if snapshot:
        emit("fleet_active_vehicles", snapshot["active_vehicles"], "Vehicles currently enroute or on a trip.")
        emit("fleet_idle_vehicles", snapshot["idle_vehicles"], "Vehicles currently idle.")
        emit("fleet_idle_ratio", float(snapshot["idle_ratio"]), "Fraction of known vehicles currently idle.")

    emit("fleet_open_alerts", open_alerts["c"] if open_alerts else 0, "Currently unresolved alerts.", "counter")

    return "\n".join(lines) + "\n"


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return FileResponse(os.path.join(STATIC_DIR, "dashboard.html"))


@app.on_event("startup")
def _log_startup():
    log.info("api_started", postgres_host=config.POSTGRES_HOST)
