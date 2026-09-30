-- Fleet Pipeline: application schema (runs against the POSTGRES_DB / "fleet" database).
-- This is the serving store for the speed layer (realtime_*, alerts, vehicle_status,
-- pipeline_health) and the batch layer (cost_staging, daily_profitability, batch_job_runs).

-- ---------------------------------------------------------------------------
-- Speed layer: written by streaming/spark_stream_job.py every micro-batch.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS realtime_zone_metrics (
    zone            TEXT NOT NULL,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    trips_started   INTEGER NOT NULL DEFAULT 0,
    earnings        NUMERIC(12, 2) NOT NULL DEFAULT 0,
    active_vehicles INTEGER NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (zone, window_start)
);
CREATE INDEX IF NOT EXISTS idx_realtime_zone_metrics_window
    ON realtime_zone_metrics (window_start DESC);

-- Junction table so "active_vehicles" per zone/hour is a true distinct
-- count across all micro-batches that touched that window, not just the
-- max seen in a single batch.
CREATE TABLE IF NOT EXISTS realtime_zone_vehicle_seen (
    zone         TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    vehicle_id   TEXT NOT NULL,
    PRIMARY KEY (zone, window_start, vehicle_id)
);

CREATE TABLE IF NOT EXISTS fleet_snapshot (
    id                  BIGSERIAL PRIMARY KEY,
    snapshot_sim_time   TIMESTAMPTZ NOT NULL,
    active_vehicles     INTEGER NOT NULL,
    idle_vehicles       INTEGER NOT NULL,
    enroute_vehicles    INTEGER NOT NULL,
    on_trip_vehicles    INTEGER NOT NULL,
    idle_ratio          NUMERIC(6, 4) NOT NULL,
    total_vehicles_seen INTEGER NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_fleet_snapshot_created ON fleet_snapshot (created_at DESC);

-- Latest known status per vehicle; used by the streaming job to detect idle
-- vehicles across micro-batches (stateful upsert, not a windowed join).
CREATE TABLE IF NOT EXISTS vehicle_status (
    vehicle_id          TEXT PRIMARY KEY,
    last_status         TEXT,
    last_zone           TEXT,
    last_event_sim_time TIMESTAMPTZ,
    idle_since_sim_time TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS alerts (
    id          BIGSERIAL PRIMARY KEY,
    alert_type  TEXT NOT NULL,      -- vehicle_idle | no_data | high_invalid_rate | batch_job_failed | missing_cost_file
    severity    TEXT NOT NULL DEFAULT 'warning',
    vehicle_id  TEXT,
    zone        TEXT,
    message     TEXT NOT NULL,
    sim_time    TIMESTAMPTZ,
    real_time   TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved    BOOLEAN NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS idx_alerts_real_time ON alerts (real_time DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_type ON alerts (alert_type);

-- One row per streaming micro-batch. Powers /health and /metrics without
-- needing a separate Prometheus server.
CREATE TABLE IF NOT EXISTS pipeline_health (
    id           BIGSERIAL PRIMARY KEY,
    component    TEXT NOT NULL,     -- streaming | batch | simulator
    batch_id     TEXT,
    real_time    TIMESTAMPTZ NOT NULL DEFAULT now(),
    sim_time     TIMESTAMPTZ,
    rows_in      INTEGER,
    rows_out     INTEGER,
    rows_rejected INTEGER,
    duration_ms  INTEGER
);
CREATE INDEX IF NOT EXISTS idx_pipeline_health_real_time ON pipeline_health (real_time DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_health_component ON pipeline_health (component, real_time DESC);

-- ---------------------------------------------------------------------------
-- Batch layer: written once per simulated day by airflow/dags/daily_profitability_dag.py
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS cost_staging (
    id                BIGSERIAL PRIMARY KEY,
    vehicle_id        TEXT,
    fuel_cost         NUMERIC(12, 2),
    maintenance_cost  NUMERIC(12, 2),
    distance_covered  NUMERIC(12, 2),
    service_flag      BOOLEAN,
    cost_date         DATE,
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_valid          BOOLEAN NOT NULL,
    reject_reason     TEXT
);
CREATE INDEX IF NOT EXISTS idx_cost_staging_date ON cost_staging (cost_date);

-- Idempotent target: re-running the DAG for the same cost_date overwrites
-- the same rows (upsert on the primary key), so replays give identical results.
CREATE TABLE IF NOT EXISTS daily_profitability (
    vehicle_id        TEXT NOT NULL,
    cost_date         DATE NOT NULL,
    trips_count       INTEGER NOT NULL DEFAULT 0,
    earnings          NUMERIC(12, 2) NOT NULL DEFAULT 0,
    fuel_cost         NUMERIC(12, 2) NOT NULL DEFAULT 0,
    maintenance_cost  NUMERIC(12, 2) NOT NULL DEFAULT 0,
    distance_covered  NUMERIC(12, 2) NOT NULL DEFAULT 0,
    profit            NUMERIC(12, 2) NOT NULL,
    profit_per_km     NUMERIC(12, 4),
    margin            NUMERIC(8, 4),
    is_unprofitable   BOOLEAN NOT NULL DEFAULT false,
    needs_service     BOOLEAN NOT NULL DEFAULT false,
    computed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (vehicle_id, cost_date)
);
CREATE INDEX IF NOT EXISTS idx_daily_profitability_date ON daily_profitability (cost_date DESC);

CREATE TABLE IF NOT EXISTS batch_job_runs (
    id            BIGSERIAL PRIMARY KEY,
    cost_date     DATE,
    status        TEXT NOT NULL,   -- running | success | failed | missing_file
    rows_loaded   INTEGER,
    rows_rejected INTEGER,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ,
    error_message TEXT
);
CREATE INDEX IF NOT EXISTS idx_batch_job_runs_date ON batch_job_runs (cost_date DESC);
