# Fleet Pipeline — Ride-Hailing Fleet Operations (EC8203 Mini Project)

A small but complete Lambda-architecture data platform for ride-hailing fleet
operations: live fleet utilization / earnings-by-zone, idle-vehicle alerts,
and a daily per-vehicle profitability reconciliation against fuel and
maintenance costs.

Full architecture rationale, tech-stack justification, and results are in
the report (`docs/report.pdf`). This README is the "how to run it" doc.

## Architecture at a glance

```
trip_simulator.py ──► Kafka topic: trip-events (3 partitions, key=vehicle_id)
                             │
                             ├──► SPEED LAYER: Spark Structured Streaming
                             │      parse, dedupe, validate, zone-enrich,
                             │      windowed aggregates, idle detection
                             │        ├──► Postgres: realtime_* tables, alerts
                             │        └──► Parquet: clean + quarantined trips
                             │             (data/parquet, data/quarantine)
cost_simulator.py ──► data/landing/costs_YYYY-MM-DD.csv (atomic write)
                             │
                             └──► BATCH LAYER: Airflow DAG (daily_profitability)
                                    find pending file → wait for it → validate
                                    → join with that day's Parquet trips
                                    → profit/margin → upsert + CSV/HTML report
                                      └──► Postgres: daily_profitability

SERVING: FastAPI (/fleet/metrics, /alerts, /reports/profitability, /health,
          /metrics) + a small live HTML dashboard at http://localhost:8000/

OBSERVABILITY: JSON logs on every stage, pipeline_health table driving
          /health and /metrics, 4 alert rules (see below).
```

Why Lambda over Kappa, and the full technology justification, are in the
report — this is the short version: utilization needs second-level latency,
profitability needs a once-a-day, correctable, replayable batch pass over a
file that can be re-submitted by fuel partners, and Airflow (a named
requirement) is a natural fit for that batch pass but has little role in a
pure Kappa design.

## Simulated clock (read this first — it explains the demo's timestamps)

- 1 simulated day = 300 real seconds (5 real minutes) by default
  (`SIM_DAY_REAL_SECONDS` in `.env`).
- Every component derives "what simulated time is it" purely from wall-clock
  time (see `common/sim_clock.py`) — no coordination file needed, but it
  also means the simulated date you see depends on what real time of day you
  start the stack, not on a fixed "day 0". This is stated as an assumption
  in the report.
- The daily cost file appears once per simulated day (once every 5 real
  minutes), representing the day that just finished.
- Idle-vehicle alert threshold: 2 simulated hours (~25 real seconds).

## Prerequisites

- Docker Desktop (with the WSL2 backend on Windows), with at least ~6–8 GB
  of RAM allocated to it. Everything else (Kafka, Spark, Airflow, Postgres)
  runs inside containers — no local Python/Java install is required to run
  the stack, only to run the unit tests directly on the host (optional).

## Run it

```bash
cp .env.example .env
docker compose up -d --build
```

First start takes a few minutes: Postgres/Kafka images pull, and the Spark
container resolves the Kafka connector jars from Maven on its very first
run (cached afterwards in the `spark_ivy_cache` volume, so later restarts
are fast even offline).

Then create the Airflow admin user and metadata tables (one-off, only
needed the first time — `airflow-init` is not part of `up -d` on purpose so
you can see it run to completion):

```bash
docker compose run --rm airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

Check everything is up:

```bash
docker compose ps
```

## Where to look

| What | URL / command |
|---|---|
| Live dashboard | http://localhost:8000/ |
| API root (all endpoints below) | http://localhost:8000 |
| Airflow UI (login `admin` / `admin` from `.env`) | http://localhost:8080 |
| Postgres | `localhost:5432`, db `fleet`, user/pass from `.env` |
| Kafka (host-visible listener) | `localhost:9094` |
| Raw Kafka messages | `docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic trip-events` |

API endpoints:

- `GET /fleet/metrics[?zone=City-Center]` — active vehicles, idle ratio, earnings/trips by zone (latest window)
- `GET /alerts[?resolved=false]` — recent alerts (idle vehicle, high invalid rate, missing/failed batch job)
- `GET /reports/profitability[?date=YYYY-MM-DD]` — daily reconciliation, unprofitable vehicles first
- `GET /health` — 200 if the streaming layer has produced data recently, 503 otherwise
- `GET /metrics` — Prometheus text-exposition format

## Demo script

1. `docker compose up -d` and `docker compose ps` — show every service healthy.
2. Open http://localhost:8000/ — watch the fleet snapshot, zone earnings and
   alerts update live (refreshes every 3s).
3. Open the Airflow UI (http://localhost:8080), show the `daily_profitability`
   DAG's graph and a successful run; open the profitability report at
   `/reports/profitability`, or `data/reports/profitability_<date>.html`.
4. `docker compose logs -f spark-stream` / `trip-simulator` — show the JSON
   structured logs.
5. `curl http://localhost:8000/metrics` — Prometheus-format metrics.
6. Trigger the "no data" alert: `docker compose stop trip-simulator`, wait
   ~2 minutes (`NO_DATA_ALERT_SECONDS`), then `curl http://localhost:8000/health`
   → 503. `docker compose start trip-simulator` to recover.
7. Trigger the "missing cost file" alert: `docker compose stop cost-simulator`
   and let the next Airflow DAG run time out waiting for the file — check
   `/alerts` for `alert_type=missing_cost_file`.

## Repository layout

```
fleet-pipeline/
├── docker-compose.yml
├── .env.example
├── common/          shared logging, config, sim clock, zone lookup, validation, profit math
├── simulators/      trip_simulator.py (streaming), cost_simulator.py (daily batch)
├── streaming/        spark_stream_job.py — Spark Structured Streaming job
├── airflow/dags/     daily_profitability_dag.py — the batch/reporting DAG
├── api/              FastAPI app + static/dashboard.html
├── sql/              Postgres schema (init scripts, run automatically)
├── tests/            unit tests for common/ (pytest)
└── docs/             architecture diagram, screenshots, report
```

## Running the unit tests

```bash
pip install pytest
python -m pytest -q
```

These cover `common/` (zone lookup, event/cost validation, profit math) —
the logic shared by both the speed and batch layers, so a bug here would
affect both sides of the Lambda architecture identically.

## Configuration

All configuration is environment variables, read once in `common/config.py`
and set in `.env` (copy `.env.example`). Notable ones: `SIM_DAY_REAL_SECONDS`,
`SIM_VEHICLE_COUNT`, `SIM_MESSY_RATE` (fraction of events deliberately
corrupted to exercise validation/quarantine), `IDLE_ALERT_SIM_HOURS`,
`NO_DATA_ALERT_SECONDS`, `INVALID_RATE_ALERT_THRESHOLD`.

## Known limitations / assumptions

See the report's Limitations section for the full list. Headline items:
single-broker Kafka (no replication), no schema registry, local-disk Parquet
standing in for S3/HDFS, `local[*]` Spark deployment mode, fare is
recognised once at trip start rather than at completion (documented
simplification so windowed "earnings" sums correctly without dedup), and
zone/hour aggregates are accumulated via Postgres upserts across
micro-batches rather than Spark's native stateful windowed operators (a
deliberate choice given the ~288x simulated-time compression — see report).
