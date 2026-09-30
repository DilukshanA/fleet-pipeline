# EC8203 Mini Project: Project Plan

Use case: Ride-Hailing Fleet Operations
Weight: 25% of module grade. Total 100 marks.
Plan length: 10 days (Mon 21 Sep to Wed 30 Sep 2026). This is my assumption, since you said "less than 2 weeks". If your real deadline is different, the day-by-day table in section 8 can be squeezed or stretched.

---

## 1. What the guideline actually asks for

You must build a small but complete data platform that does five things:

1. Simulates two sources with Python scripts: a streaming source (trip events every few seconds) and a daily-batch source (one vehicle expense file per simulated day).
2. Ingests both, with Apache Kafka for the stream.
3. Processes them with a real transformation (clean, enrich, join, aggregate, window), using Spark Structured Streaming or Storm, and Airflow for the batch or reporting job.
4. Stores results in a queryable store (PostgreSQL, Cassandra, or Parquet on a file system) and serves a consolidated report or dashboard.
5. Is observable: structured logging in every stage, plus at least one alert or health-check rule.

You must also decide between Lambda and Kappa, and defend that decision in the report, including the alternative you rejected.

Deliverables: code repo (with README and Docker Compose), a PDF report (8 to 15 pages), and a 5 to 10 minute demo video or a live demo. If you work in a group, add a short statement of individual contributions.

The viva rule matters: you can use AI for boilerplate, but you must be able to explain every architecture decision and every line of core pipeline logic.

## 2. Business question to answer

"What is fleet utilization and earnings by area and time of day right now, and which vehicles are becoming unprofitable once yesterday's fuel and maintenance costs are factored in?"

Your system needs to give two kinds of answer:

- Right now (seconds old): active vehicles, idle ratio, trips per hour, earnings by zone, and an alert when a vehicle has been idle too long.
- Yesterday (once per day): a per-vehicle profitability report = earnings from trips minus fuel cost minus maintenance cost, with unprofitable vehicles flagged.

## 3. Architecture decision: Lambda or Kappa

My recommendation: a simplified Lambda architecture.

Why Lambda fits this use case:

- The two questions have different latency needs. Utilization needs seconds. Profitability needs correctness once a day. Lambda has a speed layer and a batch layer built for exactly that split.
- The daily cost file is a batch input by nature. It arrives late (yesterday's costs) and can be corrected by the garage or fuel partner. A batch job can simply recompute the whole day. In Kappa you would have to replay the stream and join it against a slow-changing source, which is harder to reason about.
- The guideline names Airflow for batch and reporting pipelines. Lambda uses Airflow naturally. In a pure Kappa design Airflow has almost nothing to do.
- Replay and reprocessing are easy: raw trips are kept as Parquet, so any day can be recomputed.

Rejected alternative: Kappa. Say clearly in the report why you rejected it: one code path is simpler and avoids duplicate logic, but it needs long Kafka retention, and a stream-to-daily-file join with late corrections is awkward. Also be honest about the cost of Lambda: two code paths that can drift apart. Reduce that risk by putting shared logic (zone lookup, status validation, revenue calculation) in one shared Python module used by both layers.

You can still choose Kappa if you prefer. If so, tell me and I will rework sections 4 to 6. The report just needs to argue it well. The rubric gives 20 marks for the quality of the argument, not for which one you pick.

## 4. Architecture at a glance

```
 trip_simulator.py ──► Kafka topic: trip-events (3 partitions, key = vehicle_id)
                              │
                              ├──► SPEED LAYER: Spark Structured Streaming
                              │       clean, dedupe, zone enrichment,
                              │       windowed aggregates, idle detection
                              │         ├──► Postgres: realtime tables (upsert)
                              │         └──► Parquet: raw + cleaned trips (master data, partitioned by sim date)
                              │
 cost_simulator.py ──► landing folder: costs_YYYY-MM-DD.csv
                              │
                              └──► BATCH LAYER: Airflow DAG (once per simulated day)
                                      file sensor → validate → load to Postgres staging
                                      → join with that day's trips (Parquet) → profitability
                                        ├──► Postgres: daily_profitability table
                                        └──► report file (CSV + HTML)

 SERVING: FastAPI  (/fleet/metrics, /alerts, /reports/profitability, /health, /metrics)
          Grafana dashboard (optional, reads Postgres + Prometheus)

 OBSERVABILITY: JSON logs on every stage, Prometheus metrics, alert rules, health check
```

## 5. Technology stack and why

| Layer | Choice | Reason tied to this use case |
|---|---|---|
| Ingestion | Apache Kafka (KRaft mode, no ZooKeeper) | Trip events are continuous and keyed by vehicle. Partitioning by vehicle_id keeps each vehicle's events in order, which idle detection needs. Kafka also decouples the simulator from processing. |
| Stream processing | Spark Structured Streaming | Event-time windows and watermarks handle late and out-of-order GPS events. The same DataFrame API is used for batch, so shared logic is easy. Storm is more manual for windowing and has a smaller community. |
| Orchestration | Apache Airflow | The cost file arrives once per simulated day. A DAG with a file sensor, retries and a run history fits a daily reconciliation job. |
| Serving store | PostgreSQL | The API needs quick filtered reads (by zone, vehicle, date), and the report needs joins. Postgres does both with plain SQL. Cassandra would suit huge write volume, which a 30 to 50 vehicle fleet does not need. |
| Data lake | Parquet on a local volume | Cheap replayable history for the batch layer and for reprocessing. Justify it as a stand-in for S3/HDFS. |
| API | FastAPI | Matches the "API endpoint for real-time utilization" output, and it is easy to add a Prometheus /metrics endpoint. |
| Monitoring | Prometheus + Grafana | Standard, lightweight, and gives you a screenshot-friendly dashboard for the report. |
| Packaging | Docker Compose | The guideline strongly recommends it and it scores under Code Quality. |

Note for your laptop: Kafka, Spark, Airflow, Postgres, Prometheus and Grafana together want roughly 8 GB of RAM. Keep Spark to one small worker and Airflow to LocalExecutor. If memory is tight, drop Grafana first and let the API and the report carry the dashboard.

## 6. Design details to decide early

### Simulated clock

Say it clearly in the report and the README.

- 1 simulated day = 5 real minutes (24 hours compressed to 300 seconds, so 1 real second = 288 simulated seconds).
- 1 simulated hour = 12.5 real seconds. Trips per hour and earnings by hour use a 1-simulated-hour window.
- The simulator emits an event batch every 2 real seconds. Event timestamp is the simulated time. Also add a real ingest time field so lag can be measured.
- Make demand vary by time of day (morning and evening peaks) so "by time of day" gives a visible result.
- Idle alert: vehicle idle for more than 2 simulated hours (about 25 real seconds).

### Streaming event (from the guideline, plus small additions)

trip_id, driver_id, vehicle_id, lat, lon, speed, status (idle / enroute / on_trip), fare, timestamp. Add: event_id (for dedupe), zone (derived in processing, not by the simulator), ingest_time.

### Daily cost file

vehicle_id, fuel_cost, maintenance_cost, distance_covered, service_flag. Add: cost_date. Name it costs_YYYY-MM-DD.csv (or JSON) and write it atomically (write to a temp name, then rename) so Airflow never reads half a file.

### Deliberately messy data (this earns the "robustness" marks)

Make both simulators occasionally produce: duplicate events, null or out-of-range lat/lon, negative fares, unknown status values, out-of-order timestamps, and a missing vehicle in a cost file. The pipeline must handle each one (drop, quarantine, or default) and count it in metrics.

### Zones

Split the map into a grid, for example 3 by 3 zones named by area. Zone lookup is done by a small function on lat/lon, shared by both layers.

### Processing logic

Speed layer:

1. Parse JSON and validate schema. Send bad records to a quarantine (a Parquet folder or a Kafka dead-letter topic).
2. Drop duplicates on event_id within the watermark.
3. Assign zone from lat/lon.
4. Windowed aggregates per zone per simulated hour: trips started, earnings, distinct active vehicles.
5. Fleet snapshot: idle ratio = idle vehicles / all vehicles seen in the last window.
6. Idle detection per vehicle (stateful, or a window over last status). Write to an alerts table.
7. Append cleaned trips to Parquet, partitioned by simulated date.

Batch layer (Airflow DAG, one run per simulated day):

1. Sensor waits for costs_YYYY-MM-DD.csv.
2. Validate and load to a staging table (reject bad rows and log them).
3. Read that simulated day's cleaned trips from Parquet. Compute earnings per vehicle.
4. Join with costs on vehicle_id. Compute profit = earnings - fuel_cost - maintenance_cost, profit per km, and a margin.
5. Flag unprofitable vehicles: profit below zero, or profit falling for 3 days in a row ("becoming unprofitable"). Also use service_flag to mark vehicles due for service.
6. Upsert to daily_profitability (idempotent, so a re-run gives the same result) and write the report file.

### Serving

- GET /fleet/metrics: active vehicles, idle ratio, trips per hour, earnings by zone (latest window).
- GET /fleet/metrics?zone=... for a filter.
- GET /alerts: recent idle alerts.
- GET /reports/profitability?date=...: the daily reconciliation, with unprofitable vehicles at the top.
- GET /health: OK or not, based on how old the latest event is.
- GET /metrics: Prometheus format.

## 7. Observability plan (10 marks)

Logging. One JSON log format shared by all components (use a small shared logger module). Every line has timestamp, stage (ingestion, processing, storage, batch, api), level, message, and useful fields like batch_id, rows_in, rows_out, rows_rejected, duration_ms.

Metrics (Prometheus).

- events_produced_total, events_consumed_total, events_invalid_total
- streaming batch duration and input rows per second (from Spark's StreamingQueryListener)
- seconds_since_last_event
- airflow DAG run success and failure counts, batch job duration
- API request count and latency

Practical tip: the streaming job can write one row per micro-batch to a pipeline_health table, and the API exposes those as Prometheus metrics. This is simpler than wiring Spark to Prometheus directly.

Alert and health-check rules (guideline needs at least one, so do three):

1. No data received in 2 minutes: seconds_since_last_event above 120 (or use a shorter limit for the demo).
2. Invalid-event rate above 5% over the last few minutes.
3. Airflow daily job failed, or the cost file did not arrive by the end of the simulated day.

Plus the business alert from the guideline: vehicle idle for more than 2 simulated hours.

Demonstrate them: kill the simulator during the demo and show the "no data" alert firing. This is the strongest thing you can show for the Observability marks.

## 8. Day-by-day timeline (10 days)

| Day | Date | Goal | Done when |
|---|---|---|---|
| 1 | Mon 21 Sep | Decisions and design. Confirm Lambda. Draw architecture diagram. Define schemas, zone grid, simulated clock. Create the repo and folder layout. | Design notes and diagram v1 exist. Repo pushed. |
| 2 | Tue 22 Sep | Docker Compose skeleton with Kafka, Postgres, Spark, Airflow. Write trip_simulator.py. | `docker compose up` starts everything. Events appear in a Kafka console consumer. |
| 3 | Wed 23 Sep | Write cost_simulator.py (daily file drop with simulated clock). Add the messy-data injection to both simulators. Create Postgres schema. | A new cost file appears every 5 minutes. Bad records show up in the data. |
| 4 | Thu 24 Sep | Spark streaming job, part 1: parse, validate, dedupe, zone enrichment, quarantine, write Parquet. | Clean Parquet grows. Bad rows go to quarantine. |
| 5 | Fri 25 Sep | Spark streaming job, part 2: windowed aggregates, fleet snapshot, idle detection, upsert into Postgres. | Realtime tables update every few seconds. Idle alerts appear. |
| 6 | Sat 26 Sep | Airflow DAG: sensor, validate, load, join, profitability, report file. | One full simulated day produces a correct profitability report. Re-run gives the same result. |
| 7 | Sun 27 Sep | FastAPI endpoints plus Grafana (or simple HTML) dashboard. | All endpoints return correct data. Dashboard shows live numbers. |
| 8 | Mon 28 Sep | Observability: JSON logging everywhere, Prometheus metrics, alert rules. Write a few automated tests (zone lookup, validation, profit calculation). | Alert fires when the simulator is stopped. Tests pass. |
| 9 | Tue 29 Sep | Full end-to-end run from a clean machine. Take screenshots. Write the report. Fix bugs. | Report draft complete with figures. README tested from scratch. |
| 10 | Wed 30 Sep | Record demo video, finish the report, write the contributions statement (if group), final checks, submit. | Repo link, PDF report and video are ready. |

If you have less than 10 days, cut in this order: Grafana (keep API plus HTML report), then extra tests, then the 3-day trend flag (keep profit below zero). Do not cut Airflow, Kafka, Spark or the observability alert. Those are direct requirements.

If you have more than 10 days, add: Grafana dashboards, a dead-letter Kafka topic, and a short load test to strengthen the "production scale" section.

## 9. Suggested repository layout

```
fleet-pipeline/
├── docker-compose.yml
├── .env.example
├── README.md
├── common/              shared logging, config, zone lookup, validation, revenue logic
├── simulators/          trip_simulator.py, cost_simulator.py
├── streaming/           spark_stream_job.py
├── airflow/dags/        daily_profitability_dag.py
├── api/                 FastAPI app
├── sql/                 schema and init scripts
├── monitoring/          prometheus.yml, alert rules, grafana dashboards
├── tests/
└── docs/                architecture diagram, screenshots, sample report
```

## 10. Report outline (8 to 15 pages, 15 marks)

| Section | Pages | Content |
|---|---|---|
| 1. Use case and requirements | 1 | Scenario, business question, what you built, simulated clock and assumptions. |
| 2. Architecture decision | 2 to 3 | Lambda vs Kappa against latency, replay, cost and consistency. Rejected alternative and honest trade-offs. |
| 3. Architecture diagrams | 1 to 2 | One overview diagram, one data-flow diagram covering ingestion, processing, storage and serving. |
| 4. Technology stack | 1 to 2 | Each major component, why it was chosen, and what was rejected, tied to this use case. |
| 5. Implementation | 2 to 3 | Simulators, streaming job, Airflow DAG, data model, key logic. |
| 6. Observability | 1 to 2 | What is measured, how, why, and the alert rules. |
| 7. Results | 2 | Screenshots: API output, dashboard, profitability report, alert firing, Airflow run. |
| 8. Limitations and production view | 1 | Single-node Kafka, no schema registry, local disk instead of S3, small data volume, exactly-once caveats, security, what you would change at scale. |
| Appendix | | Individual contributions (if group), how to run. |

Write the report as you build. Save screenshots each day so you are not scrambling on day 9.

## 11. How the work maps to the marking rubric

| Criterion | Marks | What earns it |
|---|---|---|
| Architecture decision (Lambda vs Kappa) | 20 | A real argument that names latency, replay, cost and consistency, plus a fairly treated rejected option. Biggest single item, so spend real time on it. |
| Technology stack | 10 | Reasons tied to fleet data and the two sources, not "it is popular". |
| Data ingestion | 15 | Both simulators work, run on the simulated clock, handle bad data, use partitioning by vehicle_id. |
| Processing | 15 | Meaningful transforms: cleaning, dedupe, zone enrichment, windowing, the join between sources, profit logic. Streaming and batch match the declared architecture. |
| Storage and serving | 10 | Postgres schema suits the queries. API returns correct live numbers. Report is produced daily. |
| Observability | 10 | Structured logs at each stage, metrics, at least one working alert, shown in the demo. |
| Report | 15 | Clear diagrams, honest limitations, screenshots. |
| Code quality and docs | 5 | Modular code, config in .env, README that works from a clean start, Docker Compose. |

## 12. Demo script (5 to 10 minutes)

1. 30 seconds: state the use case and the Lambda choice.
2. `docker compose up` and show the services running.
3. Start the simulators. Show Kafka events flowing.
4. Show the live API and dashboard changing (active vehicles, idle ratio, earnings by zone).
5. Show a simulated day end: the cost file arrives, the Airflow DAG runs, the profitability report appears with unprofitable vehicles flagged.
6. Show logs and Prometheus metrics.
7. Stop the trip simulator and show the "no data" alert firing.
8. Close with limitations in 30 seconds.

## 13. Risks and how to handle them

| Risk | What to do |
|---|---|
| Laptop runs out of memory | Small Spark settings, LocalExecutor for Airflow, drop Grafana if needed. Keep the vehicle count around 30 to 50. |
| Spark-Kafka-Postgres connector version problems | Pin every image and jar version in Docker Compose on day 2. Do not upgrade later. |
| Simulated time makes windows behave oddly | Use event time from the simulator, keep the watermark small (a few simulated minutes) and test with one zone first. |
| Batch job and stream disagree | Reuse the same revenue and zone functions from common/. Compare the streaming daily earnings with the batch earnings as a sanity check. |
| Airflow reads a half-written file | Write the cost file with a temp name, then rename. |
| Running out of time on the report | Write a section every day. Screenshots on day 8 and 9 at the latest. |
| Viva questions | Be ready to explain: why partition by vehicle_id, what a watermark does, how dedupe works, why the batch job is idempotent, what happens if Kafka or Spark restarts (checkpointing), and why Lambda over Kappa. |

## 14. Final submission checklist

- [ ] Git repo link (or zip) with the full code
- [ ] README: architecture summary, setup, run steps, how to reproduce results
- [ ] Docker Compose works from a clean clone
- [ ] Automated tests pass
- [ ] Report PDF, 8 to 15 pages, all sections above
- [ ] Simulated clock and all assumptions stated clearly
- [ ] Demo video (5 to 10 minutes) showing the pipeline end to end plus the alert
- [ ] Individual contributions statement (if a group)
- [ ] You can explain every core line of logic in a viva
