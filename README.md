# Hospital Patient Vital Signs Monitoring — Lambda Architecture Pipeline

**EC8203 Applied Big Data Engineering — Mini Project**
**Use Case 2:** A hospital ward wants continuous, near-real-time monitoring of patient
vitals from bedside sensors, correlated daily with lab results uploaded once a day by
the pathology lab.

> Business question this system answers: *Which patients show concerning vital-sign
> trends right now, and how do yesterday's lab results change the risk picture for
> those patients going forward?*

Full design rationale (architecture decision, tech stack justification, phase-by-phase
build plan and verification steps) is in [`PLAN.md`](PLAN.md). This README covers how to
run it.

---

## 1. Architecture at a glance

**Lambda architecture.** Two structurally different sources — a continuous vitals
stream and a once-a-day lab file — are processed on two paths that share one clinical
scoring module, and are reconciled in a serving layer:

```
                         +----------------------+
 bedside monitors  --->  |  Kafka: vitals.raw   | ---> Spark Structured Streaming
 (vitals_producer)       | (3 partitions,       |      (stream_job.py -- SPEED LAYER)
                         |  keyed by patient)    |        |
                         +----------+-----------+        v
                                    |              speed.patient_vitals_1m
                                    |              speed.patient_risk_now
                                    v              speed.clinical_alerts
                          Spark (raw_archiver.py)         |
                                    |                      |
                                    v                      |
                    data/master/vitals/ (Parquet,          |
                    partitioned dt=/hour=)                 |
                    -- the immutable MASTER DATASET --      |
                                    |                        |
                                    v                        v
 pathology lab   --->  data/landing/labs/  --->  Airflow DAG ---> batch_job.py
 (lab_batch_producer)  labs_{sim_day}.csv        (daily_patient_risk_dag)  (BATCH LAYER,
                                                           |                 recomputes from
                                                           v                 the master dataset)
                                               batch.patient_daily_vitals
                                               batch.patient_lab_results
                                               batch.patient_risk_daily
                                                           |
                                    +----------------------+
                                    v
                    serving.patient_360 (materialized view -- THE SERVING LAYER,
                    merges "now" from speed.* with "as of yesterday" from batch.*)
                                    |
                      +-------------+--------------+
                      v             v              v
               FastAPI (serving/api.py)   Grafana dashboards   daily HTML/CSV report
```

`common/clinical.py` (the NEWS2-style risk scoring, validation and lab-flagging logic)
is imported by **both** `processing/stream_job.py` and `processing/batch_job.py` — one
implementation, two execution contexts. This is the project's answer to Lambda's classic
"two codebases" criticism.

See [`PLAN.md` section 0.1](PLAN.md) for the full Lambda-vs-Kappa justification.

## 2. Technology stack

| Layer | Choice |
|---|---|
| Ingestion | Apache Kafka 3.9 (KRaft mode) |
| Stream processing | Spark Structured Streaming (PySpark 3.5) |
| Batch processing | Spark (batch mode), same codebase |
| Orchestration | Apache Airflow 2.10 (LocalExecutor) |
| Master dataset | Parquet on a mounted volume |
| Serving store | PostgreSQL 16 |
| Serving API | FastAPI + Uvicorn |
| Dashboards | Grafana 11 |
| Metrics / alerting | Prometheus + Alertmanager |
| Logging | structlog (JSON lines) |
| Reproducibility | Docker Compose |

Justification for every row against this specific use case: [`PLAN.md` section 0.2](PLAN.md).

## 3. Simulated clock

```
1 simulated day        = 5 real minutes   (SIM_DAY_SECONDS=300 in .env)
Vitals emitted         every 2 real seconds per patient (12 patients, about 6 events/s)
Lab file dropped       at the end of each simulated day
Airflow DAG schedule   */5 * * * *  (processes "yesterday's" simulated day)
```
A 60-minute demo session covers roughly 12 simulated days.

**Note (see `PLAN.md` section 0.3 for the full rationale):** vitals events carry a real
wall-clock `event_ts` (so Spark's watermark/windowing stay meaningful), while a separate
`sim_day` label on every vitals event and lab row drives the daily-batch cadence — the
Parquet master dataset's partitioning, the lab file's filename, and which day Airflow
processes. Streaming runs on real time; the daily-batch cycle runs on the compressed
simulated calendar.

## 4. Prerequisites

- **Docker Desktop** (Windows: WSL2 backend). Settings -> Resources -> **Memory at least
  8 GB**, CPUs at least 4. Spark + Airflow + Kafka together will be OOM-killed under
  about 6 GB.
- Git, with the repo's `.gitattributes` respected (already configured) — this avoids the
  classic Windows "CRLF breaks a Linux container's entrypoint script" failure.
- Nothing else needs to be installed on the host: every Python process (producers,
  Spark jobs, the API) runs inside containers on Python 3.11. Host Python is only used
  for optional dev-side unit tests (`pip install -r requirements-dev.txt`).

## 5. Running it

```bash
git clone <this-repo-url> && cd <repo>
cp .env.example .env          # adjust only if a port conflicts with something else
docker compose up -d
docker compose ps             # everything should reach healthy/running within ~60s
```

### Ports

| Port | Service |
|---|---|
| 29092 | Kafka (external listener, for host CLI tools) |
| 8080  | Kafka UI |
| 5432  | PostgreSQL |
| 9090  | Prometheus |
| 9093  | Alertmanager |
| 3000  | Grafana (`admin` / `admin` by default — see `.env`) |
| 8001  | Vitals producer Prometheus metrics |
| 8002  | Lab producer Prometheus metrics |
| 4040  | Spark UI / metrics (spark-stream) |
| 8081  | Airflow webserver (`admin` / `admin`) |
| 8000  | Serving API (`/docs` for interactive OpenAPI docs) |

### Tearing down

```bash
docker compose down          # stop everything, keep volumes (Kafka/Postgres/Grafana data)
docker compose down -v       # stop and wipe all volumes -- use for a true cold-start test
```

## 6. Current status / how to verify each phase

This project is built in five phases, each with its own hard verification gate before
moving on. See [`PLAN.md`](PLAN.md) for the full checklist of each phase. Quick summary:

- **Phase 1 -- Foundation** (done): Kafka (KRaft), Postgres (with `speed`/`batch`/`serving`/
  `ops` schemas), Prometheus, Alertmanager, Grafana (datasources provisioned), all on one
  Docker network, all started by `docker compose up -d`.
  Verify: `docker compose ps` (all healthy), topics exist (`kafka-topics.sh --describe`),
  schemas exist (`psql -c "\dn"`), UIs reachable at the ports above.
- **Phase 2 -- Ingestion** (done): `vitals-producer` (baseline + random-walk vitals for
  12 patients, injected anomaly episodes logged to `data/ground_truth.jsonl`, dirty/
  malformed/late-event injection, idempotent keyed Kafka delivery, Prometheus metrics on
  :8001), `lab-producer` (atomic daily CSV drop, anomaly-correlated lab values, injected
  data-quality issues), `spark-archiver` (Kafka -> Parquet master dataset).
  Verify: `docker compose up -d vitals-producer lab-producer spark-archiver`, then run
  `scripts/check_partitioning.py` and `scripts/sample_topic.py` from the host (see
  `PLAN.md` section 2.2 for the full checklist). Pure-Python logic (the random walk,
  anomaly injection, lab correlation, data-quality injection, the simulated clock) is
  covered by `tests/test_ingestion.py`, runnable without Docker: `pytest tests/ -v`.
- **Phase 3 -- Speed layer** (done): `spark-stream` runs three Structured Streaming
  queries sharing the `vitals.raw` topic -- malformed payloads to `vitals.dlq`;
  valid readings cleaned (`common/clinical.py` validation), watermarked +
  deduplicated (`dropDuplicates` on `event_id`), written to `speed.vitals_clean` and
  republished to `vitals.clean`; and a 1-minute/30s-slide windowed aggregation scored
  with a worst-case-in-window NEWS2 variant into `speed.patient_vitals_1m` /
  `speed.patient_risk_now`, with sustained (>=2 consecutive breached windows) alerts
  into `speed.clinical_alerts` and the `clinical.alerts` topic. All Postgres writes
  are idempotent upserts. The shared scoring/validation logic lives in
  `common/clinical.py` (imported by both this layer and the batch layer -- Lambda's
  answer to the "two codebases" criticism) and is covered by 35 pytest cases.
  Verify: `docker compose up -d spark-stream`, then `pytest tests/test_clinical.py -v`
  (no Docker needed) and the host scripts `scripts/verify_speed_vs_raw.py`,
  `scripts/verify_alerts.py`, `scripts/measure_latency.py` (see `PLAN.md` section 3.2
  for the full checklist, including the restart-idempotency check).
- **Phase 4 -- Batch layer + Airflow** (done): `processing/batch_job.py` recomputes a
  simulated day from scratch, straight from the Parquet master dataset -- worst-of-day
  NEWS2 scoring (`common.clinical.news2_score`, computed via Spark's `max_by()`), the
  day's lab file cleaned with five row-level data-quality rules (bad rows quarantined,
  not the whole file rejected), and **the join**: `common.clinical.adjust_risk_with_labs`
  combines the two into an adjusted risk score + reason codes + day-over-day delta,
  written with a delete-then-insert per sim_day inside one transaction (idempotent
  re-runs/backfills). `orchestration/dags/daily_patient_risk_dag.py` orchestrates it
  (wait for the lab file -> validate -> confirm master data -> run the batch job -> DQ
  gate -> build the HTML/CSV report -> refresh `serving.patient_360`), and
  `pipeline_health_dag.py` runs a separate heartbeat/freshness health check every 2
  minutes. No Airflow Connections are configured -- every DB access goes through
  `common.db`/`config.settings`, so there's zero manual Airflow UI setup.
  Verify: `docker compose up -d airflow-webserver airflow-scheduler` (UI on :8081,
  `admin`/`admin`), then `airflow dags trigger daily_patient_risk`; pure-Python
  join/scoring logic is covered by `pytest tests/test_batch_transforms.py` (no Docker
  needed). See `PLAN.md` section 4.2 for the full checklist, including the
  idempotency/backfill recompute demo.
- **Phase 5 -- Serving, observability, docs** (done): `serving/api.py` (FastAPI) --
  `/health`/`/ready`, `/ward/status`, `/patients`, `/patients/{id}/risk` (the merged
  realtime + lab-adjusted answer), `/patients/{id}/vitals`, `/alerts/active` +
  `/alerts/{id}/ack`, `/reports/daily/{sim_day}`, `/internal/alertmanager` webhook
  receiver, and `/metrics` (per-route HTTP metrics via
  prometheus-fastapi-instrumentator, sharing a registry with custom pipeline-health
  gauges a background poller republishes from Postgres every 15s). Every request
  carries an `X-Trace-Id`, logged as structured JSON -- grep one across
  `data/logs/*.jsonl` to see a reading's whole journey. `observability/alerts.yml` has
  7 rules, every one backed by a metric this codebase actually exports (no rules
  stubbed against unimplemented instrumentation -- see that file's header comment for
  what's deliberately out of scope and why). Two Grafana dashboards
  (`observability/grafana/dashboards/`) are auto-provisioned: Ward Clinical Dashboard
  (Postgres) and Pipeline Health Dashboard (Prometheus).
  Verify: `docker compose up -d api`, then `curl localhost:8000/health` and open
  `localhost:8000/docs`; Grafana dashboards at `localhost:3000` should appear with zero
  manual setup on a fresh `docker compose down -v && up`. See `PLAN.md` section 5.4 for
  the full checklist (deliberately triggering each alert rule, tracing a record
  end-to-end, the cold-start reproducibility timing).

## 7. Repository layout

See [`PLAN.md` section 2](PLAN.md) for the full annotated tree. Short version:

```
config/        settings.py (single source of config truth), patients.csv, thresholds.yml
common/        shared logging + clinical scoring logic (imported by speed AND batch)
ingestion/     the two simulated data sources (streaming + daily batch)
processing/    Spark jobs: stream_job.py, raw_archiver.py, batch_job.py
orchestration/ Airflow DAGs
serving/       FastAPI app + daily report generator
observability/ Prometheus / Alertmanager / Grafana provisioning
sql/           Postgres schema + table DDL, run automatically on first boot
scripts/       one-off verification helpers used in the phase gates
tests/         pytest unit tests (pure Python, run without Docker)
data/          gitignored; bind-mounted runtime data (landing files, Parquet, logs, reports)
```

## 8. Troubleshooting

- **A container keeps restarting with an exec/format error** -- almost always CRLF line
  endings on a shell script. Confirm `.gitattributes` is in effect (`git config
  core.autocrlf` should not be forcing CRLF) and re-checkout the file.
- **Containers get OOM-killed (exit code 137)** -- raise Docker Desktop's memory limit
  (Settings -> Resources) to at least 8 GB.
- **Port already in use** -- check `netstat -ano | findstr :<port>` and either stop the
  conflicting process or change the host-side port mapping in `docker-compose.yml`.
- **Slow Parquet writes / general sluggishness** -- if the repo lives under `/mnt/<drive>`
  from WSL's perspective, bind-mount I/O is slow; consider moving the repo into the WSL
  filesystem, or accept the overhead for a project of this size.
- **`docker compose exec postgres psql ...` says role "postgres" does not exist** --
  the Postgres volume was initialized before `.env` was finalized; run
  `docker compose down -v` once to reinitialize (only during setup -- this wipes data).

## 9. Assumptions & simplifications

- Parquet on a local bind-mounted volume stands in for HDFS/S3 (stated explicitly, per
  the assignment's note that this is an acceptable substitution to justify in the report).
  Kafka is a single broker (KRaft, RF=1) -- adequate for a laptop demo, not production.
- No PHI security/encryption/audit trail is implemented; this is simulated data, but a
  real clinical system would require this from day one. Discussed in the report's
  Limitations section.
- Airflow uses `LocalExecutor` (single-machine); a production deployment would use
  `CeleryExecutor` or a Kubernetes executor.

## 10. Report & demo video

- Report outline: [`docs/report/OUTLINE.md`](docs/report/OUTLINE.md) -- structure only,
  mapped to the marking rubric; fill in the analysis and screenshots yourself (the
  assignment's viva requirement means you need to be able to defend it).
- Architecture diagrams: [`docs/architecture.md`](docs/architecture.md) (two Mermaid
  diagrams -- render via GitHub/VS Code preview or [mermaid.live](https://mermaid.live)
  and paste the images into the report; don't screenshot the raw Markdown).
- Demo video storyboard: see [`PLAN.md` section 5.3](PLAN.md).
