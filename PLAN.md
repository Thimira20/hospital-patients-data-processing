# EC8203 Mini Project — Implementation Plan
**Use Case 2 — Hospital Patient Vital Signs Monitoring**
**Architecture: Lambda** · **Target: Windows 10 laptop (Docker Desktop + WSL2)**

---

## 0. Decisions locked before any code is written

### 0.1 Architecture decision (Rubric: 20 marks — the single biggest item)

**Chosen: Lambda architecture.** Batch layer + speed layer + serving layer, over an immutable master dataset.

Argument to defend in the report (write it as prose, not bullets):

| Requirement from the use case | Why it pushes to Lambda |
|---|---|
| **Two structurally different sources.** Vitals arrive every few seconds from bedside monitors; lab results arrive **once per day** as a bulk file from pathology. | The lab feed is inherently batch. Kappa would force you to fabricate a stream out of a daily file (log-ify it into Kafka) purely to satisfy the architecture — added complexity, no benefit. Lambda lets each source use its natural path. |
| **Two different latency SLAs.** "Which patients are concerning *right now*" = seconds. "How do yesterday's labs change the risk picture" = once per day. | Lambda's explicit split maps 1:1 onto these two questions. Speed layer answers Q1, batch layer answers Q2, serving layer merges them. |
| **Correctness of the clinical record.** Late-arriving vitals, corrected lab values, re-issued lab files (labs *do* get amended). | The batch layer **recomputes views from scratch** from the immutable master dataset every simulated day, so any approximation, dropped late event, or bad record in the speed layer is corrected within one batch cycle. This is Lambda's core guarantee (Marz's "human fault-tolerance"). In a clinical context an eventually-authoritative record matters more than in ad-tech. |
| **Replay / reprocessing.** Change the risk-score formula (e.g. adopt a new NEWS2 revision) and you must restate history. | Master dataset in Parquet, immutable and append-only → full recompute is a batch re-run, not a Kafka-retention gamble. Kafka retention is finite; 7-day retention cannot replay a 6-month admission history. |
| **Cost / hardware.** A single student laptop. | Batch runs 1× per simulated day over compacted Parquet; the streaming job holds only small windowed state. Kappa would replay the *entire* history through the streaming engine on every logic change — far more expensive per recompute on constrained hardware. |

**Rejected alternative — Kappa** (be specific; this is where marks are won): Kappa is attractive because it removes Lambda's "two codebases for the same logic" problem and is simpler operationally. We rejected it because (a) the lab feed is a genuine daily batch, so Kappa's "everything is a stream" premise is artificial here; (b) unbounded replay of a full patient history through Structured Streaming is bounded by Kafka retention, which we would have to size to the whole admission history; (c) correcting an amended lab result in a pure streaming model needs a retraction/upsert protocol Structured Streaming does not give for free.

**Honest trade-off we accept:** logic duplication between speed and batch layers (the risk-score function). **Mitigation:** factor the scoring logic into one shared Python module `common/clinical.py` imported by *both* the streaming job and the batch job — one implementation, two execution contexts. Put this in the report; it directly answers the standard Lambda criticism.

### 0.2 Technology stack (Rubric: 10 marks — justify against *this* use case, not popularity)

| Layer | Choice | Justification tied to the use case |
|---|---|---|
| Ingestion | **Apache Kafka 3.9 (KRaft, no ZooKeeper)** | Bedside monitors are many independent producers; Kafka decouples them from processing so a Spark restart never loses a reading. Topic `vitals.raw` keyed by `patient_id` across **3 partitions** → per-patient ordering guaranteed, which trend detection requires. KRaft = one less container, less laptop RAM. |
| Stream processing | **Spark Structured Streaming (PySpark 3.5)** | Native event-time windowing + watermarks handle out-of-order sensor readings (a real property of bedside telemetry). Chosen over Storm because Storm has no built-in event-time windowing, no stateful aggregation primitives, and no DataFrame/SQL API — we would hand-roll all three. Also lets one engine serve both Lambda layers. |
| Batch processing | **Spark (batch mode), same codebase** | Same engine as the speed layer = shared transformation code, one dependency set, one skill set. |
| Orchestration | **Apache Airflow 2.10 (LocalExecutor)** | The daily job has real dependencies: wait for the lab file → validate → recompute → report. Airflow gives sensors, retries, backfill (= Lambda recompute) and a UI that is itself an observability surface. |
| Master dataset | **Parquet on a mounted volume, partitioned `dt=/hour=`** | Immutable append-only raw store — the backbone of Lambda. Columnar + predicate pushdown makes a full recompute fast. Stands in for HDFS/S3; state this simplification in the report. |
| Serving store | **PostgreSQL 16** | Serving queries are small indexed per-patient lookups and short time-range scans, **joined across batch and speed views** — relational is the right shape. Cassandra rejected: we need cross-view joins and ad-hoc SQL for the report; Cassandra's query-first modelling would force denormalisation per question. Postgres also hosts the Airflow metadata DB (fewer containers). |
| Serving API | **FastAPI + Uvicorn** | Required "API endpoint for real-time ward metrics". Async, auto OpenAPI docs at `/docs` (a free demo surface), `prometheus-client` integrates in ~5 lines. |
| Dashboard | **Grafana 11** | Reads Postgres directly (no frontend to build) *and* Prometheus — the clinical dashboard and the pipeline-health dashboard live in one tool. |
| Metrics/alerting | **Prometheus + Alertmanager** | Pull-based scraping of every stage; alert rules as code (`alerts.yml`) — exactly what the rubric asks to see. |
| Logging | **structlog → JSON lines** | Structured logs with a `trace_id` carried producer → Kafka header → Spark → Postgres → API, which makes "tracing across pipeline stages" real rather than claimed. |
| Reproducibility | **Docker Compose** | Rubric explicitly rewards it. One `docker compose up`. |

### 0.3 Simulated clock (state in README *and* report)

```
1 simulated day        = 5 real minutes
Vitals emitted         every 2 real seconds per patient (12 patients ≈ 6 events/s)
Lab file dropped       at the end of each simulated day (every 5 real minutes)
Airflow DAG schedule   */5 * * * *  (processes "yesterday's" simulated day)
A 60-minute session    ≈ 12 simulated days
```

**Implementation refinement (decided during Phase 2 build, worth a paragraph in the
report's Assumptions section):** the pipeline actually runs on *two* independent
clocks, not one. Each vital reading's `event_ts` is a **real wall-clock timestamp**
(events really do arrive ~2 seconds apart), because Structured Streaming's watermark
(45s) and windows (1 min, sliding 30s) are only meaningful if defined against a
timeline that advances at the same rate events are produced. If `event_ts` instead used
the compressed simulated calendar, two events 2 real-seconds apart would land ~9.6
simulated *minutes* apart (at the default 5-min-per-day compression) — blowing past any
sane watermark and leaving most windows empty. Separately, every vitals event and lab
row also carries a **`sim_day` label** (e.g. `"2026-01-03"`) from `SimClock`, which
advances every `SIM_DAY_SECONDS` real seconds — this is what actually drives the Parquet
master dataset's `dt=` partitioning, the daily lab file's cadence/filename, and which
day the Airflow DAG/backfill processes. In short: **streaming/windowing runs on real
time; the daily-batch cadence runs on the compressed simulated calendar.**

### 0.4 The business question, decomposed into concrete outputs

> *"Which patients show concerning vital-sign trends right now, and how do yesterday's lab results change the risk picture for those patients going forward?"*

1. **Speed layer** → `speed.patient_vitals_1m` (per-patient windowed aggregates), `speed.patient_risk_now` (current NEWS2-style score + trend direction), `speed.clinical_alerts`.
2. **Batch layer** → `batch.patient_daily_vitals` (authoritative full-day aggregates), `batch.patient_lab_results` (reference-range-flagged), `batch.patient_risk_daily` (**the join**: vitals trend + lab abnormality → adjusted risk band + reason codes + day-over-day delta).
3. **Serving layer** → `GET /ward/status`, `GET /patients/{id}/risk`, `GET /alerts/active`, `GET /reports/daily/{sim_day}`; a Grafana ward dashboard; a generated daily report file (HTML + CSV) in `data/reports/`.

---

## 1. Prerequisites (before Phase 1)

1. **Enable WSL2** — PowerShell as Admin: `wsl --install`, then reboot.
2. **Install Docker Desktop for Windows** → Settings → General → *Use the WSL 2 based engine*; Resources → **Memory ≥ 8 GB, CPUs ≥ 4, Swap 2 GB**. Spark + Airflow + Kafka will be OOM-killed under 6 GB.
3. Prefer a path with **no spaces**. `D:\8th sem\Big data\project` works but every volume path must be quoted; moving to `D:\bigdata-project` removes a whole class of problems.
4. `git init`, plus `.gitattributes` containing `* text=auto eol=lf` and `*.sh text eol=lf`. **CRLF line endings in shell scripts breaking Linux containers is the #1 Windows failure here.**
5. Your host has Python 3.13 and Java 21. **All Python runs inside containers on 3.11** — PySpark 3.5 does not support 3.13. Host Python is only for small helper scripts.

**Plan B if Docker Desktop is not possible:** run the whole stack inside one WSL2 Ubuntu 22.04 distro with native installs (Kafka tarball in KRaft mode, `pip install pyspark apache-airflow`, `apt install postgresql`). Everything else in this plan is unchanged; `docker compose up` becomes `scripts/start_all.sh`. Do **not** attempt native Windows: Airflow has no Windows support, and Spark needs `winutils.exe`/`hadoop.dll` hacks.

---

## 2. Target repository layout

```
project/
├── docker-compose.yml
├── .env  .env.example  .gitattributes  .gitignore
├── README.md
├── config/
│   ├── settings.py            # pydantic-settings, single source of truth
│   ├── patients.csv           # patient_id,name,age,sex,ward,bed,admitted_at,baseline_hr,baseline_spo2
│   └── thresholds.yml         # clinical thresholds + NEWS2 bands
├── docker/
│   ├── spark.Dockerfile  airflow.Dockerfile  app.Dockerfile
├── common/
│   ├── logging_setup.py       # structlog JSON config + trace_id
│   ├── clinical.py            # SHARED risk logic (speed + batch import this)
│   ├── schemas.py             # pydantic models + Spark StructTypes
│   └── db.py                  # SQLAlchemy engine + helpers
├── ingestion/
│   ├── vitals_producer.py     # streaming source -> Kafka
│   ├── lab_batch_producer.py  # daily source -> data/landing/labs/
│   └── sim_clock.py
├── processing/
│   ├── stream_job.py          # Structured Streaming (speed layer)
│   ├── raw_archiver.py        # Kafka -> Parquet master dataset
│   └── batch_job.py           # Spark batch (batch layer), --sim-day
├── orchestration/dags/
│   ├── daily_patient_risk_dag.py
│   └── pipeline_health_dag.py
├── serving/
│   ├── api.py                 # FastAPI
│   └── report_builder.py      # HTML + CSV daily report
├── observability/
│   ├── prometheus.yml  alerts.yml  alertmanager.yml
│   └── grafana/provisioning/{datasources,dashboards}/
├── sql/  001_schemas.sql 002_speed_tables.sql 003_batch_tables.sql 004_ops_tables.sql
├── scripts/                   # verification helpers used in each phase gate
├── tests/  test_clinical.py test_schemas.py test_batch_transforms.py
├── data/   landing/labs/  master/vitals/  checkpoints/  reports/  logs/   (gitignored)
└── docs/   architecture.drawio|png   report/
```

---

# PHASE 1 — Foundation & Infrastructure

**Goal:** every container starts, services talk to each other, shared plumbing (config, logging, DB schema) exists. No business logic yet.

### 1.1 Tasks

1. **`.env` + `config/settings.py`** — one `Settings` class (pydantic-settings) reading env vars: Kafka bootstrap, topic names, Postgres DSN, data paths, `SIM_DAY_SECONDS=300`, `VITALS_INTERVAL_SECONDS=2`, `NUM_PATIENTS=12`, `ANOMALY_PROBABILITY=0.05`. **No magic numbers anywhere else in the codebase** — this is a chunk of the "configuration management" marks.
2. **`docker-compose.yml`** services:
   - `kafka` — `apache/kafka:3.9.1`, **KRaft single node**, listeners `PLAINTEXT://kafka:9092` (internal) and `EXTERNAL://localhost:29092` (host tools), `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`, healthcheck via `kafka-broker-api-versions.sh`.
   - `kafka-init` — one-shot container creating topics with explicit partitions: `vitals.raw` (**3 partitions**), `vitals.clean` (3), `clinical.alerts` (1), `vitals.dlq` (1, dead-letter). Exits 0.
   - `kafka-ui` — `provectuslabs/kafka-ui` on :8080 (valuable in the demo video).
   - `postgres` — `postgres:16`, `./sql` mounted to `/docker-entrypoint-initdb.d`, databases `vitals` and `airflow`.
   - `prometheus` (:9090), `alertmanager` (:9093), `grafana` (:3000, provisioned datasources).
   - Declare `spark-stream`, `spark-archiver`, `airflow-*`, `api` now but keep them behind `profiles:` so they don't start yet.
   - One bridge network; `depends_on: {condition: service_healthy}` throughout.
3. **`docker/spark.Dockerfile`** — `FROM python:3.11-slim`; install `openjdk-17-jre-headless procps`; `pip install pyspark==3.5.6 structlog psycopg2-binary pyyaml pydantic-settings prometheus-client`. **Critical:** pre-download connector jars at *build* time so runtime needs no internet:
   ```dockerfile
   RUN spark-submit --packages \
       org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.postgresql:postgresql:42.7.4 \
       --conf spark.jars.ivy=/opt/ivy /opt/warmup.py
   ```
   where `warmup.py` just builds a SparkSession and exits.
4. **`common/logging_setup.py`** — `configure_logging(service)` returning a structlog logger emitting **JSON lines** with `ts, level, service, stage, event, trace_id, patient_id, count, latency_ms`. Every service calls it at startup; logs go to stdout **and** `data/logs/{service}.jsonl`.
5. **`sql/*.sql`** — schemas `speed`, `batch`, `ops`; create the ops tables now: `ops.pipeline_heartbeat(service, stage, last_seen_at, records_processed, errors)`, `ops.data_quality(sim_day, stage, rule, passed, failed, checked_at)`, `ops.batch_runs(sim_day, dag_run_id, started_at, finished_at, status, rows_written)`, `ops.alerts_received(...)`.
6. **`config/patients.csv`** — 12 patients with the dimension columns listed above (used for enrichment joins).

### 1.2 ✅ VERIFICATION — Phase 1

```bash
# 1. Everything healthy
docker compose up -d && docker compose ps
#    kafka/postgres/prometheus/grafana/alertmanager = healthy ; kafka-init = exited (0)

# 2. Topics with correct partition counts
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic vitals.raw          # must show PartitionCount: 3

# 3. Round-trip a message
docker compose exec kafka bash -c "echo hello | /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic vitals.raw"
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic vitals.raw --from-beginning --max-messages 1 --timeout-ms 10000

# 4. Postgres schemas
docker compose exec postgres psql -U postgres -d vitals -c "\dn"      # speed, batch, ops
docker compose exec postgres psql -U postgres -d vitals -c "\dt ops.*"

# 5. Spark image works offline (proves jars are baked in)
docker compose run --rm --network none spark-stream python -c \
 "from pyspark.sql import SparkSession; s=SparkSession.builder.master('local[2]').getOrCreate(); print(s.version)"

# 6. Structured logging shape
docker compose run --rm spark-stream python -c \
 "from common.logging_setup import configure_logging; configure_logging('test').info('boot', stage='init', trace_id='abc')"
#    -> a single-line JSON object containing service/stage/trace_id

# 7. UIs reachable: :8080 Kafka UI, :9090 Prometheus, :3000 Grafana, :9093 Alertmanager
```

**Exit criteria:** all 7 pass, and `docker compose down && docker compose up -d` reproduces the same state. **Commit `feat: phase 1 infrastructure`.**

---

# PHASE 2 — Ingestion Layer (Rubric: 15 marks)

**Goal:** both simulated sources produce realistic, well-formed, *robust* data; the immutable master dataset starts filling.

### 2.1 Tasks

1. **`ingestion/sim_clock.py`** — `SimClock(start_epoch, sim_day_seconds)` with `.sim_now()`, `.sim_day()` → `YYYY-MM-DD`, `.day_progress()`. Producers and Airflow share the **same** start epoch, persisted in `data/sim_clock.json` so a restart doesn't reset simulated time.
2. **`ingestion/vitals_producer.py`** (streaming source):
   - Loads `patients.csv`; each patient gets a **baseline + random walk**, so readings are correlated over time rather than white noise — this is what makes trend detection meaningful.
   - Plausible ranges: HR 50–110, SpO2 92–100, systolic 95–140, diastolic 60–90, temp 36.1–37.5.
   - **Injected scenarios** (where the marks are): with probability `ANOMALY_PROBABILITY`, start a *sustained episode* on one patient lasting 30–90 s, drawn from `{tachycardia, desaturation, hypotension, fever, sepsis_pattern}`. The sepsis pattern moves HR↑, BP↓, temp↑ together. Append the injected ground truth to `data/ground_truth.jsonl` so you can *prove* detection worked in the demo.
   - **Deliberate dirty data (~2%)**: null `spo2`, out-of-range `heart_rate=300`, duplicate `event_id`s, and **late events** (timestamp back-dated 10–40 s) to demonstrate watermarking. Plus ~0.5% malformed JSON → must land in `vitals.dlq`.
   - Kafka `key = patient_id`; headers carry `trace_id` and `producer_ts`.
   - Producer config: `acks=all`, `enable.idempotence=True`, `linger.ms=50`, `compression.type=snappy`, retries, and a delivery-report callback incrementing a Prometheus counter.
   - `prometheus_client.start_http_server(8001)` exposing `vitals_events_emitted_total`, `vitals_emit_errors_total`, `vitals_anomalies_injected_total`, `vitals_emit_latency_seconds`.
   - Graceful SIGTERM shutdown (`producer.flush()`); `--rate` CLI override for load tests.
3. **`ingestion/lab_batch_producer.py`** (daily-batch source):
   - Sleeps until the end of each simulated day, then writes `data/landing/labs/labs_{sim_day}.csv` **atomically**: write `.tmp` then `os.replace()` — otherwise Airflow's sensor reads a half-written file. Also write a `labs_{sim_day}.csv.done` marker (the sensor watches the marker) and a `.manifest.json` (row count + checksum).
   - Columns: `patient_id, test_type, result_value, unit, reference_low, reference_high, collected_at, lab_batch_id`. Tests: `WBC, CRP, Lactate, Creatinine, Hemoglobin, Platelets, Troponin`.
   - **Correlate labs with the streamed anomalies**: a patient who had a sepsis episode that sim-day gets elevated WBC/CRP/Lactate. This is what makes the batch join produce a genuine insight instead of noise.
   - Include occasional missing patients, one duplicate row, one out-of-schema value per file → the batch layer's data-quality rules must catch these.
4. **`processing/raw_archiver.py`** — a *separate* Structured Streaming job whose only job is `vitals.raw → Parquet`, `partitionBy("dt","hour")`, `trigger(processingTime="30 seconds")`, append mode, its own checkpoint. Keep it separate from the speed layer so the master dataset survives any change to analytics logic. **This job writes the Lambda master dataset — say so in the report.**
5. Add `vitals-producer`, `lab-producer`, `spark-archiver` to compose with `restart: unless-stopped`.

### 2.2 ✅ VERIFICATION — Phase 2

```bash
docker compose up -d vitals-producer lab-producer spark-archiver

# 1. Events flowing, keyed, with headers
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic vitals.raw --property print.key=true --property print.headers=true --max-messages 5
docker compose exec kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 --topic vitals.raw     # all 3 partitions have offsets > 0

# 2. Partition affinity: each patient_id maps to exactly one partition
python scripts/check_partitioning.py            # consumes 200 msgs, asserts 1 partition per key

# 3. Throughput matches config: offsets grow ~360 msgs/60s (12 patients / 2s), ±10%

# 4. Dirty data really is present (Phase 3 depends on it)
python scripts/sample_topic.py --n 2000
#    expect ~2% null/out-of-range, ~0.5% malformed, some back-dated timestamps

# 5. Daily lab file appears once per simulated day
ls -l data/landing/labs/        # after ~11 min: two labs_*.csv + .done + .manifest.json
#    assert: no leftover .tmp files; manifest row count == actual row count

# 6. Master dataset filling
find data/master/vitals -name "*.parquet" | head
docker compose exec spark-archiver python -c "
from pyspark.sql import SparkSession; s=SparkSession.builder.master('local[2]').getOrCreate()
df=s.read.parquet('/data/master/vitals'); df.printSchema(); print(df.count()); df.groupBy('dt','hour').count().show()"
#    count within ~1% of total Kafka offsets (archiver is at-least-once)

# 7. Metrics
curl http://localhost:8001/metrics | grep vitals_events_emitted_total

# 8. Restart resilience  ← the real robustness test
docker compose restart kafka          # producer reconnects, logs retries, does not crash
docker compose restart spark-archiver # resumes from checkpoint, count keeps climbing, no dupes

# 9. Logs are valid JSON with trace_id
docker compose logs vitals-producer | tail -5 | python -m json.tool
```

**Exit criteria:** producers survive a Kafka restart, master-dataset count tracks Kafka, lab files are atomic and correlated with injected anomalies. **Commit `feat: phase 2 ingestion`.**

---

# PHASE 3 — Speed Layer, Spark Structured Streaming (Rubric: part of 15)

**Goal:** near-real-time cleaned vitals, windowed trends, current risk score and per-patient clinical alerts in Postgres, within seconds.

### 3.1 Tasks

1. **`common/clinical.py`** — the **shared** logic (your answer to the code-duplication criticism). Pure functions, **zero Spark imports**, so they are unit-testable, usable as a Spark UDF, and callable as plain Python in the batch job:
   - `validate_reading(row) -> (is_valid, reason)` — per-vital range checks.
   - `news2_score(hr, spo2, sbp, temp, rr=None) -> (score, band, components)` — NEWS2-style early warning score, bands `low/medium/high`. Cite NEWS2 in the report: it makes the transformation clinically credible rather than arbitrary.
   - `trend_direction(current, previous) -> improving|stable|deteriorating`.
   - `lab_flag(value, ref_low, ref_high) -> low|normal|high`.
   - `adjust_risk_with_labs(vitals_score, lab_flags) -> (adjusted_score, reason_codes)`.
2. **`processing/stream_job.py`** — one Spark app, several sinks:
   - Read `vitals.raw`, `startingOffsets=latest` (checkpointed thereafter), `maxOffsetsPerTrigger` from config for backpressure.
   - **Parse & split:** `from_json` with an **explicit schema** (never `inferSchema` on a stream). Parse failures → `vitals.dlq` with raw payload + error reason.
   - **Clean:** apply `validate_reading`; `dropDuplicates(["event_id"])` **with a watermark** — unbounded dedup state is the classic memory leak; mention this in the report.
   - **Enrich:** broadcast join with `patients.csv` (static DataFrame) adding `ward, bed, age, baseline_hr`. This is your **stream–static join**.
   - **Window:** `withWatermark("event_ts","45 seconds")` then `groupBy(window("event_ts","1 minute","30 seconds"), "patient_id")` → avg/min/max/stddev per vital + `reading_count`. Sliding windows give smooth trend detection.
   - **Score:** `news2_score` over the windowed aggregates → `speed.patient_risk_now` with `trend_direction`.
   - **Alert:** raise an alert only when a breach is **sustained across ≥2 consecutive windows** (stateful compare in `foreachBatch`, or `flatMapGroupsWithState`). This suppresses single-sample noise and is a genuinely defensible clinical design decision. Alerts → `speed.clinical_alerts` **and** the `clinical.alerts` topic.
   - **Sinks:** a single `foreachBatch` writing all Postgres targets, using **upsert** (`INSERT … ON CONFLICT DO UPDATE`) keyed on `(patient_id, window_start)` so reprocessed micro-batches are **idempotent** — effectively exactly-once into Postgres. The same function updates `ops.pipeline_heartbeat` (last_seen_at, batch_id, input rows, output rows, duration); that row is what the health-check alert watches.
   - Config: `spark.sql.shuffle.partitions=4` (laptop), `spark.ui.prometheus.enabled=true`, checkpoint in `data/checkpoints/stream/`.
3. **`sql/002_speed_tables.sql`** — `speed.vitals_clean`, `speed.patient_vitals_1m`, `speed.patient_risk_now`, `speed.clinical_alerts`, with primary keys and an index on `(patient_id, window_start DESC)`.
4. **`tests/test_clinical.py`** — table-driven pytest for `news2_score` boundaries, `validate_reading` on nulls/out-of-range, `lab_flag` edges. ≥15 cases.

### 3.2 ✅ VERIFICATION — Phase 3

```bash
pytest tests/test_clinical.py -v          # green BEFORE running the job
docker compose up -d spark-stream

# 1. Data landing continuously
watch -n5 'docker compose exec -T postgres psql -U postgres -d vitals -c \
 "select count(*), max(window_end) from speed.patient_vitals_1m;"'
#    count grows every ~30s; max(window_end) stays within ~90s of now

# 2. Aggregation CORRECTNESS — reconcile against the raw master dataset
python scripts/verify_speed_vs_raw.py --window <a completed window>
#    recomputes avg HR for one patient/window straight from Parquet and diffs against
#    speed.patient_vitals_1m -> must match within float tolerance

# 3. Cleaning actually happened
#    select count(*) from speed.vitals_clean where heart_rate > 250;   -- 0
#    select count(*) from speed.vitals_clean where spo2 is null;       -- 0
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic vitals.dlq --max-messages 3      # malformed payloads with an error reason

# 4. Dedup: select event_id,count(*) from speed.vitals_clean group by 1 having count(*)>1;  -- 0 rows

# 5. Alerts fire on the INJECTED anomalies  ← the money shot for the demo
python scripts/verify_alerts.py            # joins speed.clinical_alerts to data/ground_truth.jsonl
#    expect recall > 0.9 on sustained episodes and ~0 alerts from single-sample spikes
#    (proves the 2-window rule works). RECORD precision/recall for the report.

# 6. End-to-end latency
python scripts/measure_latency.py          # producer_ts (Kafka header) -> Postgres inserted_at
#    p95 < 90s given a 1-min window + 30s trigger. RECORD THIS NUMBER.

# 7. Idempotency / restart recovery ← key Lambda-layer proof
docker compose stop spark-stream           # note count(*) from speed.patient_vitals_1m
docker compose start spark-stream && sleep 120
#    select patient_id, window_start, count(*) ... group by 1,2 having count(*)>1;  -- 0 rows

# 8. Backpressure: run producer at 5x rate for 2 min — job must not crash and the
#    heartbeat gap must stay < 60s. Note the behaviour for the "limitations" section.

# 9. select * from ops.pipeline_heartbeat;   -- updating every micro-batch
```

**Exit criteria:** aggregates reconcile with raw Parquet, alerts match ground truth, no duplicates after restart, latency measured. **Commit `feat: phase 3 speed layer`.**

---

# PHASE 4 — Batch Layer + Airflow Orchestration (Rubric: part of 15 + 10 storage)

**Goal:** the authoritative daily recompute, the vitals↔labs join, and a real orchestrated pipeline with sensors, retries and idempotent re-runs.

### 4.1 Tasks

1. **`docker/airflow.Dockerfile`** — `FROM apache/airflow:2.10.5-python3.11`; as `root` `apt-get install -y openjdk-17-jre-headless`; back to the `airflow` user, `pip install pyspark==3.5.6 structlog pandas jinja2 psycopg2-binary`, and copy the same pre-cached ivy jars. Airflow runs `spark-submit` **in local mode inside its own container** — no Docker socket, no Spark cluster, no cross-container RPC. This is the most reliable option on Windows. *(Fallback for stronger isolation: `DockerOperator` with `//var/run/docker.sock` mounted — the double slash is required on Windows.)*
2. **`processing/batch_job.py`** — `spark-submit batch_job.py --sim-day 2026-01-05`:
   - **Read the master dataset** for that sim day (`spark.read.parquet(...).where(dt=…)`) — recomputing from immutable raw, **not** from the speed layer. State this explicitly in the report; it is the definition of the Lambda batch layer.
   - **Read the lab file** with an explicit schema, `PERMISSIVE` mode and a `_corrupt_record` column; corrupt rows → `data/quarantine/`.
   - **Data-quality rules** (results into `ops.data_quality`): row count > 0; no null `patient_id`; `result_value` numeric; every `patient_id` exists in `patients.csv`; no duplicate `(patient_id,test_type)` per day; `collected_at` inside the sim day. Fail the task on a **critical** rule; warn otherwise.
   - **Transform:**
     - (a) Full-day per-patient vitals aggregates: avg/min/max/stddev/p95, reading count, % of time in each NEWS2 band, longest deteriorating streak, hours containing abnormal readings.
     - (b) Lab results flagged with `lab_flag`, pivoted to one row per patient with `abnormal_test_count` and the list of abnormal tests.
     - (c) **The join** — `patient_daily_vitals ⟕ patient_lab_results` on `patient_id`, then `adjust_risk_with_labs(...)` → `adjusted_risk_score`, `risk_band`, `reason_codes[]` (e.g. `["sustained_tachycardia","lactate_high","crp_high"]`), plus a **day-over-day delta** against the previous day's batch view so the report can say *"deteriorating vs yesterday"*. This is the second half of the business question.
   - **Write to Postgres** with delete-then-insert **scoped to that sim_day inside one transaction**, so re-running a day is **idempotent** (required for backfill).
   - Log start/end/row counts to `ops.batch_runs`.
3. **`serving/report_builder.py`** — queries `batch.patient_risk_daily` + top alerts + ward rollups → a Jinja2 HTML report (embedded matplotlib PNG: ward risk distribution + per-patient HR sparkline) and a CSV, written to `data/reports/daily_risk_report_{sim_day}.{html,csv}`. **This is the "consolidated report" deliverable — screenshot it.**
4. **`orchestration/dags/daily_patient_risk_dag.py`** — `schedule="*/5 * * * *"`, `catchup=True`, `max_active_runs=1`, `retries=2`, `retry_delay=30s`, an SLA, and an `on_failure_callback` that writes a structured log and pushes to Alertmanager. Tasks:
   ```
   resolve_sim_day       PythonOperator, from SimClock ("yesterday" in sim time)
        ↓
   wait_for_lab_file     FileSensor on labs_{sim_day}.csv.done, poke=10s,
                         timeout=180s, mode="reschedule"      ← a real dependency
        ↓
   validate_lab_file     manifest checksum + row count
        ↓
   ensure_master_data    assert Parquet partition dt={sim_day} exists with >= min rows
        ↓
   run_batch_job         BashOperator: spark-submit batch_job.py --sim-day {{ ... }}
        ↓
   data_quality_gate     SQLCheckOperator against ops.data_quality
        ↓
   build_report          report_builder
        ↓
   refresh_serving_view  REFRESH MATERIALIZED VIEW serving.patient_360
        ↓
   publish_metrics       push batch freshness/rows to Prometheus Pushgateway
   ```
5. **`orchestration/dags/pipeline_health_dag.py`** — every 2 min: check `ops.pipeline_heartbeat` staleness, Kafka consumer-group lag, master-dataset freshness; write results to `ops.data_quality` and raise on breach.
6. **`serving.patient_360` materialized view** — merges the speed view (now) with the batch view (yesterday) per patient. **This materialized view *is* the Lambda serving layer** — put that sentence next to the diagram in the report.

### 4.2 ✅ VERIFICATION — Phase 4

```bash
docker compose up -d airflow-webserver airflow-scheduler     # UI on :8081 (admin/admin)

# 1. DAG parses
docker compose exec airflow-scheduler airflow dags list-import-errors    # empty
docker compose exec airflow-scheduler airflow dags list | grep daily_patient_risk

# 2. Task-level dry run
docker compose exec airflow-scheduler airflow tasks test daily_patient_risk run_batch_job 2026-01-05

# 3. Full manual run, all tasks green (watch the Graph view;
#    wait_for_lab_file must genuinely WAIT and then succeed)
docker compose exec airflow-scheduler airflow dags trigger daily_patient_risk

# 4. Batch tables populated AND hand-verified
#    select sim_day, count(*) from batch.patient_daily_vitals group by 1 order by 1;
#    select * from batch.patient_risk_daily order by adjusted_risk_score desc limit 10;
python scripts/verify_batch.py --sim-day <d> --patient P003
#    recomputes avg HR + lab flags in pandas from Parquet+CSV and diffs against Postgres

# 5. THE JOIN produced insight, not nulls
#    select count(*) from batch.patient_risk_daily where abnormal_test_count is null;  -- ~0
#    select patient_id, reason_codes from batch.patient_risk_daily where risk_band='high';
#    the sepsis patients from ground_truth.jsonl MUST appear with BOTH a vitals reason
#    code and a lab reason code -> this is your headline result

# 6. IDEMPOTENCY (key Lambda property)
docker compose exec airflow-scheduler airflow tasks clear -y -t run_batch_job -s <d> -e <d> daily_patient_risk
#    re-run -> identical row count for that sim_day, no duplicates

# 7. RECOMPUTE demo (record this for the video)
#    edit a threshold in config/thresholds.yml, then:
docker compose exec airflow-scheduler airflow dags backfill -s <d1> -e <d3> daily_patient_risk
#    batch views change, speed layer untouched -> Lambda's "batch corrects speed" story, live

# 8. Failure handling
#    delete a lab .done file  -> sensor times out -> retries -> failure callback + alert
#    corrupt a lab CSV row    -> quarantine file appears, ops.data_quality shows a failed rule

# 9. Report artifacts
ls -l data/reports/                                   # HTML + CSV per sim day
start data/reports/daily_risk_report_<d>.html         # renders correctly in the browser
```

**Exit criteria:** DAG green end-to-end, batch numbers hand-verified, re-run idempotent, backfill works, report renders. **Commit `feat: phase 4 batch layer + airflow`.**

---

# PHASE 5 — Serving API, Observability, Dashboards & Documentation (Rubric: 10 + 10 + 15 + 5)

### 5.1 Serving layer — `serving/api.py` (FastAPI, :8000)

| Endpoint | Answers |
|---|---|
| `GET /health` | liveness — Postgres + Kafka reachable |
| `GET /ready` | readiness — heartbeat fresh **and** serving view non-empty |
| `GET /ward/status` | **real-time ward figures**: patients monitored, readings/min, counts by risk band, active alerts, avg NEWS2 by ward |
| `GET /patients` | list with current risk band + trend arrow |
| `GET /patients/{id}/risk` | **the merged answer**: current speed-layer score *and* yesterday's lab-adjusted batch score + reason codes + delta |
| `GET /patients/{id}/vitals?window=30m` | recent windowed series for charts |
| `GET /alerts/active` | unacknowledged clinical alerts, newest first |
| `POST /alerts/{id}/ack` | acknowledge (shows the store is read-write) |
| `GET /reports/daily/{sim_day}` | serves the generated consolidated report |
| `POST /internal/alertmanager` | webhook receiver → logs pipeline alerts into `ops.alerts_received` |
| `GET /metrics` | Prometheus exposition |

Details that earn marks: pydantic response models, correct HTTP status codes, an `X-Trace-Id` middleware logging every request as structured JSON with duration, and `prometheus_fastapi_instrumentator` for per-route latency histograms.

### 5.2 Observability — three signals across five stages (put this table in the report)

| Stage | Logs | Metrics | Health / alert rule |
|---|---|---|---|
| Ingestion | JSON per N events; one line per error | `vitals_events_emitted_total`, `vitals_emit_errors_total`, `lab_file_written_total`, `lab_file_rows` | **NoVitalsIngested**: 5-min rate == 0 → critical. **LabFileMissing**: no file in 1.5 sim-days |
| Kafka | broker logs | consumer-group lag, topic offsets | **KafkaConsumerLagHigh**: lag > 5000 for 2 min |
| Stream processing | per-micro-batch JSON: batch_id, input rows, output rows, duration | Spark `PrometheusServlet` (`spark.ui.prometheus.enabled=true`) + heartbeat gauges exported by the API from `ops.pipeline_heartbeat` | **StreamStalled**: `now - last_seen > 120s` → critical. **StreamBatchSlow**: p95 batch duration > trigger interval |
| Batch / Airflow | Airflow task logs + `ops.batch_runs` | `batch_last_success_sim_day`, `batch_rows_written`, DAG duration via Pushgateway | **BatchLate**: no successful run in 12 min. **DataQualityFailed**: any critical rule failed |
| Serving | per-request JSON with trace_id + duration | request rate, p95 latency, 5xx ratio | **ApiErrorRateHigh**: 5xx > 5% for 5 min. **ServingDataStale**: `max(window_end)` older than 3 min |

- **Tracing:** `trace_id` generated in the producer → Kafka header → stored on `speed.vitals_clean` → returned in API responses. Demo: take a `trace_id` from an API response and `grep` it across `data/logs/*.jsonl` to show one record's whole journey. That satisfies the rubric's "tracing across pipeline stages" literally.
- **Alertmanager → webhook → `POST /internal/alertmanager`** → logged, stored, and shown on a Grafana panel. No email/Slack setup needed for the demo.
- **Grafana dashboards, provisioned as JSON in git (not clicked by hand):**
  1. **Ward Clinical Dashboard** (Postgres) — patients by risk band, live HR/SpO2 series per patient, active-alerts table, yesterday's lab-adjusted risk table.
  2. **Pipeline Health Dashboard** (Prometheus) — events/s in vs rows/s out, consumer lag, micro-batch duration, heartbeat age, batch freshness, API p95, firing alerts.

### 5.3 Documentation & report

- **README.md** — inline architecture diagram, prerequisites, `docker compose up -d`, a **demo script with exact timings** ("t+0 start · t+5m first lab file · t+6m first DAG run · t+8m dashboards populated"), how to reproduce every verification, a ports table, troubleshooting (WSL memory, port conflicts, CRLF), teardown.
- **`docs/architecture.png`** — draw **two** diagrams: (1) the layered Lambda diagram labelling master dataset / batch view / realtime view / serving layer; (2) a concrete component-deployment diagram showing containers, topics, tables and ports. Keep the draw.io/Excalidraw source in the repo alongside the PNG.
- **Report (8–15 pages), mapped onto the rubric:**
  1. Use case + business requirements interpreted, including both questions and their SLAs — 1 p
  2. **Lambda vs Kappa**, §0.1 table, explicit rejected alternative, honest code-duplication trade-off + mitigation — **3 p (this is 20 marks)**
  3. Technology stack, §0.2 table, each row tied to a *use-case constraint* — 1.5 p
  4. Architecture diagrams + a walk-through of one reading end-to-end — 2 p
  5. Implementation: schemas, transformations, the join, watermark/idempotency decisions — 2 p
  6. Observability design (§5.2 table) + what each alert protects against — 1.5 p
  7. **Results**: both Grafana dashboards, API `/docs` + a sample response, Airflow graph view, the generated daily report, alert precision/recall vs ground truth, **measured p95 latency** — 2 p
  8. Limitations & production-scale changes: single-broker Kafka with RF=1, local Parquet instead of S3/HDFS, Spark local mode, **no PHI security/encryption/audit trail** (say this — it is healthcare data), no schema registry (would add Avro + Confluent Schema Registry), no exactly-once into Kafka, LocalExecutor vs Celery/K8s, full-day rather than incremental batch recompute — 1.5 p
  9. Assumptions, simulated-time compression, individual contributions statement.
- **Demo video (5–10 min) storyboard:** 0:00 architecture + Lambda justification → 1:00 `docker compose up`, containers healthy → 2:00 Kafka UI, events across 3 partitions → 3:00 Grafana ward dashboard, inject an anomaly, alert appears live → 5:00 Airflow DAG run for a sim day → 6:30 the daily report showing a patient whose **risk band changed because of labs** → 8:00 kill the Spark job, show `StreamStalled` firing, restart, show recovery with no duplicates → 9:30 wrap.

### 5.4 ✅ VERIFICATION — Phase 5 (final acceptance)

```bash
# 1. API contract
curl -s localhost:8000/health | jq
curl -s localhost:8000/ward/status | jq
curl -s localhost:8000/patients/P003/risk | jq
#    MUST contain both a realtime block and a batch (lab-adjusted) block + reason_codes
#    open http://localhost:8000/docs -> every endpoint documented

# 2. The business question is genuinely answered. Pick a sepsis-episode patient and show:
#    - the speed layer flagged them within ~1 min (speed.clinical_alerts)
#    - the batch layer raised their band the next sim day (WBC/CRP/Lactate high)
#    - /patients/{id}/risk returns both, with a delta vs the previous day

# 3. Dashboards load from provisioning on a FRESH volume
docker compose down -v && docker compose up -d && sleep 420
#    both dashboards appear automatically with data; zero manual clicking

# 4. Alerts actually fire — trigger each rule deliberately
docker compose stop vitals-producer     # -> NoVitalsIngested within 5 min
docker compose stop spark-stream        # -> StreamStalled within ~2 min
rm data/landing/labs/labs_<next>.csv.done   # -> sensor timeout -> BatchLate
#    verify state=firing in Prometheus /alerts and in Alertmanager, and:
#    select * from ops.alerts_received order by received_at desc;
#    then restart everything and confirm the alerts RESOLVE

# 5. Trace one record end to end
TID=$(curl -s "localhost:8000/patients/P003/vitals?window=5m" | jq -r '.items[0].trace_id')
grep -h "$TID" data/logs/*.jsonl        # producer -> stream -> api lines, in order

# 6. Tests and lint
pytest -q && ruff check . && black --check .

# 7. FULL COLD-START REPRODUCIBILITY (the 5-mark check)
git clone <repo> fresh && cd fresh && cp .env.example .env && docker compose up -d
#    wait 15 real minutes (= 3 sim days) with NO manual intervention, then confirm:
#    3 lab files processed, 3 green DAG runs, 3 report files, dashboards populated,
#    /ward/status non-empty. Time it and put the number in the README.

# 8. Resource sanity
docker stats --no-stream        # total < ~7 GB RAM; note it in limitations

# 9. Deliverables checklist
#    [ ] repo pushed: README, .env.example, docker-compose.yml, tests
#    [ ] report PDF 8-15 pages, all 9 sections, screenshots embedded
#    [ ] demo video 5-10 min OR a rehearsed live-demo runbook
#    [ ] assumptions + simulated clock stated in BOTH README and report
#    [ ] individual contributions statement (if group)
```

---

## 6. Rubric traceability — check before submitting

| Criterion | Marks | Where it is earned |
|---|---|---|
| Architecture decision & justification | 20 | §0.1 table, report §2, the live recompute demo (Phase 4 verify #7), honest trade-off + mitigation via `common/clinical.py` |
| Tech stack selection & justification | 10 | §0.2 table, report §3 — every row tied to a use-case constraint with the rejected option named |
| Data ingestion implementation | 15 | Phase 2: two sources, keyed partitioning, idempotent producer, dirty/late/malformed data, DLQ, atomic file writes, restart resilience |
| Processing layer implementation | 15 | Phase 3 (watermark, dedup, stream–static join, sliding windows, sustained-alert state) + Phase 4 (recompute from master, DQ rules, the vitals↔labs join) |
| Storage & serving layer | 10 | Parquet master dataset + Postgres `speed`/`batch`/`serving` schemas + `serving.patient_360` MV + FastAPI + Grafana |
| Observability | 10 | §5.2 three-signal table, trace_id propagation, 8 alert rules deliberately triggered in Phase 5 verify #4 |
| Report | 15 | §5.3 section map, two diagrams, measured latency and detection precision/recall, real limitations |
| Code quality & documentation | 5 | Single `settings.py`, shared `common/`, pytest, ruff/black, README, `.env.example`, cold-start reproducibility test |

## 7. Suggested schedule (2 weeks)

| Day | Work |
|---|---|
| 1 | Prereqs, Docker Desktop/WSL2, repo skeleton, Phase 1 |
| 2 | Phase 1 verification; start report §1–§3 while the reasoning is fresh |
| 3–4 | Phase 2 + verification |
| 5–6 | Phase 3 + verification (hardest phase — budget the extra day) |
| 7–8 | Phase 4 + verification |
| 9–10 | Phase 5 API, observability, dashboards |
| 11 | Full cold-start test, tuning, collect screenshots + measurements |
| 12–13 | Write the report (diagrams first), record the demo video |
| 14 | Buffer, final checklist, submit |

## 8. Windows-specific pitfalls — hit these on day 1, not day 13

1. **CRLF** in `.sh`/entrypoint files → `exec format error`. Fix with `.gitattributes` (§1.4).
2. **Bind-mount performance** from `/mnt/d` under WSL is slow. If Parquet writes crawl, move the repo into the WSL filesystem (`\\wsl$\Ubuntu\home\<you>\project`) or use a named Docker volume for `data/`.
3. **Docker Desktop default memory too low** → containers OOM-killed with exit code 137. Raise to 8 GB first.
4. **Port conflicts**: Kafka UI on 8080 collides with Airflow's default — put Airflow on 8081. Check with `netstat -ano | findstr :8080`.
5. **`AIRFLOW_UID=50000`** in `.env` on Windows.
6. **Docker socket path** if you use `DockerOperator`: `//var/run/docker.sock` (double slash).
7. **Path with spaces** (`D:\8th sem\...`) — quote every compose volume path, or move the project to a space-free path.
