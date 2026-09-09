# Report Outline (target 8-15 pages)

Structure only, mapped directly to the marking rubric. Write the actual analysis and
prose yourself -- the assignment's viva requirement means you need to be able to defend
every decision here from first principles, not just paste this in. Each section below
points at the exact place in the codebase/PLAN.md/README.md that backs it up, and at
what to run to get the real numbers/screenshots.

## 1. Use case & business requirements (~1 page)
- Restate Use Case 2 in your own words: two questions, two SLAs (seconds vs. daily).
- Source: `EC8203 MiniProject 2026.pdf` page 3, `README.md` intro.

## 2. Architecture decision: Lambda vs Kappa (~3 pages, 20 of 100 marks)
- Walk through the table in `PLAN.md` section 0.1 as prose, in your own words -- don't
  paste the table itself, argue it.
- State the rejected alternative (Kappa) and why, explicitly.
- The honest trade-off: logic duplication between speed/batch layers, and your
  mitigation (`common/clinical.py` -- one implementation, two execution contexts:
  `processing/stream_job.py` and `processing/batch_job.py` both import it).
- Live proof to describe: the backfill/recompute demo (change a threshold in
  `config/thresholds.yml`, run `airflow dags backfill -s <d1> -e <d3> daily_patient_risk`,
  show batch views change while the speed layer is untouched).

## 3. Technology stack, justified per layer (~1.5 pages, 10 marks)
- Walk through `PLAN.md` section 0.2's table, one paragraph per row, each tied to a
  concrete constraint from the use case (not generic popularity).
- Explicitly name what you rejected for storage (Cassandra) and why.

## 4. Architecture diagrams + data walk-through (~2 pages)
- Render `docs/architecture.md`'s two Mermaid diagrams (GitHub preview / VS Code Mermaid
  extension / mermaid.live) and paste the images.
- Walk through ONE vitals reading end-to-end: producer -> Kafka (`vitals.raw`) ->
  `stream_job.py` (clean/dedup/window/score) -> `speed.*` tables -> also archived via
  `raw_archiver.py` into the Parquet master dataset -> later read by `batch_job.py`.

## 5. Implementation details (~2 pages)
- Schemas: `common/schemas.py` (wire format), `sql/*.sql` (storage schema).
- Key transformations: watermark + `dropDuplicates` (why the watermark must come first
  -- unbounded state otherwise), the worst-case-in-window NEWS2 scoring
  (`common.clinical.news2_score_from_window`), the sustained-alert rule (>=2 consecutive
  high-band windows -- `processing/stream_job.py`'s `AlertState`), the batch layer's
  5 lab data-quality rules with row-level quarantine (not whole-file rejection).
- Idempotency: the Postgres upserts in the speed layer, the delete-then-insert-per-
  sim_day transaction in `batch_job.py`.

## 6. Observability design (~1.5 pages, 10 marks)
- Reproduce the "three signals across N stages" table from `README.md` /
  `observability/alerts.yml`'s comments: what's logged, what's measured, what alerts on
  each stage.
- Explain the trace_id propagation: producer -> Kafka header -> `speed.vitals_clean` ->
  API response. Demo command:
  ```
  TID=$(curl -s "localhost:8000/patients/P001/vitals?window=5m" | jq -r '.items[0].trace_id')
  grep -h "$TID" data/logs/*.jsonl
  ```

## 7. Results (~2 pages) -- fill in AFTER running the full stack
- [ ] Screenshot: Ward Clinical Dashboard (Grafana, `ward-clinical`)
- [ ] Screenshot: Pipeline Health Dashboard (Grafana, `pipeline-health`)
- [ ] Screenshot: API docs at `/docs` + one real response from `/patients/{id}/risk`
      showing BOTH the realtime and batch-lab-adjusted blocks for a patient whose risk
      band changed because of labs
- [ ] Screenshot: Airflow graph view, `daily_patient_risk` DAG, all-green run
- [ ] Screenshot or excerpt: the generated `data/reports/daily_risk_report_<day>.html`
- [ ] Numbers: run `python scripts/verify_alerts.py` -- record precision/recall
- [ ] Numbers: run `python scripts/measure_latency.py` -- record the p95 figure

## 8. Limitations & production-scale changes (~1.5 pages) -- be honest
Things already deliberately out of scope, worth naming explicitly:
- Single-broker Kafka (RF=1) -- no fault tolerance if the broker dies.
- Parquet on a local bind mount stands in for HDFS/S3.
- No PHI security/encryption/audit trail -- real clinical data would need this from day one.
- No schema registry (would add Avro + Confluent Schema Registry for real).
- No Kafka consumer-group lag exporter or per-micro-batch duration histogram --
  `observability/alerts.yml`'s header comment explains why those two specific rules
  aren't implemented (would need a lag-exporter sidecar / custom Spark instrumentation).
- Sustained-alert streak state is in-memory in `stream_job.py`, not checkpointed --
  resets on a stream restart (documented in that file's module docstring).
- Airflow `LocalExecutor` -- a production deployment would use Celery/Kubernetes.
- `event_ts` (real time) vs. `sim_day` (compressed calendar) dual-clock design --
  explain why (see `PLAN.md` section 0.3 / `ingestion/sim_clock.py`'s docstring), since
  it's a non-obvious decision a grader will likely ask about in the viva.

## 9. Assumptions, simulated clock, contributions
- State the simulated-clock compression explicitly (`README.md` section 3).
- If a group submission: individual contributions statement.
