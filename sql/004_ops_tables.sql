-- Observability tables. Populated by every stage of the pipeline so pipeline
-- health can be queried directly and scraped for Prometheus/Grafana, and so
-- Airflow's data-quality gate has something concrete to check.

CREATE TABLE IF NOT EXISTS ops.pipeline_heartbeat (
    service           TEXT PRIMARY KEY,      -- e.g. 'vitals-producer', 'spark-stream'
    stage             TEXT NOT NULL,         -- 'ingestion' | 'processing' | 'storage' | 'serving'
    last_seen_at      TIMESTAMPTZ NOT NULL,
    records_processed BIGINT NOT NULL DEFAULT 0,
    errors            BIGINT NOT NULL DEFAULT 0,
    detail            JSONB
);

CREATE TABLE IF NOT EXISTS ops.data_quality (
    id            BIGSERIAL PRIMARY KEY,
    sim_day       DATE NOT NULL,
    stage         TEXT NOT NULL,             -- 'lab_ingest' | 'batch_join' | 'stream_clean' ...
    rule          TEXT NOT NULL,             -- e.g. 'no_null_patient_id'
    passed        BOOLEAN NOT NULL,
    failed_count  BIGINT NOT NULL DEFAULT 0,
    checked_count BIGINT NOT NULL DEFAULT 0,
    is_critical   BOOLEAN NOT NULL DEFAULT FALSE,
    checked_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_data_quality_sim_day ON ops.data_quality (sim_day);

CREATE TABLE IF NOT EXISTS ops.batch_runs (
    id            BIGSERIAL PRIMARY KEY,
    sim_day       DATE NOT NULL,
    dag_run_id    TEXT,
    started_at    TIMESTAMPTZ NOT NULL,
    finished_at   TIMESTAMPTZ,
    status        TEXT NOT NULL DEFAULT 'running',   -- running | success | failed
    rows_written  BIGINT,
    error_message TEXT
);
CREATE INDEX IF NOT EXISTS idx_batch_runs_sim_day ON ops.batch_runs (sim_day);

CREATE TABLE IF NOT EXISTS ops.alerts_received (
    id            BIGSERIAL PRIMARY KEY,
    source        TEXT NOT NULL,             -- 'alertmanager' | 'internal'
    alert_name    TEXT NOT NULL,
    severity      TEXT,
    status        TEXT NOT NULL DEFAULT 'firing',    -- firing | resolved
    labels        JSONB,
    annotations   JSONB,
    received_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_alerts_received_received_at ON ops.alerts_received (received_at DESC);
