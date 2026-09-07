-- Speed-layer tables, written by processing/stream_job.py (Phase 3).
-- All writes are idempotent upserts (ON CONFLICT ...) keyed so that
-- reprocessing a micro-batch after a restart never creates duplicates --
-- see PLAN.md Phase 3 verification #7.

CREATE TABLE IF NOT EXISTS speed.vitals_clean (
    event_id      TEXT PRIMARY KEY,
    patient_id    TEXT NOT NULL,
    heart_rate    DOUBLE PRECISION,
    spo2          DOUBLE PRECISION,
    systolic_bp   DOUBLE PRECISION,
    diastolic_bp  DOUBLE PRECISION,
    temperature   DOUBLE PRECISION,
    event_ts      TIMESTAMPTZ NOT NULL,
    producer_ts   TIMESTAMPTZ,
    sim_day       TEXT NOT NULL,
    trace_id      TEXT,
    inserted_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_vitals_clean_patient_ts ON speed.vitals_clean (patient_id, event_ts DESC);

CREATE TABLE IF NOT EXISTS speed.patient_vitals_1m (
    patient_id     TEXT NOT NULL,
    window_start   TIMESTAMPTZ NOT NULL,
    window_end     TIMESTAMPTZ NOT NULL,
    avg_hr         DOUBLE PRECISION,
    min_hr         DOUBLE PRECISION,
    max_hr         DOUBLE PRECISION,
    stddev_hr      DOUBLE PRECISION,
    avg_spo2       DOUBLE PRECISION,
    min_spo2       DOUBLE PRECISION,
    avg_sbp        DOUBLE PRECISION,
    min_sbp        DOUBLE PRECISION,
    max_sbp        DOUBLE PRECISION,
    avg_dbp        DOUBLE PRECISION,
    avg_temp       DOUBLE PRECISION,
    min_temp       DOUBLE PRECISION,
    max_temp       DOUBLE PRECISION,
    reading_count  INTEGER,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (patient_id, window_start)
);
CREATE INDEX IF NOT EXISTS idx_patient_vitals_1m_patient_ws ON speed.patient_vitals_1m (patient_id, window_start DESC);

CREATE TABLE IF NOT EXISTS speed.patient_risk_now (
    patient_id     TEXT NOT NULL,
    window_start   TIMESTAMPTZ NOT NULL,
    score          INTEGER NOT NULL,
    risk_band      TEXT NOT NULL,     -- 'low' | 'medium' | 'high'
    trend          TEXT NOT NULL,     -- 'improving' | 'stable' | 'deteriorating'
    components     JSONB,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (patient_id, window_start)
);
CREATE INDEX IF NOT EXISTS idx_patient_risk_now_patient_ws ON speed.patient_risk_now (patient_id, window_start DESC);

CREATE TABLE IF NOT EXISTS speed.clinical_alerts (
    id             BIGSERIAL PRIMARY KEY,
    patient_id     TEXT NOT NULL,
    window_start   TIMESTAMPTZ NOT NULL,
    score          INTEGER NOT NULL,
    risk_band      TEXT NOT NULL,
    components     JSONB,
    raised_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    acknowledged   BOOLEAN NOT NULL DEFAULT FALSE,
    acknowledged_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_clinical_alerts_raised_at ON speed.clinical_alerts (raised_at DESC);
CREATE INDEX IF NOT EXISTS idx_clinical_alerts_patient ON speed.clinical_alerts (patient_id, raised_at DESC);
