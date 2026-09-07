-- Batch-layer tables, written by processing/batch_job.py (Phase 4). Every
-- write for a given sim_day is a delete-then-insert inside one transaction
-- (see batch_job.py's write_results()), so re-running/backfilling a day is
-- idempotent -- the key property the Lambda batch layer depends on.

CREATE TABLE IF NOT EXISTS reference.patients (
    patient_id     TEXT PRIMARY KEY,
    name           TEXT,
    age            INTEGER,
    sex            TEXT,
    ward           TEXT,
    bed            TEXT,
    admitted_at    TEXT,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS batch.patient_daily_vitals (
    patient_id          TEXT NOT NULL,
    sim_day             DATE NOT NULL,
    avg_hr              DOUBLE PRECISION,
    min_hr              DOUBLE PRECISION,
    max_hr              DOUBLE PRECISION,
    stddev_hr           DOUBLE PRECISION,
    avg_spo2            DOUBLE PRECISION,
    min_spo2            DOUBLE PRECISION,
    avg_sbp             DOUBLE PRECISION,
    min_sbp             DOUBLE PRECISION,
    max_sbp             DOUBLE PRECISION,
    avg_dbp             DOUBLE PRECISION,
    avg_temp            DOUBLE PRECISION,
    min_temp            DOUBLE PRECISION,
    max_temp            DOUBLE PRECISION,
    reading_count       INTEGER,
    worst_news2_score   INTEGER,
    worst_news2_band    TEXT,
    vitals_reason_codes JSONB,
    computed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (patient_id, sim_day)
);
CREATE INDEX IF NOT EXISTS idx_patient_daily_vitals_sim_day ON batch.patient_daily_vitals (sim_day);

CREATE TABLE IF NOT EXISTS batch.patient_lab_results (
    id              BIGSERIAL PRIMARY KEY,
    patient_id      TEXT NOT NULL,
    sim_day         DATE NOT NULL,
    test_type       TEXT NOT NULL,
    result_value    DOUBLE PRECISION,
    unit            TEXT,
    reference_low   DOUBLE PRECISION,
    reference_high  DOUBLE PRECISION,
    flag            TEXT,          -- 'low' | 'normal' | 'high' | 'unknown'
    collected_at    TIMESTAMPTZ,
    lab_batch_id    TEXT,
    UNIQUE (patient_id, sim_day, test_type)
);
CREATE INDEX IF NOT EXISTS idx_patient_lab_results_sim_day ON batch.patient_lab_results (sim_day);

CREATE TABLE IF NOT EXISTS batch.patient_risk_daily (
    patient_id           TEXT NOT NULL,
    sim_day              DATE NOT NULL,
    vitals_score         INTEGER,
    vitals_band          TEXT,
    abnormal_test_count  INTEGER,
    adjusted_risk_score  INTEGER,
    risk_band            TEXT,
    reason_codes         JSONB,
    previous_day_score   INTEGER,
    score_delta          INTEGER,
    delta_direction      TEXT,     -- 'improving' | 'stable' | 'deteriorating' | 'new'
    computed_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (patient_id, sim_day)
);
CREATE INDEX IF NOT EXISTS idx_patient_risk_daily_sim_day ON batch.patient_risk_daily (sim_day);
CREATE INDEX IF NOT EXISTS idx_patient_risk_daily_band ON batch.patient_risk_daily (risk_band);

-- ---------------------------------------------------------------------------
-- Serving layer: THE materialized view that merges "right now" (speed.*)
-- with "as of yesterday" (batch.*) per patient. This IS the Lambda serving
-- layer described in PLAN.md/the report -- refreshed by the Airflow DAG's
-- refresh_serving_view task after every successful batch run.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS serving.patient_360 AS
SELECT
    p.patient_id,
    p.name,
    p.ward,
    p.bed,
    rn.window_start   AS current_window_start,
    rn.score          AS current_score,
    rn.risk_band      AS current_risk_band,
    rn.trend          AS current_trend,
    rd.sim_day        AS batch_sim_day,
    rd.vitals_band    AS batch_vitals_band,
    rd.adjusted_risk_score AS batch_adjusted_score,
    rd.risk_band      AS batch_risk_band,
    rd.reason_codes   AS batch_reason_codes,
    rd.score_delta    AS batch_score_delta,
    rd.delta_direction AS batch_delta_direction
FROM reference.patients p
LEFT JOIN LATERAL (
    SELECT * FROM speed.patient_risk_now
    WHERE patient_id = p.patient_id
    ORDER BY window_start DESC LIMIT 1
) rn ON true
LEFT JOIN LATERAL (
    SELECT * FROM batch.patient_risk_daily
    WHERE patient_id = p.patient_id
    ORDER BY sim_day DESC LIMIT 1
) rd ON true;

CREATE UNIQUE INDEX IF NOT EXISTS idx_patient_360_patient_id ON serving.patient_360 (patient_id);
