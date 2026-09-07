-- Lambda-architecture schema layout inside the `vitals` database:
--   speed    -> speed-layer (near-real-time) outputs, written by stream_job.py
--   batch    -> batch-layer (authoritative, daily-recomputed) outputs,
--               written by batch_job.py
--   serving  -> merged/serving views consumed by the API and Grafana
--               (serving.patient_360 materialized view is the Lambda
--               "serving layer" proper)
--   ops      -> pipeline observability: heartbeats, data-quality results,
--               batch run history, received alerts
CREATE SCHEMA IF NOT EXISTS speed;
CREATE SCHEMA IF NOT EXISTS batch;
CREATE SCHEMA IF NOT EXISTS serving;
CREATE SCHEMA IF NOT EXISTS ops;
-- Small reference/dimension data (the patient roster from config/patients.csv,
-- kept in sync by processing/batch_job.py on every run) so the serving layer
-- can join names/ward/bed without reading a CSV file at query time.
CREATE SCHEMA IF NOT EXISTS reference;
