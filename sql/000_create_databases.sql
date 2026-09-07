-- Runs once, against the default database (POSTGRES_DB=vitals), on first
-- container boot only (docker-entrypoint-initdb.d semantics). Creates the
-- second database used as the Airflow metadata store, so we only need one
-- Postgres container instead of two.
SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')
\gexec
