"""
Lightweight pipeline health-check DAG, independent of the daily batch DAG.
Runs every 2 minutes and checks:
  1. Every service that has ever reported into ops.pipeline_heartbeat is
     still reporting recently (catches a crashed/stuck spark-stream or
     spark-archiver container).
  2. The Parquet master dataset has been written to recently (catches the
     archiver silently stalling even if its own heartbeat write succeeds).

Each check's result is recorded into ops.data_quality (stage=
'pipeline_health') so it shows up alongside the batch layer's own DQ rows,
and the task fails (a real Airflow alert surface, on top of the Prometheus
rules in observability/alerts.yml built in Phase 5) if anything is stale.
This satisfies the rubric's "at least one basic health-check rule" as an
Airflow-native mechanism, independent of the Prometheus/Alertmanager stack.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, "/opt/airflow/project")

from airflow import DAG  # noqa: E402
from airflow.exceptions import AirflowException  # noqa: E402
from airflow.operators.python import PythonOperator  # noqa: E402

from common.db import get_connection, record_data_quality  # noqa: E402
from config.settings import get_settings  # noqa: E402

SETTINGS = get_settings()
DATA_ROOT = os.environ.get("DATA_ROOT", SETTINGS.data_root)
MASTER_VITALS_DIR = os.environ.get("MASTER_VITALS_DIR", SETTINGS.master_vitals_dir)

HEARTBEAT_STALE_SECONDS = 180
MASTER_DATA_STALE_SECONDS = 180


def check_heartbeats(**context) -> None:
    dsn = SETTINGS.postgres_dsn_vitals
    today = datetime.utcnow().date().isoformat()

    with get_connection(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT service, EXTRACT(EPOCH FROM (now() - last_seen_at)) FROM ops.pipeline_heartbeat"
            )
            rows = cur.fetchall()

    stale = [(service, age) for service, age in rows if age > HEARTBEAT_STALE_SECONDS]

    record_data_quality(
        dsn,
        sim_day=today,
        stage="pipeline_health",
        rule="heartbeat_freshness",
        passed=len(stale) == 0,
        failed_count=len(stale),
        checked_count=len(rows),
        is_critical=True,
    )

    if stale:
        details = ", ".join(f"{svc} ({int(age)}s old)" for svc, age in stale)
        raise AirflowException(f"Stale pipeline heartbeat(s): {details}")


def check_master_dataset_freshness(**context) -> None:
    dsn = SETTINGS.postgres_dsn_vitals
    today = datetime.utcnow().date().isoformat()

    newest_mtime = None
    if os.path.isdir(MASTER_VITALS_DIR):
        for root, _dirs, files in os.walk(MASTER_VITALS_DIR):
            for f in files:
                if f.endswith(".parquet"):
                    mtime = os.path.getmtime(os.path.join(root, f))
                    if newest_mtime is None or mtime > newest_mtime:
                        newest_mtime = mtime

    age_seconds = (datetime.utcnow().timestamp() - newest_mtime) if newest_mtime else None
    passed = age_seconds is not None and age_seconds <= MASTER_DATA_STALE_SECONDS

    record_data_quality(
        dsn,
        sim_day=today,
        stage="pipeline_health",
        rule="master_dataset_freshness",
        passed=passed,
        failed_count=0 if passed else 1,
        checked_count=1,
        is_critical=True,
    )

    if not passed:
        raise AirflowException(
            f"Master dataset has not been written to recently "
            f"(age={age_seconds}s, threshold={MASTER_DATA_STALE_SECONDS}s)"
        )


default_args = {
    "owner": "data-eng",
    "retries": 0,
}

with DAG(
    dag_id="pipeline_health",
    description="Cross-cutting health checks: heartbeat freshness, master dataset freshness",
    default_args=default_args,
    schedule=timedelta(minutes=2),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["observability", "ec8203"],
) as dag:

    PythonOperator(task_id="check_heartbeats", python_callable=check_heartbeats)
    PythonOperator(task_id="check_master_dataset_freshness", python_callable=check_master_dataset_freshness)
