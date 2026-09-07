"""
The Lambda BATCH LAYER orchestrator: waits for the day's lab file, validates
it, confirms the master dataset has that day's vitals, runs the batch
recompute, gates on data quality, builds the consolidated report, and
refreshes the serving materialized view.

DEVIATION FROM A LITERAL catchup=True (documented here and in PLAN.md):
the DAG uses catchup=False for live scheduling. With a 5-minute schedule and
a start_date fixed at the project's SIM_START_DATE, catchup=True would make
Airflow immediately schedule every missed interval since that date the first
time the DAG is unpaused -- potentially thousands of runs. Batch RECOMPUTE
(the property PLAN.md Phase 4 verification #7 actually wants to demonstrate)
is instead triggered on demand with the explicit CLI command, which works
regardless of the DAG's catchup setting and is the intended way to backfill:

    airflow dags backfill -s 2026-01-01 -e 2026-01-03 daily_patient_risk

Sim-day resolution: resolve_sim_day reads the SAME shared sim_clock.json
state file the ingestion producers use (ingestion/sim_clock.py), so "the day
this run processes" always means "the simulated day that just completed",
consistent across every component of the pipeline.

No Airflow Connections are configured anywhere in this DAG -- every database
access goes through common.db / config.settings, the same single source of
truth every other service in this project uses. This means the pipeline
needs zero manual Airflow UI setup after `docker compose up`.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, "/opt/airflow/project")

from airflow import DAG  # noqa: E402
from airflow.exceptions import AirflowException  # noqa: E402
from airflow.operators.bash import BashOperator  # noqa: E402
from airflow.operators.python import PythonOperator  # noqa: E402
from airflow.sensors.filesystem import FileSensor  # noqa: E402

from common.db import get_connection, upsert_pipeline_heartbeat  # noqa: E402
from config.settings import get_settings  # noqa: E402
from ingestion.sim_clock import SimClock  # noqa: E402

SETTINGS = get_settings()
DATA_ROOT = os.environ.get("DATA_ROOT", SETTINGS.data_root)
LANDING_LABS_DIR = os.environ.get("LANDING_LABS_DIR", SETTINGS.landing_labs_dir)
MASTER_VITALS_DIR = os.environ.get("MASTER_VITALS_DIR", SETTINGS.master_vitals_dir)
SIM_CLOCK_PATH = os.path.join(DATA_ROOT, "sim_clock.json")

MIN_EXPECTED_MASTER_ROWS = 1  # a real deployment would tune this to expected daily volume


def _sim_clock() -> SimClock:
    return SimClock.load_or_create(SETTINGS.sim_start_date, SETTINGS.sim_day_seconds, SIM_CLOCK_PATH)


def resolve_sim_day(**context) -> str:
    """The simulated day this run processes: the one that JUST completed,
    per the Lambda batch layer's "recompute yesterday, fully" convention."""
    sim_day = _sim_clock().previous_sim_day()
    context["ti"].xcom_push(key="sim_day", value=sim_day)
    return sim_day


def validate_lab_file(**context) -> None:
    import json

    sim_day = context["ti"].xcom_pull(task_ids="resolve_sim_day", key="sim_day")
    csv_path = os.path.join(LANDING_LABS_DIR, f"labs_{sim_day}.csv")
    manifest_path = os.path.join(LANDING_LABS_DIR, f"labs_{sim_day}.manifest.json")
    done_path = csv_path + ".done"

    if not (os.path.exists(csv_path) and os.path.exists(manifest_path) and os.path.exists(done_path)):
        raise AirflowException(f"Lab file artifacts incomplete for sim_day={sim_day}")

    with open(manifest_path, encoding="utf-8") as fh:
        manifest = json.load(fh)
    with open(done_path, encoding="utf-8") as fh:
        done = json.load(fh)

    if manifest["checksum_md5"] != done["checksum_md5"]:
        raise AirflowException(
            f"Lab file checksum mismatch for sim_day={sim_day} -- possible truncated write"
        )

    with open(csv_path, encoding="utf-8") as fh:
        actual_rows = sum(1 for _ in fh) - 1  # minus header
    if actual_rows != manifest["row_count"]:
        raise AirflowException(
            f"Lab file row count mismatch for sim_day={sim_day}: "
            f"manifest says {manifest['row_count']}, file has {actual_rows}"
        )


def ensure_master_data(**context) -> None:
    sim_day = context["ti"].xcom_pull(task_ids="resolve_sim_day", key="sim_day")
    partition_glob_root = os.path.join(MASTER_VITALS_DIR, f"dt={sim_day}")
    if not os.path.isdir(partition_glob_root):
        raise AirflowException(
            f"No master-dataset partition found for sim_day={sim_day} at {partition_glob_root} "
            "-- has spark-archiver processed this day yet?"
        )
    row_count = 0
    for root, _dirs, files in os.walk(partition_glob_root):
        row_count += sum(1 for f in files if f.endswith(".parquet"))
    if row_count < MIN_EXPECTED_MASTER_ROWS:
        raise AirflowException(
            f"Master-dataset partition for sim_day={sim_day} looks too small ({row_count} files)"
        )


def data_quality_gate(**context) -> None:
    sim_day = context["ti"].xcom_pull(task_ids="resolve_sim_day", key="sim_day")
    with get_connection(SETTINGS.postgres_dsn_vitals) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT rule, failed_count FROM ops.data_quality WHERE sim_day = %s AND is_critical AND NOT passed",
                (sim_day,),
            )
            failures = cur.fetchall()
    if failures:
        details = ", ".join(f"{rule} ({count} rows)" for rule, count in failures)
        raise AirflowException(f"Critical data-quality rule(s) failed for sim_day={sim_day}: {details}")


def build_report_task(**context) -> None:
    sys.path.insert(0, "/opt/airflow/project")
    from common.logging_setup import configure_logging
    from serving.report_builder import build_report

    sim_day = context["ti"].xcom_pull(task_ids="resolve_sim_day", key="sim_day")
    logger = configure_logging("airflow-report-builder", logs_dir=os.environ.get("LOGS_DIR"))
    reports_dir = os.environ.get("REPORTS_DIR", SETTINGS.reports_dir)
    build_report(sim_day, SETTINGS.postgres_dsn_vitals, reports_dir, logger)


def refresh_serving_view(**context) -> None:
    with get_connection(SETTINGS.postgres_dsn_vitals) as conn:
        with conn.cursor() as cur:
            cur.execute("REFRESH MATERIALIZED VIEW serving.patient_360")


def publish_run_heartbeat(**context) -> None:
    sim_day = context["ti"].xcom_pull(task_ids="resolve_sim_day", key="sim_day")
    upsert_pipeline_heartbeat(
        SETTINGS.postgres_dsn_vitals,
        service="airflow-daily-patient-risk-dag",
        stage="orchestration",
        records_processed=1,
        detail={"sim_day": sim_day},
    )


default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="daily_patient_risk",
    description="Lambda batch layer: recompute one simulated day's patient risk picture",
    default_args=default_args,
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,  # see module docstring -- use `airflow dags backfill` for recompute demos
    max_active_runs=1,
    tags=["lambda-batch-layer", "ec8203"],
) as dag:

    resolve_sim_day_task = PythonOperator(
        task_id="resolve_sim_day",
        python_callable=resolve_sim_day,
    )

    wait_for_lab_file = FileSensor(
        task_id="wait_for_lab_file",
        filepath=(
            LANDING_LABS_DIR + "/labs_{{ ti.xcom_pull(task_ids='resolve_sim_day', key='sim_day') }}.csv.done"
        ),
        poke_interval=10,
        timeout=180,
        mode="reschedule",
    )

    validate_lab_file_task = PythonOperator(
        task_id="validate_lab_file",
        python_callable=validate_lab_file,
    )

    ensure_master_data_task = PythonOperator(
        task_id="ensure_master_data",
        python_callable=ensure_master_data,
    )

    run_batch_job = BashOperator(
        task_id="run_batch_job",
        bash_command=(
            "spark-submit /opt/airflow/project/processing/batch_job.py "
            "--sim-day {{ ti.xcom_pull(task_ids='resolve_sim_day', key='sim_day') }}"
        ),
    )

    data_quality_gate_task = PythonOperator(
        task_id="data_quality_gate",
        python_callable=data_quality_gate,
    )

    build_report = PythonOperator(
        task_id="build_report",
        python_callable=build_report_task,
    )

    refresh_serving_view_task = PythonOperator(
        task_id="refresh_serving_view",
        python_callable=refresh_serving_view,
    )

    publish_metrics = PythonOperator(
        task_id="publish_metrics",
        python_callable=publish_run_heartbeat,
    )

    (
        resolve_sim_day_task
        >> wait_for_lab_file
        >> validate_lab_file_task
        >> ensure_master_data_task
        >> run_batch_job
        >> data_quality_gate_task
        >> build_report
        >> refresh_serving_view_task
        >> publish_metrics
    )
