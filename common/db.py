"""
Thin PostgreSQL helpers shared by every Spark job and the Airflow DAGs.

Deliberately NOT an ORM -- every write in this pipeline is either a simple
upsert (heartbeat) or a bulk COPY-like insert done inside a Spark
`foreachBatch`, where a lightweight `psycopg2` connection per micro-batch is
both simpler and faster than standing up SQLAlchemy inside executors.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from typing import Iterator, Optional

import psycopg2
import psycopg2.extras


@contextmanager
def get_connection(dsn: str) -> Iterator["psycopg2.extensions.connection"]:
    conn = psycopg2.connect(dsn)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def upsert_pipeline_heartbeat(
    dsn: str,
    service: str,
    stage: str,
    records_processed: int = 0,
    errors: int = 0,
    detail: Optional[dict] = None,
) -> None:
    """
    Upsert one row per `service` into ops.pipeline_heartbeat. Called after
    every micro-batch by the archiver and the speed-layer stream job, and
    periodically by producers/the API -- this single table is what every
    "is stage X alive" alert rule in observability/alerts.yml queries.
    """
    with get_connection(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.pipeline_heartbeat
                    (service, stage, last_seen_at, records_processed, errors, detail)
                VALUES (%s, %s, now(), %s, %s, %s)
                ON CONFLICT (service) DO UPDATE SET
                    stage = EXCLUDED.stage,
                    last_seen_at = EXCLUDED.last_seen_at,
                    records_processed = ops.pipeline_heartbeat.records_processed + EXCLUDED.records_processed,
                    errors = ops.pipeline_heartbeat.errors + EXCLUDED.errors,
                    detail = EXCLUDED.detail
                """,
                (service, stage, records_processed, errors, json.dumps(detail or {})),
            )


def upsert_patients(dsn: str, patients: list[dict]) -> None:
    """Keep reference.patients in sync with config/patients.csv. Called once
    per batch_job.py run (cheap -- a dozen rows) so the serving layer can
    join names/ward/bed without reading a CSV at query time."""
    with get_connection(dsn) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO reference.patients (patient_id, name, age, sex, ward, bed, admitted_at, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s, now())
                ON CONFLICT (patient_id) DO UPDATE SET
                    name = EXCLUDED.name, age = EXCLUDED.age, sex = EXCLUDED.sex,
                    ward = EXCLUDED.ward, bed = EXCLUDED.bed, admitted_at = EXCLUDED.admitted_at,
                    updated_at = now()
                """,
                [
                    (
                        p["patient_id"],
                        p.get("name"),
                        p.get("age"),
                        p.get("sex"),
                        p.get("ward"),
                        p.get("bed"),
                        p.get("admitted_at"),
                    )
                    for p in patients
                ],
            )


def record_data_quality(
    dsn: str,
    sim_day: str,
    stage: str,
    rule: str,
    passed: bool,
    failed_count: int = 0,
    checked_count: int = 0,
    is_critical: bool = False,
) -> None:
    """Insert one data-quality check result (Phase 4's batch job uses this)."""
    with get_connection(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.data_quality
                    (sim_day, stage, rule, passed, failed_count, checked_count, is_critical, checked_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                """,
                (sim_day, stage, rule, passed, failed_count, checked_count, is_critical),
            )
