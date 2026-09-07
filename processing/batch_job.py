"""
Spark batch job: the LAMBDA BATCH LAYER.

Invoked by Airflow (orchestration/dags/daily_patient_risk_dag.py) once per
simulated day, as `spark-submit batch_job.py --sim-day 2026-01-05`. This is
the authoritative recompute: it reads straight from the immutable Parquet
master dataset (NOT from anything the speed layer produced), joins that
day's vitals against that day's lab file, and writes the result with a
delete-then-insert scoped to that single sim_day inside one transaction --
which is what makes re-running or backfilling a day idempotent (PLAN.md
Phase 4 verification #6/#7).

Pipeline:
  1. Read config/patients.csv (the patient roster) and sync it into
     reference.patients (cheap -- a dozen rows).
  2. Read the master dataset filtered to dt=<sim_day>; drop rows that failed
     JSON parsing or fail common.clinical.validate_reading (the SAME
     validation rule the speed layer applies -- one implementation, two
     execution contexts, per PLAN.md section 0.1).
  3. Score EVERY reading with common.clinical.news2_score and keep the
     worst-scoring reading per patient via Spark's max_by() aggregate --
     this is "how bad did today get for this patient", computed once with
     one implementation shared with the speed layer.
  4. Read that day's lab CSV with pandas (the file is a handful of rows --
     Spark's parallelism argument applies to the vitals stream, not this),
     apply row-level data-quality rules (each one recorded into
     ops.data_quality), quarantine rows that fail any rule into
     data/quarantine/, and flag the surviving rows with common.clinical.lab_flag.
  5. For every patient: combine the day's worst vitals score with that day's
     lab flags via common.clinical.adjust_risk_with_labs -- THE JOIN that
     answers the second half of the business question -- plus a
     day-over-day delta against yesterday's adjusted score.
  6. Write batch.patient_daily_vitals / batch.patient_lab_results /
     batch.patient_risk_daily, and record the run in ops.batch_runs.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# NOTE: pyspark is imported LAZILY, inside build_spark()/compute_daily_vitals()
# only -- not at module level. This means every other function in this file
# (vitals_reason_codes_from_components, process_lab_file, flag_lab_results,
# build_risk_daily, write_results) can be unit-tested on a host that has no
# PySpark installed at all (see tests/test_batch_transforms.py), which is
# most of this module's actual decision logic. Only compute_daily_vitals()
# needs a real Spark runtime, and it is exercised end-to-end when the job
# actually runs inside the Airflow container.
from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

from common.clinical import (  # noqa: E402
    adjust_risk_with_labs,
    lab_flag,
    news2_score,
    risk_band_for_score,
    trend_direction,
    validate_reading,
)
from common.db import get_connection, record_data_quality, upsert_patients  # noqa: E402
from common.logging_setup import configure_logging  # noqa: E402
from config.settings import get_settings  # noqa: E402

SERVICE_NAME = "batch-job"

# Vitals-side reason codes: a NEWS2 component contributing >= 2 points on the
# day's worst reading is considered a meaningful reason to flag that vital.
VITALS_REASON_LABELS = {
    "heart_rate": "abnormal_heart_rate",
    "spo2": "low_spo2",
    "systolic_bp": "abnormal_blood_pressure",
    "temperature": "abnormal_temperature",
}
REASON_POINT_THRESHOLD = 2


def build_spark(settings) -> "SparkSession":
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("batch-layer")
        .config("spark.sql.shuffle.partitions", settings.spark_shuffle_partitions)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.postgresql:postgresql:42.7.4",
        )
        # NOTE: different ivy cache path than processing/stream_job.py's
        # /opt/ivy -- this job runs inside the Airflow container, which
        # builds its own jar cache at /opt/airflow/ivy (see
        # docker/airflow.Dockerfile).
        .config("spark.jars.ivy", "/opt/airflow/ivy")
        .getOrCreate()
    )


def load_patients(csv_path: str) -> list[dict]:
    with open(csv_path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def vitals_reason_codes_from_components(components: dict) -> list[str]:
    return sorted(
        VITALS_REASON_LABELS[vital]
        for vital, points in components.items()
        if vital in VITALS_REASON_LABELS and points >= REASON_POINT_THRESHOLD
    )


# --------------------------------------------------------------------------
# Step 2-3: vitals aggregation + worst-of-day scoring (Spark, distributed)
# --------------------------------------------------------------------------


def _news2_struct():
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    return StructType(
        [
            StructField("score", IntegerType()),
            StructField("band", StringType()),
            StructField("components_json", StringType()),
        ]
    )


def _score_row(hr, spo2, sbp, temp):
    score, band, components = news2_score(hr, spo2, sbp, temp)
    return (score, band, json.dumps(components))


def compute_daily_vitals(spark: "SparkSession", master_dir: str, sim_day: str, logger) -> list[dict]:
    from pyspark.sql.functions import avg, col, lit, max_by, stddev, udf
    from pyspark.sql.functions import count as spark_count
    from pyspark.sql.functions import max as spark_max
    from pyspark.sql.functions import min as spark_min

    df = spark.read.parquet(master_dir).filter(col("dt") == sim_day)
    total_rows = df.count()
    if total_rows == 0:
        logger.warning("no_master_data_for_sim_day", stage="processing", sim_day=sim_day)
        return []

    parsed = df.filter(col("event_id").isNotNull())

    def _valid(hr, spo2, sbp, dbp, temp) -> bool:
        ok, _ = validate_reading(hr, spo2, sbp, dbp, temp)
        return ok

    valid_udf = udf(_valid, "boolean")
    valid = parsed.filter(
        valid_udf(col("heart_rate"), col("spo2"), col("systolic_bp"), col("diastolic_bp"), col("temperature"))
    )

    valid_count = valid.count()
    logger.info(
        "vitals_read_for_sim_day",
        stage="processing",
        sim_day=sim_day,
        total_rows=total_rows,
        valid_rows=valid_count,
    )
    if valid_count == 0:
        return []

    score_udf = udf(_score_row, _news2_struct())
    scored = valid.withColumn(
        "news2", score_udf(col("heart_rate"), col("spo2"), col("systolic_bp"), col("temperature"))
    )

    aggregated = scored.groupBy("patient_id").agg(
        avg("heart_rate").alias("avg_hr"),
        spark_min("heart_rate").alias("min_hr"),
        spark_max("heart_rate").alias("max_hr"),
        stddev("heart_rate").alias("stddev_hr"),
        avg("spo2").alias("avg_spo2"),
        spark_min("spo2").alias("min_spo2"),
        avg("systolic_bp").alias("avg_sbp"),
        spark_min("systolic_bp").alias("min_sbp"),
        spark_max("systolic_bp").alias("max_sbp"),
        avg("diastolic_bp").alias("avg_dbp"),
        avg("temperature").alias("avg_temp"),
        spark_min("temperature").alias("min_temp"),
        spark_max("temperature").alias("max_temp"),
        spark_count(lit(1)).alias("reading_count"),
        max_by(col("news2"), col("news2.score")).alias("worst_news2"),
    )

    rows = aggregated.collect()
    results = []
    for r in rows:
        worst = r["worst_news2"]
        components = json.loads(worst["components_json"]) if worst else {}
        results.append(
            {
                "patient_id": r["patient_id"],
                "avg_hr": r["avg_hr"],
                "min_hr": r["min_hr"],
                "max_hr": r["max_hr"],
                "stddev_hr": r["stddev_hr"],
                "avg_spo2": r["avg_spo2"],
                "min_spo2": r["min_spo2"],
                "avg_sbp": r["avg_sbp"],
                "min_sbp": r["min_sbp"],
                "max_sbp": r["max_sbp"],
                "avg_dbp": r["avg_dbp"],
                "avg_temp": r["avg_temp"],
                "min_temp": r["min_temp"],
                "max_temp": r["max_temp"],
                "reading_count": r["reading_count"],
                "worst_score": worst["score"] if worst else None,
                "worst_band": worst["band"] if worst else None,
                "vitals_reason_codes": vitals_reason_codes_from_components(components) if worst else [],
            }
        )
    return results


# --------------------------------------------------------------------------
# Step 4: lab file -- read, validate row-by-row, quarantine, flag
# --------------------------------------------------------------------------


def process_lab_file(
    landing_dir: str,
    sim_day: str,
    known_patient_ids: set[str],
    quarantine_dir: str,
    dsn: str,
    logger,
) -> pd.DataFrame:
    """Returns a cleaned DataFrame of surviving lab rows. Every rule's
    pass/fail counts are recorded into ops.data_quality; rows failing ANY
    rule are written to data/quarantine/ rather than silently dropped."""
    lab_path = os.path.join(landing_dir, f"labs_{sim_day}.csv")

    empty_columns = [
        "patient_id",
        "test_type",
        "result_value",
        "unit",
        "reference_low",
        "reference_high",
        "collected_at",
        "lab_batch_id",
    ]
    if not os.path.exists(lab_path):
        logger.warning("lab_file_missing", stage="processing", sim_day=sim_day, path=lab_path)
        record_data_quality(
            dsn,
            sim_day,
            "lab_ingest",
            "file_not_empty",
            passed=False,
            failed_count=1,
            checked_count=1,
            is_critical=True,
        )
        return pd.DataFrame(columns=empty_columns)

    raw = pd.read_csv(lab_path, dtype=str, keep_default_na=False, na_values=[""])
    total = len(raw)

    record_data_quality(
        dsn,
        sim_day,
        "lab_ingest",
        "file_not_empty",
        passed=total > 0,
        failed_count=0 if total > 0 else 1,
        checked_count=1,
        is_critical=True,
    )
    if total == 0:
        return pd.DataFrame(columns=empty_columns)

    keep_mask = pd.Series(True, index=raw.index)

    # Rule: patient_id must be a known patient.
    valid_patient_mask = raw["patient_id"].isin(known_patient_ids)
    failed = int((~valid_patient_mask).sum())
    record_data_quality(
        dsn,
        sim_day,
        "lab_ingest",
        "valid_patient_id",
        passed=failed == 0,
        failed_count=failed,
        checked_count=total,
        is_critical=failed > total * 0.5,
    )
    keep_mask &= valid_patient_mask

    # Rule: result_value must parse as a number (catches the injected "ERROR" corruption).
    numeric_result = pd.to_numeric(raw["result_value"], errors="coerce")
    numeric_mask = numeric_result.notna()
    failed = int((~numeric_mask).sum())
    record_data_quality(
        dsn,
        sim_day,
        "lab_ingest",
        "numeric_result_value",
        passed=failed == 0,
        failed_count=failed,
        checked_count=total,
        is_critical=failed > total * 0.5,
    )
    keep_mask &= numeric_mask

    # Rule: no duplicate (patient_id, test_type) per day -- keep first occurrence.
    duplicate_mask = raw.duplicated(subset=["patient_id", "test_type"], keep="first")
    failed = int(duplicate_mask.sum())
    record_data_quality(
        dsn,
        sim_day,
        "lab_ingest",
        "no_duplicate_test_per_patient",
        passed=failed == 0,
        failed_count=failed,
        checked_count=total,
        is_critical=False,
    )
    keep_mask &= ~duplicate_mask

    # Rule: collected_at should fall on the sim_day being processed.
    collected_at_mask = raw["collected_at"].fillna("").str.startswith(sim_day)
    failed = int((~collected_at_mask).sum())
    record_data_quality(
        dsn,
        sim_day,
        "lab_ingest",
        "collected_at_within_sim_day",
        passed=failed == 0,
        failed_count=failed,
        checked_count=total,
        is_critical=False,
    )
    keep_mask &= collected_at_mask

    quarantined = raw[~keep_mask]
    if not quarantined.empty:
        os.makedirs(quarantine_dir, exist_ok=True)
        quarantined.to_csv(os.path.join(quarantine_dir, f"quarantine_{sim_day}.csv"), index=False)
        logger.warning("lab_rows_quarantined", stage="processing", sim_day=sim_day, count=len(quarantined))

    clean = raw[keep_mask].copy()
    clean["result_value"] = pd.to_numeric(clean["result_value"], errors="coerce")
    clean["reference_low"] = pd.to_numeric(clean["reference_low"], errors="coerce")
    clean["reference_high"] = pd.to_numeric(clean["reference_high"], errors="coerce")
    return clean


def flag_lab_results(clean_labs: pd.DataFrame) -> tuple[dict[str, dict[str, str]], list[dict]]:
    """Returns (lab_flags_by_patient, rows_for_storage)."""
    lab_flags_by_patient: dict[str, dict[str, str]] = {}
    rows_for_storage: list[dict] = []

    for _, row in clean_labs.iterrows():
        flag = lab_flag(row["result_value"], row["reference_low"], row["reference_high"], row["test_type"])
        lab_flags_by_patient.setdefault(row["patient_id"], {})[row["test_type"]] = flag
        rows_for_storage.append(
            {
                "patient_id": row["patient_id"],
                "test_type": row["test_type"],
                "result_value": float(row["result_value"]),
                "unit": row.get("unit"),
                "reference_low": row["reference_low"] if pd.notna(row["reference_low"]) else None,
                "reference_high": row["reference_high"] if pd.notna(row["reference_high"]) else None,
                "flag": flag,
                "collected_at": row.get("collected_at"),
                "lab_batch_id": row.get("lab_batch_id"),
            }
        )
    return lab_flags_by_patient, rows_for_storage


# --------------------------------------------------------------------------
# Step 5: the join -- combine vitals + labs into the daily risk picture
# --------------------------------------------------------------------------


def previous_adjusted_score(dsn: str, patient_id: str, sim_day: str) -> int | None:
    previous_day = (date.fromisoformat(sim_day) - timedelta(days=1)).isoformat()
    with get_connection(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT adjusted_risk_score FROM batch.patient_risk_daily WHERE patient_id = %s AND sim_day = %s",
                (patient_id, previous_day),
            )
            row = cur.fetchone()
    return row[0] if row else None


def build_risk_daily(
    patients: list[dict],
    daily_vitals: list[dict],
    lab_flags_by_patient: dict[str, dict[str, str]],
    sim_day: str,
    dsn: str,
) -> list[dict]:
    vitals_by_patient = {v["patient_id"]: v for v in daily_vitals}
    results = []

    for patient in patients:
        patient_id = patient["patient_id"]
        vitals = vitals_by_patient.get(patient_id)
        vitals_score = vitals["worst_score"] if vitals else 0
        vitals_band = vitals["worst_band"] if vitals else "low"
        vitals_reasons = vitals["vitals_reason_codes"] if vitals else []

        lab_flags = lab_flags_by_patient.get(patient_id, {})
        abnormal_test_count = sum(1 for f in lab_flags.values() if f in ("low", "high"))

        adjusted_score, lab_reasons = adjust_risk_with_labs(vitals_score, lab_flags)
        risk_band = risk_band_for_score(adjusted_score)
        reason_codes = sorted(set(vitals_reasons) | set(lab_reasons))

        previous_score = previous_adjusted_score(dsn, patient_id, sim_day)
        if previous_score is None:
            delta = None
            direction = "new"
        else:
            delta = adjusted_score - previous_score
            direction = trend_direction(adjusted_score, previous_score)

        results.append(
            {
                "patient_id": patient_id,
                "vitals_score": vitals_score,
                "vitals_band": vitals_band,
                "abnormal_test_count": abnormal_test_count,
                "adjusted_risk_score": adjusted_score,
                "risk_band": risk_band,
                "reason_codes": reason_codes,
                "previous_day_score": previous_score,
                "score_delta": delta,
                "delta_direction": direction,
            }
        )
    return results


# --------------------------------------------------------------------------
# Step 6: write results (delete-then-insert per sim_day, one transaction)
# --------------------------------------------------------------------------


def write_results(
    dsn: str,
    sim_day: str,
    daily_vitals: list[dict],
    lab_rows: list[dict],
    risk_daily: list[dict],
) -> int:
    rows_written = 0
    with get_connection(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM batch.patient_daily_vitals WHERE sim_day = %s", (sim_day,))
            cur.executemany(
                """
                INSERT INTO batch.patient_daily_vitals
                    (patient_id, sim_day, avg_hr, min_hr, max_hr, stddev_hr, avg_spo2, min_spo2,
                     avg_sbp, min_sbp, max_sbp, avg_dbp, avg_temp, min_temp, max_temp,
                     reading_count, worst_news2_score, worst_news2_band, vitals_reason_codes)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                [
                    (
                        v["patient_id"],
                        sim_day,
                        v["avg_hr"],
                        v["min_hr"],
                        v["max_hr"],
                        v["stddev_hr"],
                        v["avg_spo2"],
                        v["min_spo2"],
                        v["avg_sbp"],
                        v["min_sbp"],
                        v["max_sbp"],
                        v["avg_dbp"],
                        v["avg_temp"],
                        v["min_temp"],
                        v["max_temp"],
                        v["reading_count"],
                        v["worst_score"],
                        v["worst_band"],
                        json.dumps(v["vitals_reason_codes"]),
                    )
                    for v in daily_vitals
                ],
            )
            rows_written += len(daily_vitals)

            cur.execute("DELETE FROM batch.patient_lab_results WHERE sim_day = %s", (sim_day,))
            cur.executemany(
                """
                INSERT INTO batch.patient_lab_results
                    (patient_id, sim_day, test_type, result_value, unit, reference_low,
                     reference_high, flag, collected_at, lab_batch_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                [
                    (
                        r["patient_id"],
                        sim_day,
                        r["test_type"],
                        r["result_value"],
                        r["unit"],
                        r["reference_low"],
                        r["reference_high"],
                        r["flag"],
                        r["collected_at"],
                        r["lab_batch_id"],
                    )
                    for r in lab_rows
                ],
            )
            rows_written += len(lab_rows)

            cur.execute("DELETE FROM batch.patient_risk_daily WHERE sim_day = %s", (sim_day,))
            cur.executemany(
                """
                INSERT INTO batch.patient_risk_daily
                    (patient_id, sim_day, vitals_score, vitals_band, abnormal_test_count,
                     adjusted_risk_score, risk_band, reason_codes, previous_day_score,
                     score_delta, delta_direction)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                [
                    (
                        r["patient_id"],
                        sim_day,
                        r["vitals_score"],
                        r["vitals_band"],
                        r["abnormal_test_count"],
                        r["adjusted_risk_score"],
                        r["risk_band"],
                        json.dumps(r["reason_codes"]),
                        r["previous_day_score"],
                        r["score_delta"],
                        r["delta_direction"],
                    )
                    for r in risk_daily
                ],
            )
            rows_written += len(risk_daily)

    return rows_written


def main() -> None:
    parser = argparse.ArgumentParser(description="Lambda batch layer: daily patient risk recompute")
    parser.add_argument("--sim-day", required=True, help="Simulated day to (re)compute, e.g. 2026-01-05")
    args = parser.parse_args()
    sim_day = args.sim_day

    settings = get_settings()
    logger = configure_logging(SERVICE_NAME, logs_dir=os.environ.get("LOGS_DIR"))
    dsn = settings.postgres_dsn_vitals

    started_at = datetime.now(timezone.utc)
    logger.info("batch_job_starting", stage="processing", sim_day=sim_day)

    patients_csv = os.environ.get("PATIENTS_CSV", "/opt/airflow/project/config/patients.csv")
    master_dir = os.environ.get("MASTER_VITALS_DIR", settings.master_vitals_dir)
    landing_dir = os.environ.get("LANDING_LABS_DIR", settings.landing_labs_dir)
    quarantine_dir = os.environ.get("QUARANTINE_DIR", settings.quarantine_dir)

    patients = load_patients(patients_csv)
    known_patient_ids = {p["patient_id"] for p in patients}
    upsert_patients(dsn, patients)

    status = "failed"
    rows_written = 0
    error_message = None

    try:
        spark = build_spark(settings)
        spark.sparkContext.setLogLevel("WARN")

        daily_vitals = compute_daily_vitals(spark, master_dir, sim_day, logger)
        clean_labs = process_lab_file(landing_dir, sim_day, known_patient_ids, quarantine_dir, dsn, logger)
        lab_flags_by_patient, lab_rows = flag_lab_results(clean_labs)

        risk_daily = build_risk_daily(patients, daily_vitals, lab_flags_by_patient, sim_day, dsn)

        rows_written = write_results(dsn, sim_day, daily_vitals, lab_rows, risk_daily)
        status = "success"

        high_band_patients = [r["patient_id"] for r in risk_daily if r["risk_band"] == "high"]
        logger.info(
            "batch_job_complete",
            stage="processing",
            sim_day=sim_day,
            rows_written=rows_written,
            patients_scored=len(risk_daily),
            high_band_patients=high_band_patients,
        )
    except Exception as exc:  # noqa: BLE001
        error_message = str(exc)
        logger.error("batch_job_failed", stage="processing", sim_day=sim_day, error=error_message)
        raise
    finally:
        finished_at = datetime.now(timezone.utc)
        with get_connection(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ops.batch_runs (sim_day, started_at, finished_at, status, rows_written, error_message)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    """,
                    (sim_day, started_at, finished_at, status, rows_written, error_message),
                )


if __name__ == "__main__":
    main()
