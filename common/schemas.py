"""
Wire-format schemas shared across the pipeline.

Two kinds of definitions live here:

1. **Pydantic models** (`VitalReading`, `LabResult`) -- the canonical field
   list/types for the two event kinds. Used by the producers to build valid
   JSON payloads, and safe to import from a lightweight container that does
   NOT have PySpark installed (no pyspark import at module load time).

2. **Spark StructType factories** (`vitals_struct_type()`, `lab_struct_type()`)
   -- used by `processing/stream_job.py` and `processing/batch_job.py` to
   parse Kafka/CSV payloads with an explicit schema (never `inferSchema` on a
   stream). PySpark is imported *lazily inside these functions* specifically
   so that importing `common.schemas` elsewhere (e.g. from the producer
   image, which has no PySpark) never fails or pulls in the JVM dependency.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class VitalReading(BaseModel):
    """One bedside-monitor reading, as published to Kafka topic `vitals.raw`.

    Fields are Optional where dirty-data injection may deliberately null them
    out (see ingestion/vitals_producer.py) -- validity is judged later by
    common/clinical.py's validate_reading(), not by this wire schema.
    """

    event_id: str
    patient_id: str
    heart_rate: Optional[float] = None
    spo2: Optional[float] = None
    systolic_bp: Optional[float] = None
    diastolic_bp: Optional[float] = None
    temperature: Optional[float] = None
    event_ts: str = Field(..., description="Real wall-clock UTC ISO-8601 timestamp")
    sim_day: str = Field(..., description="Simulated calendar day label, e.g. 2026-01-03")
    producer_ts: str = Field(..., description="Real wall-clock time the producer sent this event")
    trace_id: str


class LabResult(BaseModel):
    """One lab test result row, as written to the daily lab CSV file."""

    patient_id: str
    test_type: str
    result_value: Optional[float] = None
    unit: Optional[str] = None
    reference_low: Optional[float] = None
    reference_high: Optional[float] = None
    collected_at: str
    lab_batch_id: str
    sim_day: str


def vitals_struct_type():
    """Explicit Spark schema for parsing `vitals.raw` JSON payloads."""
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    return StructType(
        [
            StructField("event_id", StringType(), nullable=False),
            StructField("patient_id", StringType(), nullable=False),
            StructField("heart_rate", DoubleType(), nullable=True),
            StructField("spo2", DoubleType(), nullable=True),
            StructField("systolic_bp", DoubleType(), nullable=True),
            StructField("diastolic_bp", DoubleType(), nullable=True),
            StructField("temperature", DoubleType(), nullable=True),
            StructField("event_ts", StringType(), nullable=False),
            StructField("sim_day", StringType(), nullable=False),
            StructField("producer_ts", StringType(), nullable=False),
            StructField("trace_id", StringType(), nullable=False),
        ]
    )


def lab_struct_type():
    """Explicit Spark schema for reading the daily lab CSV files."""
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    return StructType(
        [
            StructField("patient_id", StringType(), nullable=True),
            StructField("test_type", StringType(), nullable=True),
            StructField("result_value", DoubleType(), nullable=True),
            StructField("unit", StringType(), nullable=True),
            StructField("reference_low", DoubleType(), nullable=True),
            StructField("reference_high", DoubleType(), nullable=True),
            StructField("collected_at", StringType(), nullable=True),
            StructField("lab_batch_id", StringType(), nullable=True),
            StructField("sim_day", StringType(), nullable=True),
        ]
    )


def patients_struct_type():
    """Explicit Spark schema for reading config/patients.csv."""
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    return StructType(
        [
            StructField("patient_id", StringType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("age", IntegerType(), nullable=True),
            StructField("sex", StringType(), nullable=True),
            StructField("ward", StringType(), nullable=True),
            StructField("bed", StringType(), nullable=True),
            StructField("admitted_at", StringType(), nullable=True),
            StructField("baseline_hr", DoubleType(), nullable=True),
            StructField("baseline_spo2", DoubleType(), nullable=True),
            StructField("baseline_sbp", DoubleType(), nullable=True),
            StructField("baseline_dbp", DoubleType(), nullable=True),
            StructField("baseline_temp", DoubleType(), nullable=True),
        ]
    )
