"""
Spark Structured Streaming job: the LAMBDA SPEED LAYER.

Reads Kafka `vitals.raw`, cleans/dedups/enriches it, computes near-real-time
NEWS2-style risk scores over sliding windows, and raises clinical alerts on
sustained (not single-sample) deterioration. See PLAN.md Phase 3 for the
full design rationale. Three independent streaming queries share the same
upstream Kafka topic (a standard, fully-supported Structured Streaming
pattern -- each query is its own consumer group with its own checkpoint):

  Query 1 (malformed_query):  vitals.raw -> vitals.dlq (Kafka)
      Messages that fail from_json() parsing (the vitals producer's
      malformed-payload injection) are routed here, unmodified, with an
      error reason and the original Kafka metadata attached.

  Query 2 (clean_table_query): vitals.raw -> speed.vitals_clean (Postgres)
                                          -> vitals.clean (Kafka, for any
                                             future downstream consumer)
      Parses successfully, drops physiologically-implausible readings
      (common.clinical.validate_reading), then applies a watermarked
      dropDuplicates(["event_id"]) -- this is what makes the duplicate-id
      dirty-data injection get caught, and is why the watermark must be
      applied BEFORE this stateful operator (unbounded dedup state
      otherwise grows forever -- see PLAN.md's "classic memory leak" note).

  Query 3 (windowed_query): the same cleaned+deduped stream ->
      1-minute sliding windows (30s slide) -> worst-case NEWS2 scoring ->
      speed.patient_vitals_1m / speed.patient_risk_now (Postgres, idempotent
      upsert keyed on (patient_id, window_start)) -> sustained-alert
      detection (>=2 consecutive breached windows) -> speed.clinical_alerts
      (Postgres) + clinical.alerts (Kafka).

Sustained-alert state (the "was the previous window also breached"
comparison) is kept in a plain Python dict on the driver, updated inside
foreachBatch. This is a deliberate simplification given the very small state
size (one dict entry per patient, ~12 patients) -- documented as a limitation
in the report: this state is NOT checkpointed, so a stream restart resets the
alert streak counters (a fresh 2-window streak must reaccumulate). This does
NOT affect the windowed aggregates themselves, which remain correct/
idempotent across restarts via the Postgres upsert.
"""

from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql.functions import (
    avg,  # noqa: E402
    col,  # noqa: E402
    from_json,  # noqa: E402
    lit,  # noqa: E402
    stddev,  # noqa: E402
    struct,  # noqa: E402
    to_json,  # noqa: E402
    to_timestamp,  # noqa: E402
    udf,  # noqa: E402
    window,  # noqa: E402
)
from pyspark.sql.functions import count as spark_count  # noqa: E402
from pyspark.sql.functions import max as spark_max  # noqa: E402
from pyspark.sql.functions import min as spark_min  # noqa: E402
from pyspark.sql.types import BooleanType  # noqa: E402

from common.clinical import news2_score_from_window, trend_direction, validate_reading  # noqa: E402
from common.db import get_connection, upsert_pipeline_heartbeat  # noqa: E402
from common.logging_setup import configure_logging  # noqa: E402
from common.schemas import vitals_struct_type  # noqa: E402
from config.settings import get_settings  # noqa: E402

SERVICE_NAME = "spark-stream"


def build_spark(settings) -> SparkSession:
    return (
        SparkSession.builder.appName("speed-layer")
        .config("spark.sql.shuffle.partitions", settings.spark_shuffle_partitions)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.postgresql:postgresql:42.7.4",
        )
        .config("spark.jars.ivy", "/opt/ivy")
        # Exposes Spark's own executor/driver metrics on the Spark UI port
        # (4040) at /metrics/executors/prometheus -- scraped by Prometheus
        # per observability/prometheus.yml's "spark-stream" job.
        .config("spark.ui.prometheus.enabled", "true")
        .getOrCreate()
    )


def is_valid_udf():
    def _check(hr, spo2, sbp, dbp, temp) -> bool:
        ok, _ = validate_reading(hr, spo2, sbp, dbp, temp)
        return ok

    return udf(_check, BooleanType())


# --------------------------------------------------------------------------
# Query 1: malformed -> DLQ
# --------------------------------------------------------------------------


def make_dlq_sink(settings, logger):
    def sink(batch_df: DataFrame, batch_id: int) -> None:
        count = batch_df.count()
        if count == 0:
            return
        dlq_df = batch_df.select(
            col("key").alias("key"),
            to_json(
                struct(
                    col("value").cast("string").alias("raw_value"),
                    lit("json_parse_failed").alias("error"),
                    col("partition").alias("kafka_partition"),
                    col("offset").alias("kafka_offset"),
                    col("timestamp").cast("string").alias("kafka_timestamp"),
                )
            ).alias("value"),
        )
        (
            dlq_df.write.format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka_bootstrap_internal)
            .option("topic", settings.topic_vitals_dlq)
            .save()
        )
        logger.warning("dlq_batch_routed", stage="processing", batch_id=batch_id, count=count)

    return sink


# --------------------------------------------------------------------------
# Query 2: clean + dedup -> Postgres speed.vitals_clean + Kafka vitals.clean
# --------------------------------------------------------------------------


def make_clean_table_sink(settings, logger):
    dsn = settings.postgres_dsn_vitals

    def sink(batch_df: DataFrame, batch_id: int) -> None:
        count = batch_df.count()
        if count == 0:
            return

        rows = batch_df.select(
            "event_id",
            "patient_id",
            "heart_rate",
            "spo2",
            "systolic_bp",
            "diastolic_bp",
            "temperature",
            "event_ts",
            "producer_ts",
            "sim_day",
            "trace_id",
        ).collect()

        with get_connection(dsn) as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO speed.vitals_clean
                        (event_id, patient_id, heart_rate, spo2, systolic_bp,
                         diastolic_bp, temperature, event_ts, producer_ts, sim_day, trace_id)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    [
                        (
                            r.event_id,
                            r.patient_id,
                            r.heart_rate,
                            r.spo2,
                            r.systolic_bp,
                            r.diastolic_bp,
                            r.temperature,
                            r.event_ts,
                            r.producer_ts,
                            r.sim_day,
                            r.trace_id,
                        )
                        for r in rows
                    ],
                )

        clean_kafka_df = batch_df.select(
            col("patient_id").alias("key"),
            to_json(
                struct(
                    "event_id",
                    "patient_id",
                    "heart_rate",
                    "spo2",
                    "systolic_bp",
                    "diastolic_bp",
                    "temperature",
                    "event_ts",
                    "producer_ts",
                    "sim_day",
                    "trace_id",
                )
            ).alias("value"),
        )
        (
            clean_kafka_df.write.format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka_bootstrap_internal)
            .option("topic", settings.topic_vitals_clean)
            .save()
        )

        logger.info("clean_batch_written", stage="processing", batch_id=batch_id, count=count)
        try:
            upsert_pipeline_heartbeat(dsn, service=SERVICE_NAME, stage="processing", records_processed=count)
        except Exception as exc:  # noqa: BLE001
            logger.warning("heartbeat_write_failed", stage="processing", error=str(exc))

    return sink


# --------------------------------------------------------------------------
# Query 3: windowed aggregation -> scoring -> sustained-alert detection
# --------------------------------------------------------------------------


class AlertState:
    """Driver-local (not checkpointed -- see module docstring) per-patient
    sustained-breach tracking, keyed by patient_id."""

    def __init__(self, required_consecutive: int):
        self.required_consecutive = required_consecutive
        self._last_window_start: dict[str, str] = {}
        self._consecutive_breaches: dict[str, int] = {}
        self._already_alerted_this_streak: dict[str, bool] = {}
        self._last_score: dict[str, int] = {}

    def previous_score(self, patient_id: str) -> int | None:
        return self._last_score.get(patient_id)

    def observe(self, patient_id: str, window_start: str, band: str, score: int) -> bool:
        """Update state for this patient's newly-finalized window and return
        True if a NEW sustained alert should fire right now (i.e. the streak
        just crossed the threshold and hasn't already been alerted)."""
        is_new_window = self._last_window_start.get(patient_id) != window_start
        self._last_window_start[patient_id] = window_start
        self._last_score[patient_id] = score

        is_breach = band == "high"

        if not is_new_window:
            # Same window re-emitted (Structured Streaming "update" mode can
            # re-emit an in-progress window multiple times) -- don't double-count.
            return False

        if is_breach:
            self._consecutive_breaches[patient_id] = self._consecutive_breaches.get(patient_id, 0) + 1
        else:
            self._consecutive_breaches[patient_id] = 0
            self._already_alerted_this_streak[patient_id] = False
            return False

        streak = self._consecutive_breaches[patient_id]
        if streak >= self.required_consecutive and not self._already_alerted_this_streak.get(patient_id):
            self._already_alerted_this_streak[patient_id] = True
            return True
        return False


def make_windowed_sink(settings, logger, alert_state: AlertState):
    dsn = settings.postgres_dsn_vitals

    def sink(batch_df: DataFrame, batch_id: int) -> None:
        rows = batch_df.collect()  # tiny: <= num_patients rows per trigger
        if not rows:
            return

        vitals_upserts = []
        risk_upserts = []
        alerts_to_insert = []
        kafka_alert_payloads = []

        for r in rows:
            score, band, components = news2_score_from_window(
                min_hr=r.min_hr,
                max_hr=r.max_hr,
                min_spo2=r.min_spo2,
                min_sbp=r.min_sbp,
                max_sbp=r.max_sbp,
                min_temp=r.min_temp,
                max_temp=r.max_temp,
            )
            previous = alert_state.previous_score(r.patient_id)
            trend = trend_direction(score, previous)
            window_start_str = str(r.window_start)

            vitals_upserts.append(
                (
                    r.patient_id,
                    r.window_start,
                    r.window_end,
                    r.avg_hr,
                    r.min_hr,
                    r.max_hr,
                    r.stddev_hr,
                    r.avg_spo2,
                    r.min_spo2,
                    r.avg_sbp,
                    r.min_sbp,
                    r.max_sbp,
                    r.avg_dbp,
                    r.avg_temp,
                    r.min_temp,
                    r.max_temp,
                    r.reading_count,
                )
            )
            risk_upserts.append((r.patient_id, r.window_start, score, band, trend, json.dumps(components)))

            should_alert = alert_state.observe(r.patient_id, window_start_str, band, score)
            if should_alert:
                alerts_to_insert.append((r.patient_id, r.window_start, score, band, json.dumps(components)))
                kafka_alert_payloads.append(
                    {
                        "patient_id": r.patient_id,
                        "window_start": window_start_str,
                        "score": score,
                        "band": band,
                        "components": components,
                    }
                )

        with get_connection(dsn) as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO speed.patient_vitals_1m
                        (patient_id, window_start, window_end, avg_hr, min_hr, max_hr, stddev_hr,
                         avg_spo2, min_spo2, avg_sbp, min_sbp, max_sbp, avg_dbp,
                         avg_temp, min_temp, max_temp, reading_count)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (patient_id, window_start) DO UPDATE SET
                        window_end = EXCLUDED.window_end,
                        avg_hr = EXCLUDED.avg_hr, min_hr = EXCLUDED.min_hr, max_hr = EXCLUDED.max_hr,
                        stddev_hr = EXCLUDED.stddev_hr,
                        avg_spo2 = EXCLUDED.avg_spo2, min_spo2 = EXCLUDED.min_spo2,
                        avg_sbp = EXCLUDED.avg_sbp, min_sbp = EXCLUDED.min_sbp, max_sbp = EXCLUDED.max_sbp,
                        avg_dbp = EXCLUDED.avg_dbp,
                        avg_temp = EXCLUDED.avg_temp, min_temp = EXCLUDED.min_temp, max_temp = EXCLUDED.max_temp,
                        reading_count = EXCLUDED.reading_count
                    """,
                    vitals_upserts,
                )
                cur.executemany(
                    """
                    INSERT INTO speed.patient_risk_now
                        (patient_id, window_start, score, risk_band, trend, components)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (patient_id, window_start) DO UPDATE SET
                        score = EXCLUDED.score,
                        risk_band = EXCLUDED.risk_band,
                        trend = EXCLUDED.trend,
                        components = EXCLUDED.components
                    """,
                    risk_upserts,
                )
                if alerts_to_insert:
                    cur.executemany(
                        """
                        INSERT INTO speed.clinical_alerts
                            (patient_id, window_start, score, risk_band, components, raised_at)
                        VALUES (%s,%s,%s,%s,%s, now())
                        """,
                        alerts_to_insert,
                    )

        if kafka_alert_payloads:
            spark = batch_df.sparkSession
            alerts_df = spark.createDataFrame(
                [(p["patient_id"], json.dumps(p)) for p in kafka_alert_payloads], ["key", "value"]
            )
            (
                alerts_df.write.format("kafka")
                .option("kafka.bootstrap.servers", settings.kafka_bootstrap_internal)
                .option("topic", settings.topic_clinical_alerts)
                .save()
            )
            for p in kafka_alert_payloads:
                logger.warning(
                    "clinical_alert_raised",
                    stage="processing",
                    patient_id=p["patient_id"],
                    score=p["score"],
                    band=p["band"],
                )

        logger.info(
            "windowed_batch_written",
            stage="processing",
            batch_id=batch_id,
            windows=len(rows),
            alerts=len(alerts_to_insert),
        )
        try:
            upsert_pipeline_heartbeat(
                dsn, service=SERVICE_NAME, stage="processing", records_processed=len(rows)
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("heartbeat_write_failed", stage="processing", error=str(exc))

    return sink


def main() -> None:
    settings = get_settings()
    logger = configure_logging(SERVICE_NAME, logs_dir=os.environ.get("LOGS_DIR"))

    checkpoint_root = os.environ.get("CHECKPOINT_STREAM_DIR", settings.checkpoint_stream_dir)
    schema = vitals_struct_type()

    spark = build_spark(settings)
    spark.sparkContext.setLogLevel("WARN")

    logger.info("stream_job_starting", stage="processing", topic=settings.topic_vitals_raw)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_internal)
        .option("subscribe", settings.topic_vitals_raw)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = raw.select(
        col("key"),
        col("value"),
        col("partition"),
        col("offset"),
        col("timestamp"),
        from_json(col("value").cast("string"), schema).alias("p"),
    )

    # ---- Query 1: malformed -> DLQ (no stateful ops needed) ----------------
    malformed = parsed.filter(col("p").isNull()).select("key", "value", "partition", "offset", "timestamp")
    malformed_query = (
        malformed.writeStream.foreachBatch(make_dlq_sink(settings, logger))
        .option("checkpointLocation", os.path.join(checkpoint_root, "dlq"))
        .trigger(processingTime=f"{settings.stream_trigger_seconds} seconds")
        .start()
    )

    # ---- Query 2: valid -> drop-implausible -> watermark+dedup -> clean sinks
    valid = (
        parsed.filter(col("p").isNotNull())
        .select("p.*")
        .withColumn("event_ts_ts", to_timestamp(col("event_ts")))
    )
    valid_checked = valid.filter(
        is_valid_udf()(
            col("heart_rate"), col("spo2"), col("systolic_bp"), col("diastolic_bp"), col("temperature")
        )
    )
    valid_deduped = valid_checked.withWatermark(
        "event_ts_ts", f"{settings.stream_watermark_seconds} seconds"
    ).dropDuplicates(["event_id"])

    clean_query = (
        valid_deduped.writeStream.foreachBatch(make_clean_table_sink(settings, logger))
        .option("checkpointLocation", os.path.join(checkpoint_root, "clean"))
        .trigger(processingTime=f"{settings.stream_trigger_seconds} seconds")
        .start()
    )

    # ---- Query 3: windowed aggregation -> scoring -> sustained alerts ------
    windowed = (
        valid_deduped.groupBy(
            window(
                col("event_ts_ts"),
                f"{settings.window_duration_minutes} minute",
                f"{settings.window_slide_seconds} seconds",
            ),
            col("patient_id"),
        )
        .agg(
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
        )
        .selectExpr(
            "patient_id",
            "window.start as window_start",
            "window.end as window_end",
            "avg_hr",
            "min_hr",
            "max_hr",
            "stddev_hr",
            "avg_spo2",
            "min_spo2",
            "avg_sbp",
            "min_sbp",
            "max_sbp",
            "avg_dbp",
            "avg_temp",
            "min_temp",
            "max_temp",
            "reading_count",
        )
    )

    alert_state = AlertState(required_consecutive=settings.sustained_alert_windows)

    windowed_query = (
        windowed.writeStream.outputMode("update")
        .foreachBatch(make_windowed_sink(settings, logger, alert_state))
        .option("checkpointLocation", os.path.join(checkpoint_root, "windowed"))
        .trigger(processingTime=f"{settings.stream_trigger_seconds} seconds")
        .start()
    )

    queries = [malformed_query, clean_query, windowed_query]
    logger.info(
        "stream_job_all_queries_started",
        stage="processing",
        query_ids=[str(q.id) for q in queries],
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
