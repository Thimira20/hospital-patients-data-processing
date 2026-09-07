"""
Spark Structured Streaming job: Kafka `vitals.raw` -> Parquet master dataset.

This is deliberately a SEPARATE job from the speed layer (processing/stream_job.py)
so the immutable master dataset keeps growing even while the analytics logic in
the speed layer is being changed, redeployed, or is temporarily down. This is
the Lambda "master dataset" writer referenced throughout PLAN.md and the report.

Design notes:
  * Every message is archived, including ones that fail JSON parsing
    (malformed-payload injection in the vitals producer) -- the raw string is
    kept in the `raw_value` column regardless of parse success, so no data is
    ever silently dropped from the master dataset. A message that fails to
    parse (and therefore has no `sim_day` field) falls back to the archiver's
    own current simulated day for partitioning purposes.
  * Partitioned by `dt` (the simulated-calendar day label, e.g. "2026-01-03")
    -- NOT the real calendar date -- because that is the unit batch_job.py
    recomputes over. Sub-partitioned by `hour` (the real wall-clock hour, for
    file-size management only; it carries no batch-recompute meaning).
  * `startingOffsets=earliest` only takes effect the first time this job runs
    (no checkpoint yet) -- guarantees the master dataset captures everything
    since the vitals producer started, not just from whenever the archiver
    happened to first come up. All subsequent restarts resume from the
    checkpoint.
  * Uses `foreachBatch` (not a native partitioned writeStream sink) so it can
    inject the `dt` fallback and update ops.pipeline_heartbeat after every
    micro-batch.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql.functions import coalesce, col, date_format, from_json, lit  # noqa: E402

from common.db import upsert_pipeline_heartbeat  # noqa: E402
from common.logging_setup import configure_logging  # noqa: E402
from common.schemas import vitals_struct_type  # noqa: E402
from config.settings import get_settings  # noqa: E402
from ingestion.sim_clock import SimClock  # noqa: E402

SERVICE_NAME = "spark-archiver"


def build_spark(settings) -> SparkSession:
    return (
        SparkSession.builder.appName("raw-archiver")
        .config("spark.sql.shuffle.partitions", settings.spark_shuffle_partitions)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.postgresql:postgresql:42.7.4",
        )
        # Must match the --conf spark.jars.ivy path used in docker/spark.Dockerfile's
        # build-time warmup step, or this SparkSession would re-download the
        # packages from the internet on every container start instead of
        # reusing the cache baked into the image.
        .config("spark.jars.ivy", "/opt/ivy")
        .getOrCreate()
    )


def make_process_batch(settings, sim_clock: SimClock, logger):
    dsn = settings.postgres_dsn_vitals
    master_dir = os.environ.get("MASTER_VITALS_DIR", settings.master_vitals_dir)
    schema = vitals_struct_type()

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        input_count = batch_df.count()
        if input_count == 0:
            return

        current_sim_day = sim_clock.sim_day()

        parsed = batch_df.select(
            col("key").cast("string").alias("kafka_key"),
            col("timestamp").alias("kafka_timestamp"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("value").cast("string").alias("raw_value"),
            from_json(col("value").cast("string"), schema).alias("parsed"),
        )

        enriched = (
            parsed.select(
                "kafka_key",
                "kafka_timestamp",
                "kafka_partition",
                "kafka_offset",
                "raw_value",
                "parsed.*",
            )
            .withColumn("dt", coalesce(col("sim_day"), lit(current_sim_day)))
            .withColumn("hour", date_format(col("kafka_timestamp"), "HH"))
        )

        (enriched.write.mode("append").partitionBy("dt", "hour").parquet(master_dir))

        unparsed_count = enriched.filter(col("event_id").isNull()).count()

        logger.info(
            "archiver_batch_written",
            stage="storage",
            batch_id=batch_id,
            input_rows=input_count,
            unparsed_rows=unparsed_count,
        )

        try:
            upsert_pipeline_heartbeat(
                dsn,
                service=SERVICE_NAME,
                stage="storage",
                records_processed=input_count,
                errors=unparsed_count,
                detail={"batch_id": batch_id, "sim_day": current_sim_day},
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("heartbeat_write_failed", stage="storage", error=str(exc))

    return process_batch


def main() -> None:
    settings = get_settings()
    logger = configure_logging(SERVICE_NAME, logs_dir=os.environ.get("LOGS_DIR"))

    data_root = os.environ.get("DATA_ROOT", settings.data_root)
    sim_clock_path = os.path.join(data_root, "sim_clock.json")
    sim_clock = SimClock.load_or_create(settings.sim_start_date, settings.sim_day_seconds, sim_clock_path)

    checkpoint_dir = os.environ.get("CHECKPOINT_ARCHIVER_DIR", settings.checkpoint_archiver_dir)

    spark = build_spark(settings)
    spark.sparkContext.setLogLevel("WARN")

    logger.info("archiver_starting", stage="storage", topic=settings.topic_vitals_raw)

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_internal)
        .option("subscribe", settings.topic_vitals_raw)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    query = (
        raw_stream.writeStream.foreachBatch(make_process_batch(settings, sim_clock, logger))
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime="{} seconds".format(settings.archiver_trigger_seconds))
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
