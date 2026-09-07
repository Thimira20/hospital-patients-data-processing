"""
Build a SparkSession once at image-build time so that the Kafka and
PostgreSQL connector jars (pulled via --packages/ivy) are cached inside the
image layer. This means spark-stream / spark-archiver / the Airflow
container's spark-submit never need network access at *runtime* -- important
for a laptop demo where you don't want the pipeline to depend on internet
availability once it's built.
"""

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("warmup").master("local[1]").getOrCreate()
    print("Spark warmup OK:", spark.version)
    spark.stop()
