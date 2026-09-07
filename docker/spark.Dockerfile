# Base image for every Spark workload in this project: the speed-layer
# streaming job (processing/stream_job.py), the raw archiver
# (processing/raw_archiver.py), and the batch job (processing/batch_job.py,
# invoked from inside the Airflow container which uses this same jar cache).
FROM python:3.11-slim

# openjdk-17-jre-headless: Spark 3.5 requires a JVM.
# procps: gives Spark's scripts `ps`, needed by spark-submit's process checks.
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYSPARK_PYTHON=python3 \
    PYTHONUNBUFFERED=1

WORKDIR /opt/app

# Python deps first (better layer caching than copying the whole repo first).
COPY docker/requirements-spark.txt /opt/app/requirements-spark.txt
RUN pip install --no-cache-dir -r /opt/app/requirements-spark.txt

# Pre-download the Spark SQL Kafka connector + Postgres JDBC driver into the
# image so no internet is needed at runtime (see spark_warmup.py docstring).
COPY docker/spark_warmup.py /opt/app/spark_warmup.py
RUN spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.postgresql:postgresql:42.7.4 \
      --conf spark.jars.ivy=/opt/ivy \
      /opt/app/spark_warmup.py

# Application code is bind-mounted at runtime (see docker-compose.yml) so
# edits during development don't require a rebuild; this COPY is a fallback
# for when the image is used standalone (e.g. built and pushed elsewhere).
COPY common/ /opt/app/common/
COPY config/ /opt/app/config/
COPY processing/ /opt/app/processing/

ENV PYTHONPATH=/opt/app

ENTRYPOINT ["python3"]
