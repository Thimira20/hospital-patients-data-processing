# Airflow image used for BOTH the webserver and the scheduler (they share one
# image in this project, distinguished only by the command docker-compose
# gives each container). Runs the batch job via `spark-submit` INSIDE this
# same container (local Spark mode) rather than talking to a separate Spark
# cluster or the Docker socket -- the most reliable option on Windows (see
# PLAN.md Phase 4, task 1).
FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless procps && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
COPY docker/requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt

# Pre-cache the Spark Kafka+Postgres connector jars at build time (same
# rationale as docker/spark.Dockerfile: no internet needed at runtime for
# spark-submit inside the DAG's run_batch_job task). Uses /opt/airflow/ivy
# (NOT /opt/ivy like spark.Dockerfile) because this build runs as the
# non-root `airflow` user, who owns /opt/airflow but not /opt --
# processing/batch_job.py's build_spark() points at this same path.
COPY docker/spark_warmup.py /opt/airflow/spark_warmup.py
RUN spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.postgresql:postgresql:42.7.4 \
      --conf spark.jars.ivy=/opt/airflow/ivy \
      /opt/airflow/spark_warmup.py

ENV PYTHONPATH=/opt/airflow/project
