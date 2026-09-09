# Lightweight image for pure-Python services that do NOT need a JVM/PySpark:
# the two ingestion producers (Phase 2) and the FastAPI serving app (Phase 5).
# Kept separate from docker/spark.Dockerfile so these containers start in a
# couple of seconds instead of carrying a JDK + Spark install they never use.
FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY docker/requirements-app.txt /app/requirements-app.txt
RUN pip install --no-cache-dir -r /app/requirements-app.txt

# Application code is bind-mounted at runtime in docker-compose.yml for fast
# iteration; these COPYs are the fallback for a standalone image build.
COPY common/ /app/common/
COPY config/ /app/config/
COPY ingestion/ /app/ingestion/
COPY serving/ /app/serving/

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["python3"]
