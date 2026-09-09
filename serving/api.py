"""
FastAPI serving layer -- the "API endpoint for real-time ward monitoring
figures" the use case's suggested outputs ask for, and the merge point where
the speed layer's "right now" and the batch layer's "as of yesterday" answer
the business question together.

Every endpoint reads from Postgres via common.db (same DSN, same module
every other service uses -- no separate config surface). A background task
started at app startup polls a handful of cheap queries every
METRICS_POLL_SECONDS and republishes them as Prometheus gauges, so pipeline
health (heartbeat age, batch freshness, data-quality failures) is visible at
/metrics alongside the automatic per-route HTTP metrics from
prometheus-fastapi-instrumentator -- both share the same default registry,
so one /metrics endpoint serves both without extra wiring.

Every request is logged as one structured JSON line carrying an
X-Trace-Id header (generated per-request, or echoed back if the caller
supplied one) -- this is what lets a demo grep one trace_id across
data/logs/*.jsonl and show a single vitals reading's journey from producer
to Kafka to Spark to this API response.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from contextlib import asynccontextmanager
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from prometheus_client import Gauge
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.db import get_connection  # noqa: E402
from common.logging_setup import configure_logging, new_trace_id  # noqa: E402
from config.settings import get_settings  # noqa: E402

SETTINGS = get_settings()
LOGGER = configure_logging("api", logs_dir=os.environ.get("LOGS_DIR"))

METRICS_POLL_SECONDS = 15

# ---- Prometheus gauges kept fresh by the background poller ----------------
HEARTBEAT_AGE = Gauge(
    "ops_pipeline_heartbeat_last_seen_seconds",
    "Seconds since each pipeline service last reported a heartbeat",
    ["service"],
)
BATCH_LAST_SUCCESS_AGE = Gauge(
    "ops_batch_last_success_seconds", "Seconds since the last successful batch_job run"
)
SPEED_LATEST_WINDOW_AGE = Gauge(
    "speed_latest_window_age_seconds", "Seconds since the most recent speed-layer window was written"
)
DATA_QUALITY_CRITICAL_FAILURES = Gauge(
    "ops_data_quality_critical_failures",
    "Count of critical data-quality rule failures for the most recent sim_day checked",
)


def _dsn() -> str:
    return SETTINGS.postgres_dsn_vitals


async def poll_metrics_loop() -> None:
    while True:
        try:
            _refresh_gauges()
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("metrics_poll_failed", stage="serving", error=str(exc))
        await asyncio.sleep(METRICS_POLL_SECONDS)


def _refresh_gauges() -> None:
    with get_connection(_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT service, EXTRACT(EPOCH FROM (now() - last_seen_at)) FROM ops.pipeline_heartbeat"
            )
            for service, age in cur.fetchall():
                HEARTBEAT_AGE.labels(service=service).set(float(age))

            cur.execute(
                "SELECT EXTRACT(EPOCH FROM (now() - finished_at)) FROM ops.batch_runs "
                "WHERE status = 'success' ORDER BY finished_at DESC LIMIT 1"
            )
            row = cur.fetchone()
            if row:
                BATCH_LAST_SUCCESS_AGE.set(float(row[0]))

            cur.execute("SELECT EXTRACT(EPOCH FROM (now() - max(window_end))) FROM speed.patient_vitals_1m")
            row = cur.fetchone()
            if row and row[0] is not None:
                SPEED_LATEST_WINDOW_AGE.set(float(row[0]))

            cur.execute(
                "SELECT COALESCE(SUM(failed_count), 0) FROM ops.data_quality "
                "WHERE is_critical AND NOT passed AND sim_day = (SELECT max(sim_day) FROM ops.data_quality)"
            )
            row = cur.fetchone()
            DATA_QUALITY_CRITICAL_FAILURES.set(float(row[0]) if row else 0.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(poll_metrics_loop())
    LOGGER.info("api_started", stage="serving")
    yield
    task.cancel()


app = FastAPI(title="Hospital Vitals Monitoring API", version="1.0.0", lifespan=lifespan)
Instrumentator().instrument(app).expose(app)


@app.middleware("http")
async def trace_id_middleware(request: Request, call_next):
    trace_id = request.headers.get("X-Trace-Id", new_trace_id())
    start = time.time()
    response = await call_next(request)
    duration_ms = round((time.time() - start) * 1000, 2)
    response.headers["X-Trace-Id"] = trace_id
    LOGGER.info(
        "api_request",
        stage="serving",
        trace_id=trace_id,
        method=request.method,
        path=request.url.path,
        status_code=response.status_code,
        duration_ms=duration_ms,
    )
    return response


# --------------------------------------------------------------------------
# Response models
# --------------------------------------------------------------------------


class HealthResponse(BaseModel):
    status: str
    postgres: bool
    kafka: bool


class ReadyResponse(BaseModel):
    ready: bool
    heartbeat_fresh: bool
    serving_view_populated: bool


class WardStatusResponse(BaseModel):
    patients_monitored: int
    readings_last_5min: int
    risk_band_counts: dict[str, int]
    active_alerts: int
    avg_score_by_ward: dict[str, Optional[float]]


class AckRequest(BaseModel):
    acknowledged_by: Optional[str] = None


class AlertmanagerWebhook(BaseModel):
    receiver: Optional[str] = None
    status: Optional[str] = "firing"
    alerts: list[dict] = []


# --------------------------------------------------------------------------
# Health / readiness
# --------------------------------------------------------------------------


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    postgres_ok = True
    try:
        with get_connection(_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
    except Exception:  # noqa: BLE001
        postgres_ok = False

    kafka_ok = True
    try:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": SETTINGS.kafka_bootstrap_internal})
        admin.list_topics(timeout=3)
    except Exception:  # noqa: BLE001
        kafka_ok = False

    status = "ok" if (postgres_ok and kafka_ok) else "degraded"
    return HealthResponse(status=status, postgres=postgres_ok, kafka=kafka_ok)


@app.get("/ready", response_model=ReadyResponse)
def ready() -> ReadyResponse:
    heartbeat_fresh = False
    serving_populated = False
    try:
        with get_connection(_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM ops.pipeline_heartbeat WHERE now() - last_seen_at < interval '5 minutes'"
                )
                heartbeat_fresh = cur.fetchone()[0] > 0

                cur.execute("SELECT count(*) FROM serving.patient_360")
                serving_populated = cur.fetchone()[0] > 0
    except Exception:  # noqa: BLE001
        pass

    is_ready = heartbeat_fresh and serving_populated
    body = ReadyResponse(
        ready=is_ready, heartbeat_fresh=heartbeat_fresh, serving_view_populated=serving_populated
    )
    if not is_ready:
        return JSONResponse(status_code=503, content=json.loads(body.model_dump_json()))
    return body


# --------------------------------------------------------------------------
# Ward / patients
# --------------------------------------------------------------------------


@app.get("/ward/status", response_model=WardStatusResponse)
def ward_status() -> WardStatusResponse:
    with get_connection(_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(DISTINCT patient_id) FROM reference.patients")
            patients_monitored = cur.fetchone()[0]

            cur.execute(
                "SELECT count(*) FROM speed.vitals_clean WHERE inserted_at >= now() - interval '5 minutes'"
            )
            readings_last_5min = cur.fetchone()[0]

            cur.execute("""
                SELECT current_risk_band, count(*) FROM serving.patient_360
                WHERE current_risk_band IS NOT NULL GROUP BY current_risk_band
                """)
            risk_band_counts = {band: count for band, count in cur.fetchall()}

            cur.execute(
                "SELECT count(*) FROM speed.clinical_alerts WHERE NOT acknowledged AND raised_at >= now() - interval '24 hours'"
            )
            active_alerts = cur.fetchone()[0]

            cur.execute("SELECT ward, avg(current_score) FROM serving.patient_360 GROUP BY ward")
            avg_score_by_ward = {
                ward: (float(avg) if avg is not None else None) for ward, avg in cur.fetchall()
            }

    return WardStatusResponse(
        patients_monitored=patients_monitored,
        readings_last_5min=readings_last_5min,
        risk_band_counts=risk_band_counts,
        active_alerts=active_alerts,
        avg_score_by_ward=avg_score_by_ward,
    )


@app.get("/patients")
def list_patients():
    with get_connection(_dsn()) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT patient_id, name, ward, bed, current_score, current_risk_band, current_trend
                FROM serving.patient_360 ORDER BY patient_id
                """)
            return [dict(r) for r in cur.fetchall()]


@app.get("/patients/{patient_id}/risk")
def patient_risk(patient_id: str):
    with get_connection(_dsn()) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM serving.patient_360 WHERE patient_id = %s", (patient_id,))
            row = cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Unknown patient_id {patient_id}")

    result = dict(row)
    return {
        "patient_id": result["patient_id"],
        "name": result["name"],
        "ward": result["ward"],
        "bed": result["bed"],
        "realtime": {
            "window_start": result["current_window_start"],
            "score": result["current_score"],
            "risk_band": result["current_risk_band"],
            "trend": result["current_trend"],
        },
        "batch_lab_adjusted": {
            "sim_day": result["batch_sim_day"],
            "vitals_band": result["batch_vitals_band"],
            "adjusted_score": result["batch_adjusted_score"],
            "risk_band": result["batch_risk_band"],
            "reason_codes": result["batch_reason_codes"],
            "score_delta_vs_previous_day": result["batch_score_delta"],
            "delta_direction": result["batch_delta_direction"],
        },
    }


@app.get("/patients/{patient_id}/vitals")
def patient_vitals(patient_id: str, window: str = "30m"):
    try:
        minutes = int(window.rstrip("m"))
    except ValueError:
        raise HTTPException(status_code=400, detail="window must look like '30m'")

    with get_connection(_dsn()) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT event_id, heart_rate, spo2, systolic_bp, diastolic_bp, temperature,
                       event_ts, trace_id
                FROM speed.vitals_clean
                WHERE patient_id = %s AND event_ts >= now() - (%s || ' minutes')::interval
                ORDER BY event_ts DESC
                LIMIT 500
                """,
                (patient_id, minutes),
            )
            items = [dict(r) for r in cur.fetchall()]
    return {"patient_id": patient_id, "window": window, "items": items}


# --------------------------------------------------------------------------
# Alerts
# --------------------------------------------------------------------------


@app.get("/alerts/active")
def active_alerts():
    with get_connection(_dsn()) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, patient_id, window_start, score, risk_band, components, raised_at
                FROM speed.clinical_alerts
                WHERE NOT acknowledged
                ORDER BY raised_at DESC
                LIMIT 100
                """)
            return [dict(r) for r in cur.fetchall()]


@app.post("/alerts/{alert_id}/ack")
def acknowledge_alert(alert_id: int, body: AckRequest = AckRequest()):
    with get_connection(_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE speed.clinical_alerts SET acknowledged = true, acknowledged_at = now() WHERE id = %s",
                (alert_id,),
            )
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail=f"Unknown alert id {alert_id}")
    return {"id": alert_id, "acknowledged": True}


# --------------------------------------------------------------------------
# Reports
# --------------------------------------------------------------------------


@app.get("/reports/daily/{sim_day}")
def daily_report(sim_day: str):
    reports_dir = os.environ.get("REPORTS_DIR", SETTINGS.reports_dir)
    path = os.path.join(reports_dir, f"daily_risk_report_{sim_day}.html")
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail=f"No report generated yet for sim_day={sim_day}")
    return FileResponse(path, media_type="text/html")


# --------------------------------------------------------------------------
# Alertmanager webhook receiver
# --------------------------------------------------------------------------


@app.post("/internal/alertmanager")
def alertmanager_webhook(payload: AlertmanagerWebhook):
    with get_connection(_dsn()) as conn:
        with conn.cursor() as cur:
            for alert in payload.alerts or [{}]:
                cur.execute(
                    """
                    INSERT INTO ops.alerts_received (source, alert_name, severity, status, labels, annotations, received_at)
                    VALUES (%s,%s,%s,%s,%s,%s, now())
                    """,
                    (
                        "alertmanager",
                        alert.get("labels", {}).get("alertname", "unknown"),
                        alert.get("labels", {}).get("severity"),
                        alert.get("status", payload.status or "firing"),
                        json.dumps(alert.get("labels", {})),
                        json.dumps(alert.get("annotations", {})),
                    ),
                )
    LOGGER.warning("alertmanager_webhook_received", stage="serving", alert_count=len(payload.alerts))
    return {"received": len(payload.alerts)}
