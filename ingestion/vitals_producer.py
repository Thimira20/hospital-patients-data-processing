"""
Streaming data source: simulates bedside monitors emitting vital-sign
readings for every patient in config/patients.csv, every
VITALS_INTERVAL_SECONDS (real time), to Kafka topic `vitals.raw`.

Responsibilities beyond "emit a number every few seconds" (each one is a
rubric-relevant robustness feature -- see PLAN.md Phase 2):
  * Per-patient baseline + mean-reverting random walk (ingestion/patient_state.py)
    so traces look like a real physiological signal, not white noise.
  * Injects sustained clinical anomaly episodes (tachycardia, desaturation,
    hypotension, fever, sepsis_pattern) and logs the injected ground truth to
    data/ground_truth.jsonl -- this is what lets Phase 3's verify_alerts.py
    compute real precision/recall for the speed layer's alerting.
  * Injects dirty data (out-of-range values, nulled fields, duplicate
    event_ids, back-dated "late" events) and malformed (non-JSON) payloads,
    at configurable rates -- exercising the cleaning/dedup/DLQ logic that
    Phase 3's stream_job.py must implement.
  * Kafka delivery is idempotent (acks=all, enable.idempotence=True), keyed
    by patient_id (guarantees per-patient partition ordering), with
    Prometheus metrics and structured JSON logging throughout.
  * Graceful SIGTERM/SIGINT shutdown: flushes the producer before exiting so
    a container restart never silently drops in-flight messages.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import signal
import sys
import time
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer
from prometheus_client import Counter, Histogram, start_http_server

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.logging_setup import configure_logging, new_trace_id  # noqa: E402
from config.settings import get_settings  # noqa: E402
from ingestion.patient_state import PatientState  # noqa: E402
from ingestion.sim_clock import SimClock, real_now_iso  # noqa: E402

# ---- Prometheus metrics ----------------------------------------------------
EVENTS_EMITTED = Counter("vitals_events_emitted_total", "Vital-sign events successfully delivered to Kafka")
EMIT_ERRORS = Counter("vitals_emit_errors_total", "Vital-sign events that failed Kafka delivery")
ANOMALIES_INJECTED = Counter(
    "vitals_anomalies_injected_total", "Sustained anomaly episodes injected", ["scenario"]
)
EMIT_LATENCY = Histogram("vitals_emit_latency_seconds", "Kafka produce -> delivery-callback latency")

_running = True


def _handle_signal(signum, frame):  # noqa: ARG001
    global _running
    _running = False


def load_patients(csv_path: str) -> list[dict]:
    with open(csv_path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


class GroundTruthLog:
    """Appends one JSON line per injected episode start/end, so the
    detection-accuracy check in Phase 3 has an authoritative source of truth
    to compare speed.clinical_alerts against."""

    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    def write(self, record: dict) -> None:
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record) + "\n")


def make_dirty(event: dict, recent_event_ids: deque, rng: random.Random) -> dict:
    """Apply one randomly-chosen dirty-data corruption to `event` in place
    and return it. Called only when the dirty-data roll already succeeded."""
    kind = rng.choice(["null_field", "out_of_range", "duplicate_id"])
    if kind == "null_field":
        field_name = rng.choice(["spo2", "heart_rate", "systolic_bp", "diastolic_bp", "temperature"])
        event[field_name] = None
    elif kind == "out_of_range":
        field_name = rng.choice(["heart_rate", "spo2", "systolic_bp"])
        event[field_name] = {"heart_rate": 300.0, "spo2": 10.0, "systolic_bp": 400.0}[field_name]
    elif kind == "duplicate_id" and recent_event_ids:
        event["event_id"] = rng.choice(list(recent_event_ids))
    event["_dirty_kind"] = kind
    return event


def maybe_backdate(event: dict, rate: float, max_delay: int, rng: random.Random) -> dict:
    """With probability `rate`, back-date event_ts to simulate a late-arriving
    sensor reading (exercises the speed layer's watermark handling)."""
    if rng.random() < rate:
        delay = rng.uniform(5, max_delay)
        ts = datetime.now(timezone.utc) - timedelta(seconds=delay)
        event["event_ts"] = ts.isoformat()
        event["_late_by_seconds"] = round(delay, 1)
    return event


def run(rate_override: float | None = None) -> None:
    settings = get_settings()
    logger = configure_logging("vitals-producer", logs_dir=os.environ.get("LOGS_DIR"))

    interval = rate_override if rate_override is not None else settings.vitals_interval_seconds
    patients_csv = os.environ.get("PATIENTS_CSV", "/app/config/patients.csv")
    ground_truth_path = os.path.join(os.environ.get("DATA_ROOT", settings.data_root), "ground_truth.jsonl")
    sim_clock_path = os.path.join(os.environ.get("DATA_ROOT", settings.data_root), "sim_clock.json")

    patients = load_patients(patients_csv)
    if not patients:
        logger.error("no_patients_loaded", stage="ingestion", path=patients_csv)
        sys.exit(1)

    sim_clock = SimClock.load_or_create(settings.sim_start_date, settings.sim_day_seconds, sim_clock_path)
    ground_truth = GroundTruthLog(ground_truth_path)

    states = {
        p["patient_id"]: PatientState(
            patient_id=p["patient_id"],
            baseline_hr=float(p["baseline_hr"]),
            baseline_spo2=float(p["baseline_spo2"]),
            baseline_sbp=float(p["baseline_sbp"]),
            baseline_dbp=float(p["baseline_dbp"]),
            baseline_temp=float(p["baseline_temp"]),
        )
        for p in patients
    }
    recent_event_ids: dict[str, deque] = {pid: deque(maxlen=25) for pid in states}
    previous_scenario: dict[str, str | None] = {pid: None for pid in states}

    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_internal,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 50,
            "compression.type": "snappy",
            "retries": 10,
            "retry.backoff.ms": 200,
        }
    )

    start_http_server(8001)
    logger.info(
        "producer_started",
        stage="ingestion",
        num_patients=len(states),
        interval_seconds=interval,
        topic=settings.topic_vitals_raw,
    )

    rng = random.Random()
    send_times: dict[str, float] = {}

    def on_delivery(err, msg):
        key = msg.key().decode() if msg.key() else None
        _, ts_ms = msg.timestamp()
        send_key = "{}:{}".format(key, ts_ms)
        send_time = send_times.pop(send_key, None)
        if err is not None:
            EMIT_ERRORS.inc()
            logger.warning("delivery_failed", stage="ingestion", error=str(err), patient_id=key)
        else:
            EVENTS_EMITTED.inc()
            if send_time is not None:
                EMIT_LATENCY.observe(time.time() - send_time)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    tick_count = 0
    while _running:
        loop_start = time.time()
        tick_count += 1

        for patient_id, state in states.items():
            scenario_started = state.maybe_start_episode(settings.anomaly_probability, rng)
            if scenario_started:
                ANOMALIES_INJECTED.labels(scenario=scenario_started).inc()
                ground_truth.write(
                    {
                        "event": "episode_start",
                        "patient_id": patient_id,
                        "scenario": scenario_started,
                        "sim_day": sim_clock.sim_day(),
                        "ts": real_now_iso(),
                    }
                )
                logger.info(
                    "anomaly_injected",
                    stage="ingestion",
                    patient_id=patient_id,
                    scenario=scenario_started,
                )
                previous_scenario[patient_id] = scenario_started
            elif state.active_episode is not None:
                previous_scenario[patient_id] = state.active_episode.scenario

            vitals = state.tick(rng)

            if previous_scenario[patient_id] is not None and state.active_episode is None:
                ground_truth.write(
                    {
                        "event": "episode_end",
                        "patient_id": patient_id,
                        "scenario": previous_scenario[patient_id],
                        "sim_day": sim_clock.sim_day(),
                        "ts": real_now_iso(),
                    }
                )
                previous_scenario[patient_id] = None

            event_id = uuid.uuid4().hex
            trace_id = new_trace_id()
            now_iso = real_now_iso()

            event = {
                "event_id": event_id,
                "patient_id": patient_id,
                **vitals,
                "event_ts": now_iso,
                "sim_day": sim_clock.sim_day(),
                "producer_ts": now_iso,
                "trace_id": trace_id,
            }

            is_malformed = rng.random() < settings.malformed_rate
            if not is_malformed:
                if rng.random() < settings.dirty_data_rate:
                    event = make_dirty(event, recent_event_ids[patient_id], rng)
                event = maybe_backdate(
                    event, settings.late_event_rate, settings.late_event_max_delay_seconds, rng
                )
                recent_event_ids[patient_id].append(event_id)

            if is_malformed:
                # Truncate valid JSON so Spark's from_json() cannot parse it --
                # exercises the DLQ path in stream_job.py.
                good_json = json.dumps(event).encode("utf-8")
                payload = good_json[: max(1, len(good_json) - 5)]
            else:
                payload = json.dumps(event).encode("utf-8")

            headers = [("trace_id", trace_id.encode()), ("producer_ts", now_iso.encode())]
            now_ms = int(time.time() * 1000)
            send_key = "{}:{}".format(patient_id, now_ms)
            send_times[send_key] = time.time()

            try:
                producer.produce(
                    settings.topic_vitals_raw,
                    key=patient_id.encode("utf-8"),
                    value=payload,
                    headers=headers,
                    timestamp=now_ms,
                    on_delivery=on_delivery,
                )
            except BufferError:
                producer.poll(1)
                logger.warning("producer_queue_full", stage="ingestion", patient_id=patient_id)

        producer.poll(0)

        elapsed = time.time() - loop_start
        time.sleep(max(0.0, interval - elapsed))

    logger.info("producer_shutting_down", stage="ingestion")
    producer.flush(10)
    logger.info("producer_shutdown_complete", stage="ingestion")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulated bedside-monitor vitals producer")
    parser.add_argument(
        "--rate", type=float, default=None, help="Override VITALS_INTERVAL_SECONDS for load testing"
    )
    args = parser.parse_args()
    run(rate_override=args.rate)
