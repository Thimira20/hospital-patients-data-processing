"""
Phase 2 verification helper: samples N messages from `vitals.raw` and reports
what fraction are malformed / null-field / out-of-range / back-dated, so you
can confirm the dirty-data injection rates configured in .env are actually
showing up on the wire before trusting Phase 3's cleaning logic against them.

Run from the host:

    pip install -r requirements-dev.txt
    python scripts/sample_topic.py --n 2000
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime

from confluent_kafka import Consumer

DEFAULT_BOOTSTRAP = "localhost:29092"
DEFAULT_TOPIC = "vitals.raw"

VALID_RANGES = {
    "heart_rate": (20, 250),
    "spo2": (50, 100),
    "systolic_bp": (40, 260),
    "diastolic_bp": (20, 180),
    "temperature": (30.0, 43.0),
}


def classify(raw_bytes: bytes) -> dict:
    result = {"malformed": False, "null_field": False, "out_of_range": False, "late": False}
    try:
        event = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        result["malformed"] = True
        return result

    for field in VALID_RANGES:
        if event.get(field) is None:
            result["null_field"] = True

    for field, (lo, hi) in VALID_RANGES.items():
        value = event.get(field)
        if value is not None and not (lo <= value <= hi):
            result["out_of_range"] = True

    event_ts = event.get("event_ts")
    producer_ts = event.get("producer_ts")
    if event_ts and producer_ts:
        try:
            evt = datetime.fromisoformat(event_ts)
            prod = datetime.fromisoformat(producer_ts)
            if (prod - evt).total_seconds() > 3:
                result["late"] = True
        except ValueError:
            pass

    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--n", type=int, default=2000)
    parser.add_argument("--timeout", type=float, default=60.0)
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": "sample-topic-{}".format(id(object())),
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([args.topic])

    counts = {"total": 0, "malformed": 0, "null_field": 0, "out_of_range": 0, "late": 0}
    deadline = time.time() + args.timeout

    try:
        while counts["total"] < args.n and time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            result = classify(msg.value())
            counts["total"] += 1
            for k in ("malformed", "null_field", "out_of_range", "late"):
                if result[k]:
                    counts[k] += 1
    finally:
        consumer.close()

    total = max(1, counts["total"])
    print("Sampled {} messages from {}:".format(counts["total"], args.topic))
    for k in ("malformed", "null_field", "out_of_range", "late"):
        pct = 100.0 * counts[k] / total
        print("  {:<12} {:5d}  ({:.2f}%)".format(k, counts[k], pct))

    if counts["total"] == 0:
        print("WARNING: consumed 0 messages -- is the producer running?")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
