"""
Phase 2 verification helper: confirms Kafka's keying-by-patient_id actually
guarantees per-patient partition affinity (required for correct per-patient
ordering, which the speed layer's trend detection depends on).

Run from the host (against Kafka's external listener on localhost:29092)
after `docker compose up -d vitals-producer lab-producer spark-archiver`:

    pip install -r requirements-dev.txt
    python scripts/check_partitioning.py --n 200

Exit code is non-zero if any patient_id is observed on more than one
partition within the sample.
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict

from confluent_kafka import Consumer

DEFAULT_BOOTSTRAP = "localhost:29092"
DEFAULT_TOPIC = "vitals.raw"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--n", type=int, default=200, help="Number of messages to sample")
    parser.add_argument("--timeout", type=float, default=30.0)
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": "check-partitioning-{}".format(id(object())),
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([args.topic])

    key_to_partitions: dict[str, set[int]] = defaultdict(set)
    consumed = 0

    try:
        import time

        deadline = time.time() + args.timeout
        while consumed < args.n and time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            key = msg.key().decode("utf-8") if msg.key() else None
            if key is not None:
                key_to_partitions[key].add(msg.partition())
            consumed += 1
    finally:
        consumer.close()

    print("Consumed {} messages across {} distinct keys.".format(consumed, len(key_to_partitions)))

    violations = {k: sorted(v) for k, v in key_to_partitions.items() if len(v) > 1}
    if violations:
        print("FAIL: the following patient_id keys appeared on multiple partitions:")
        for key, partitions in violations.items():
            print("  {} -> partitions {}".format(key, partitions))
        return 1

    for key, partitions in sorted(key_to_partitions.items()):
        print("  {} -> partition {}".format(key, next(iter(partitions))))
    print("PASS: every patient_id maps to exactly one partition.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
