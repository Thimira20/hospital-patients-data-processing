"""
Phase 3 verification helper: measures end-to-end pipeline latency from
producer send time (producer_ts) to the row landing in speed.vitals_clean
(inserted_at), and reports percentiles -- PLAN.md Phase 3.2 asks for this
number to be recorded in the report.

Run from the host:

    pip install -r requirements-dev.txt
    python scripts/measure_latency.py
"""

from __future__ import annotations

import argparse

import psycopg2

DEFAULT_DSN = "postgresql://postgres:postgres@localhost:5432/vitals"


def percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return float("nan")
    idx = min(len(sorted_values) - 1, int(round(pct / 100 * (len(sorted_values) - 1))))
    return sorted_values[idx]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=DEFAULT_DSN)
    parser.add_argument("--limit", type=int, default=5000, help="Most recent rows to sample")
    args = parser.parse_args()

    with psycopg2.connect(args.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXTRACT(EPOCH FROM (inserted_at - producer_ts))
                FROM speed.vitals_clean
                WHERE producer_ts IS NOT NULL
                ORDER BY inserted_at DESC
                LIMIT %s
                """,
                (args.limit,),
            )
            latencies = sorted(float(row[0]) for row in cur.fetchall() if row[0] is not None)

    if not latencies:
        print("No rows with producer_ts found yet -- is spark-stream running and has it processed a batch?")
        return 1

    print(f"Sampled {len(latencies)} rows from speed.vitals_clean.")
    print(f"  min    : {min(latencies):.2f}s")
    print(f"  p50    : {percentile(latencies, 50):.2f}s")
    print(f"  p95    : {percentile(latencies, 95):.2f}s")
    print(f"  p99    : {percentile(latencies, 99):.2f}s")
    print(f"  max    : {max(latencies):.2f}s")
    print(f"  mean   : {sum(latencies) / len(latencies):.2f}s")
    print(
        "\nExpected ballpark: p95 well under the trigger interval + a couple of "
        "seconds of network/processing overhead (see STREAM_TRIGGER_SECONDS in .env). "
        "Record the p95 figure in the report's Results section."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
