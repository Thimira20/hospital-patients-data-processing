"""
Phase 3 verification helper: computes precision/recall of speed.clinical_alerts
against the injected ground truth (data/ground_truth.jsonl) written by
ingestion/vitals_producer.py -- this is the "prove the alerts fire on the
injected anomalies" check described in PLAN.md Phase 3.2.

An alert counts as a true positive if it was raised for the same patient
while one of that patient's injected anomaly episodes was active (with a
grace window after the episode ends, since the sustained-alert rule needs
>= 2 consecutive 30s-apart windows of data to accumulate after the episode
starts, and the windowed aggregate lags the raw event stream slightly).

Run from the host once the pipeline has been running for a few minutes:

    pip install -r requirements-dev.txt
    python scripts/verify_alerts.py
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta, timezone

import psycopg2

DEFAULT_DSN = "postgresql://postgres:postgres@localhost:5432/vitals"
DEFAULT_GROUND_TRUTH = os.path.join("data", "ground_truth.jsonl")

# How long after an episode officially ends an alert can still legitimately
# fire and count as a true positive -- accounts for window-aggregation lag
# (trigger interval + the 2-window sustained-breach requirement).
GRACE_SECONDS = 120


def parse_iso(ts: str) -> datetime:
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def load_episodes(path: str) -> list[dict]:
    """Pair episode_start/episode_end records per patient into
    {patient_id, scenario, start, end} intervals. An episode without a
    matching end (still active when the log was read) gets end=now."""
    open_episodes: dict[str, dict] = {}
    episodes: list[dict] = []

    if not os.path.exists(path):
        return episodes

    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            key = record["patient_id"]
            if record["event"] == "episode_start":
                open_episodes[key] = {
                    "patient_id": key,
                    "scenario": record["scenario"],
                    "start": parse_iso(record["ts"]),
                    "end": None,
                }
            elif record["event"] == "episode_end" and key in open_episodes:
                open_episodes[key]["end"] = parse_iso(record["ts"])
                episodes.append(open_episodes.pop(key))

    now = datetime.now(timezone.utc)
    for leftover in open_episodes.values():
        leftover["end"] = now
        episodes.append(leftover)

    return episodes


def load_alerts(dsn: str) -> list[dict]:
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT patient_id, raised_at, score, risk_band FROM speed.clinical_alerts")
            return [
                {"patient_id": r[0], "raised_at": r[1], "score": r[2], "band": r[3]} for r in cur.fetchall()
            ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=DEFAULT_DSN)
    parser.add_argument("--ground-truth", default=DEFAULT_GROUND_TRUTH)
    args = parser.parse_args()

    episodes = load_episodes(args.ground_truth)
    alerts = load_alerts(args.dsn)

    print(f"Loaded {len(episodes)} injected anomaly episodes and {len(alerts)} raised alerts.\n")

    if not episodes:
        print("WARNING: no episodes found in ground truth -- has the producer run long enough?")
        return 1

    true_positive_alerts = 0
    for alert in alerts:
        raised_at = alert["raised_at"]
        if raised_at.tzinfo is None:
            raised_at = raised_at.replace(tzinfo=timezone.utc)
        matched = any(
            ep["patient_id"] == alert["patient_id"]
            and ep["start"] <= raised_at <= ep["end"] + timedelta(seconds=GRACE_SECONDS)
            for ep in episodes
        )
        if matched:
            true_positive_alerts += 1

    recalled_episodes = 0
    for ep in episodes:
        matched = any(
            a["patient_id"] == ep["patient_id"]
            and ep["start"]
            <= (a["raised_at"] if a["raised_at"].tzinfo else a["raised_at"].replace(tzinfo=timezone.utc))
            <= ep["end"] + timedelta(seconds=GRACE_SECONDS)
            for a in alerts
        )
        if matched:
            recalled_episodes += 1

    precision = true_positive_alerts / len(alerts) if alerts else float("nan")
    recall = recalled_episodes / len(episodes) if episodes else float("nan")

    print(
        f"Precision: {true_positive_alerts}/{len(alerts)} alerts matched an injected episode = {precision:.2%}"
    )
    print(f"Recall:    {recalled_episodes}/{len(episodes)} episodes had >=1 matching alert = {recall:.2%}")
    print(
        "\n(Not every episode is expected to alert -- only sustained, high-band ones; "
        "brief/mild scenarios like a single tachycardia episode may legitimately score 'medium'.)"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
