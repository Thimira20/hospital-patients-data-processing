"""
Phase 3 verification helper: reconciles one windowed aggregate in
speed.patient_vitals_1m against an independent recomputation straight from
the Parquet master dataset -- proves the speed layer's aggregation logic is
actually correct, not just "running without crashing".

Reads the master dataset directly with pandas/pyarrow (no Spark needed for
this check) and applies the SAME cleaning rules stream_job.py applies
(common.clinical.validate_reading + drop-duplicate-event_id) before
averaging, so the comparison is apples-to-apples.

Run from the host (data/ must be reachable at the given --data-root, which
defaults to ./data as bind-mounted by docker-compose.yml):

    pip install -r requirements-dev.txt
    python scripts/verify_speed_vs_raw.py --patient P001
    # or target one exact window:
    python scripts/verify_speed_vs_raw.py --patient P001 --window-start "2026-02-01 10:00:00+00:00"
"""

from __future__ import annotations

import argparse
import glob
import os
import sys

import pandas as pd
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.clinical import validate_reading  # noqa: E402

DEFAULT_DSN = "postgresql://postgres:postgres@localhost:5432/vitals"
DEFAULT_DATA_ROOT = "data"
TOLERANCE = 0.5


def load_target_window(dsn: str, patient_id: str | None, window_start: str | None) -> dict:
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            if patient_id and window_start:
                cur.execute(
                    """
                    SELECT patient_id, window_start, window_end, avg_hr, reading_count
                    FROM speed.patient_vitals_1m
                    WHERE patient_id = %s AND window_start = %s
                    """,
                    (patient_id, window_start),
                )
            elif patient_id:
                cur.execute(
                    """
                    SELECT patient_id, window_start, window_end, avg_hr, reading_count
                    FROM speed.patient_vitals_1m
                    WHERE patient_id = %s
                    ORDER BY window_start DESC LIMIT 1
                    """,
                    (patient_id,),
                )
            else:
                cur.execute("""
                    SELECT patient_id, window_start, window_end, avg_hr, reading_count
                    FROM speed.patient_vitals_1m
                    ORDER BY window_start DESC LIMIT 1
                    """)
            row = cur.fetchone()
    if row is None:
        raise SystemExit(
            "No matching row found in speed.patient_vitals_1m -- has spark-stream produced output yet?"
        )
    return {
        "patient_id": row[0],
        "window_start": row[1],
        "window_end": row[2],
        "avg_hr": row[3],
        "reading_count": row[4],
    }


def load_raw_master(data_root: str) -> pd.DataFrame:
    pattern = os.path.join(data_root, "master", "vitals", "**", "*.parquet")
    files = glob.glob(pattern, recursive=True)
    if not files:
        raise SystemExit(f"No Parquet files found under {pattern} -- has spark-archiver run yet?")
    frames = [pd.read_parquet(f) for f in files]
    return pd.concat(frames, ignore_index=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=DEFAULT_DSN)
    parser.add_argument("--data-root", default=DEFAULT_DATA_ROOT)
    parser.add_argument("--patient", default=None)
    parser.add_argument("--window-start", default=None)
    args = parser.parse_args()

    target = load_target_window(args.dsn, args.patient, args.window_start)
    print(
        "Target window: patient={patient_id} window=[{window_start}, {window_end}) "
        "avg_hr={avg_hr} reading_count={reading_count}".format(**target)
    )

    raw = load_raw_master(args.data_root)
    raw = raw[raw["event_id"].notna()]  # drop unparsed/malformed rows (they carry no event_id)
    raw["event_ts_dt"] = pd.to_datetime(raw["event_ts"], utc=True, errors="coerce")

    window_start = (
        pd.Timestamp(target["window_start"]).tz_convert("UTC")
        if pd.Timestamp(target["window_start"]).tzinfo
        else pd.Timestamp(target["window_start"], tz="UTC")
    )
    window_end = (
        pd.Timestamp(target["window_end"]).tz_convert("UTC")
        if pd.Timestamp(target["window_end"]).tzinfo
        else pd.Timestamp(target["window_end"], tz="UTC")
    )

    in_window = raw[
        (raw["patient_id"] == target["patient_id"])
        & (raw["event_ts_dt"] >= window_start)
        & (raw["event_ts_dt"] < window_end)
    ].copy()

    def is_row_valid(r) -> bool:
        ok, _ = validate_reading(
            r["heart_rate"], r["spo2"], r["systolic_bp"], r["diastolic_bp"], r["temperature"]
        )
        return ok

    in_window["valid"] = in_window.apply(is_row_valid, axis=1)
    clean = in_window[in_window["valid"]].drop_duplicates(subset=["event_id"])

    if clean.empty:
        print("FAIL: no clean raw rows found in this window -- cannot reconcile.")
        return 1

    recomputed_avg_hr = clean["heart_rate"].mean()
    diff = abs(recomputed_avg_hr - target["avg_hr"]) if target["avg_hr"] is not None else float("inf")

    print(f"Recomputed from raw Parquet: avg_hr={recomputed_avg_hr:.3f} over {len(clean)} clean rows")
    print(f"Difference: {diff:.4f} (tolerance: {TOLERANCE})")

    if diff <= TOLERANCE:
        print("PASS: speed layer's windowed average matches independent recomputation from raw data.")
        return 0
    print("FAIL: speed layer's windowed average does NOT match the raw recomputation.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
