"""
Daily-batch data source: writes one lab-results CSV file per simulated day
to data/landing/labs/, representing the pathology lab's once-a-day extract.

Responsibilities (each maps to a rubric-relevant robustness feature -- see
PLAN.md Phase 2):
  * Atomic file writes: write to a `.tmp` path then os.replace() into place,
    so Airflow's FileSensor (Phase 4) never reads a half-written file. The
    sensor actually watches a `.done` marker written *last*, after the CSV
    and its manifest are both safely on disk.
  * A `.manifest.json` sidecar (row count + checksum) that Airflow's
    validate_lab_file task uses to confirm nothing was truncated in transit.
  * Correlates lab abnormalities with whatever anomaly episodes the vitals
    producer injected for that simulated day (read from
    data/ground_truth.jsonl) -- a patient with a sepsis_pattern episode gets
    elevated WBC/CRP/Lactate/Troponin that day. This is what makes the batch
    layer's vitals<->labs join (Phase 4) produce a genuine finding instead
    of noise.
  * Deliberately injects data-quality problems the batch job's DQ rules
    (Phase 4) must catch: one missing patient, one duplicate row, one
    out-of-schema value per file.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import random
import signal
import sys
import time

from prometheus_client import Counter, Gauge, start_http_server

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.logging_setup import configure_logging  # noqa: E402
from config.settings import get_settings  # noqa: E402
from ingestion.sim_clock import SimClock  # noqa: E402

# ---- Prometheus metrics ----------------------------------------------------
# Exposed on :8002 -- referenced by observability/prometheus.yml's
# "lab-producer" scrape job and by the LabFileMissing alert rule.
LAB_FILES_WRITTEN = Counter("lab_file_written_total", "Daily lab CSV files successfully written")
LAB_FILE_ROWS = Gauge("lab_file_rows", "Row count of the most recently written lab file")
LAB_FILE_LAST_WRITTEN_TS = Gauge(
    "lab_file_last_written_timestamp_seconds", "Unix timestamp of the most recent lab file write"
)

# Normal / abnormal (sepsis-elevated) ranges per test. Kept hardcoded here
# (mirroring config/thresholds.yml's lab_reference_defaults) so this module
# has no YAML-parsing dependency and stays trivially testable.
LAB_TESTS = {
    "WBC": {
        "unit": "10^9/L",
        "ref_low": 4.0,
        "ref_high": 11.0,
        "normal": (3.5, 11.5),
        "elevated": (14.0, 24.0),
    },
    "CRP": {
        "unit": "mg/L",
        "ref_low": 0.0,
        "ref_high": 10.0,
        "normal": (0.0, 9.0),
        "elevated": (50.0, 200.0),
    },
    "Lactate": {
        "unit": "mmol/L",
        "ref_low": 0.5,
        "ref_high": 2.0,
        "normal": (0.4, 2.0),
        "elevated": (3.0, 9.0),
    },
    "Creatinine": {
        "unit": "umol/L",
        "ref_low": 60.0,
        "ref_high": 110.0,
        "normal": (55.0, 110.0),
        "elevated": (140.0, 250.0),
    },
    "Hemoglobin": {
        "unit": "g/L",
        "ref_low": 120.0,
        "ref_high": 170.0,
        "normal": (115.0, 170.0),
        "elevated": (80.0, 105.0),
    },
    "Platelets": {
        "unit": "10^9/L",
        "ref_low": 150.0,
        "ref_high": 400.0,
        "normal": (150.0, 400.0),
        "elevated": (60.0, 130.0),
    },
    "Troponin": {
        "unit": "ng/mL",
        "ref_low": 0.0,
        "ref_high": 0.04,
        "normal": (0.0, 0.03),
        "elevated": (0.08, 0.6),
    },
}

ROUTINE_PANEL = ["WBC", "CRP", "Lactate"]
SEPSIS_ELEVATED_TESTS = ["WBC", "CRP", "Lactate", "Troponin"]
FEVER_ELEVATED_TESTS = ["WBC", "CRP"]
HYPOTENSION_ELEVATED_TESTS = ["Lactate", "Troponin"]

_running = True


def _handle_signal(signum, frame):  # noqa: ARG001
    global _running
    _running = False


def load_patient_ids(csv_path: str) -> list[str]:
    with open(csv_path, newline="", encoding="utf-8") as fh:
        return [row["patient_id"] for row in csv.DictReader(fh)]


def read_episode_scenarios_for_day(ground_truth_path: str, sim_day: str) -> dict[str, set[str]]:
    """Returns {patient_id: {scenario, ...}} for every episode_start logged
    on `sim_day`, so lab values can be correlated with vitals anomalies."""
    result: dict[str, set[str]] = {}
    if not os.path.exists(ground_truth_path):
        return result
    with open(ground_truth_path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if record.get("event") == "episode_start" and record.get("sim_day") == sim_day:
                result.setdefault(record["patient_id"], set()).add(record["scenario"])
    return result


def pick_tests_for_patient(scenarios: set[str], rng: random.Random) -> list[str]:
    tests = set(ROUTINE_PANEL)
    extra_pool = [t for t in LAB_TESTS if t not in tests]
    tests.update(rng.sample(extra_pool, k=rng.randint(0, len(extra_pool))))
    return sorted(tests)


def elevated_tests_for(scenarios: set[str]) -> set[str]:
    elevated: set[str] = set()
    if "sepsis_pattern" in scenarios:
        elevated.update(SEPSIS_ELEVATED_TESTS)
    if "fever" in scenarios:
        elevated.update(FEVER_ELEVATED_TESTS)
    if "hypotension" in scenarios:
        elevated.update(HYPOTENSION_ELEVATED_TESTS)
    return elevated


def sample_value(test_type: str, elevated: bool, rng: random.Random) -> float:
    spec = LAB_TESTS[test_type]
    low, high = spec["elevated"] if elevated else spec["normal"]
    return round(rng.uniform(low, high), 2)


def generate_rows(
    patient_ids: list[str],
    sim_day: str,
    scenarios_by_patient: dict[str, set[str]],
    rng: random.Random,
) -> list[dict]:
    rows: list[dict] = []
    for patient_id in patient_ids:
        scenarios = scenarios_by_patient.get(patient_id, set())
        elevated = elevated_tests_for(scenarios)
        for test_type in pick_tests_for_patient(scenarios, rng):
            spec = LAB_TESTS[test_type]
            value = sample_value(test_type, test_type in elevated, rng)
            rows.append(
                {
                    "patient_id": patient_id,
                    "test_type": test_type,
                    "result_value": value,
                    "unit": spec["unit"],
                    "reference_low": spec["ref_low"],
                    "reference_high": spec["ref_high"],
                    "collected_at": sim_day + "T06:00:00+00:00",
                    "lab_batch_id": "{}-{}".format(sim_day, "LAB"),
                    "sim_day": sim_day,
                }
            )
    return rows


def inject_data_quality_issues(rows: list[dict], patient_ids: list[str], rng: random.Random) -> list[dict]:
    """Deliberately degrade the row list: drop one patient entirely,
    duplicate one row, and corrupt one value -- exercising the batch job's
    data-quality gate (Phase 4)."""
    if not rows:
        return rows

    # 1. Missing patient: drop all rows for one randomly chosen patient.
    if len(patient_ids) > 1:
        dropped_patient = rng.choice(patient_ids)
        rows = [r for r in rows if r["patient_id"] != dropped_patient]

    # 2. Duplicate row: re-append a copy of a random existing row.
    if rows:
        rows.append(dict(rng.choice(rows)))

    # 3. Out-of-schema value: corrupt one row's result_value into free text.
    if rows:
        target = rng.choice(rows)
        target["result_value"] = "ERROR"

    return rows


def write_atomic_csv(rows: list[dict], final_path: str) -> tuple[int, str]:
    tmp_path = final_path + ".tmp"
    fieldnames = [
        "patient_id",
        "test_type",
        "result_value",
        "unit",
        "reference_low",
        "reference_high",
        "collected_at",
        "lab_batch_id",
        "sim_day",
    ]
    with open(tmp_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    with open(tmp_path, "rb") as fh:
        content = fh.read()
    checksum = hashlib.md5(content).hexdigest()

    os.replace(tmp_path, final_path)
    return len(rows), checksum


def generate_lab_file(
    sim_day: str,
    patient_ids: list[str],
    ground_truth_path: str,
    landing_dir: str,
    logger,
) -> None:
    rng = random.Random()
    scenarios_by_patient = read_episode_scenarios_for_day(ground_truth_path, sim_day)

    rows = generate_rows(patient_ids, sim_day, scenarios_by_patient, rng)
    rows = inject_data_quality_issues(rows, patient_ids, rng)

    os.makedirs(landing_dir, exist_ok=True)
    final_csv = os.path.join(landing_dir, "labs_{}.csv".format(sim_day))
    row_count, checksum = write_atomic_csv(rows, final_csv)

    manifest_path = os.path.join(landing_dir, "labs_{}.manifest.json".format(sim_day))
    manifest = {
        "sim_day": sim_day,
        "row_count": row_count,
        "checksum_md5": checksum,
        "patients_with_correlated_anomalies": {pid: sorted(s) for pid, s in scenarios_by_patient.items()},
    }
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)

    # .done marker written LAST -- this is what Airflow's FileSensor watches.
    done_path = final_csv + ".done"
    with open(done_path, "w", encoding="utf-8") as fh:
        fh.write(json.dumps({"row_count": row_count, "checksum_md5": checksum}))

    LAB_FILES_WRITTEN.inc()
    LAB_FILE_ROWS.set(row_count)
    LAB_FILE_LAST_WRITTEN_TS.set(time.time())

    logger.info(
        "lab_file_written",
        stage="ingestion",
        sim_day=sim_day,
        row_count=row_count,
        anomalous_patients=len(scenarios_by_patient),
    )


def run() -> None:
    settings = get_settings()
    logger = configure_logging("lab-producer", logs_dir=os.environ.get("LOGS_DIR"))

    data_root = os.environ.get("DATA_ROOT", settings.data_root)
    patients_csv = os.environ.get("PATIENTS_CSV", "/app/config/patients.csv")
    landing_dir = os.environ.get("LANDING_LABS_DIR", settings.landing_labs_dir)
    ground_truth_path = os.path.join(data_root, "ground_truth.jsonl")
    sim_clock_path = os.path.join(data_root, "sim_clock.json")

    patient_ids = load_patient_ids(patients_csv)
    sim_clock = SimClock.load_or_create(settings.sim_start_date, settings.sim_day_seconds, sim_clock_path)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    start_http_server(8002)
    logger.info("lab_producer_started", stage="ingestion", landing_dir=landing_dir)

    last_generated_index = -1
    while _running:
        current_index = sim_clock.sim_day_index()
        if current_index > last_generated_index:
            completed_day = sim_clock.sim_day_for_index(last_generated_index + 1)
            generate_lab_file(completed_day, patient_ids, ground_truth_path, landing_dir, logger)
            last_generated_index += 1
            continue  # check immediately in case multiple days elapsed (e.g. after a restart)

        sleep_for = min(5.0, sim_clock.seconds_until_next_day_boundary() + 0.5)
        time.sleep(max(0.5, sleep_for))

    logger.info("lab_producer_shutdown", stage="ingestion")


if __name__ == "__main__":
    run()
