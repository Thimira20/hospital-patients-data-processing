"""
Unit tests for the pure-Python ingestion logic (no Kafka/Docker required).
Run with: pytest tests/test_ingestion.py -v
"""

from __future__ import annotations

import os
import random
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.lab_batch_producer import (  # noqa: E402
    elevated_tests_for,
    generate_rows,
    inject_data_quality_issues,
    read_episode_scenarios_for_day,
)
from ingestion.patient_state import PatientState  # noqa: E402
from ingestion.sim_clock import SimClock  # noqa: E402

# --------------------------------------------------------------------------
# PatientState / anomaly injection
# --------------------------------------------------------------------------


def make_patient(**overrides) -> PatientState:
    defaults = dict(
        patient_id="P001",
        baseline_hr=78.0,
        baseline_spo2=97.0,
        baseline_sbp=120.0,
        baseline_dbp=78.0,
        baseline_temp=36.7,
    )
    defaults.update(overrides)
    return PatientState(**defaults)


def test_tick_stays_within_physiological_clamp_bounds():
    p = make_patient()
    rng = random.Random(0)
    for _ in range(500):
        reading = p.tick(rng)
        assert 20.0 <= reading["heart_rate"] <= 260.0
        assert 50.0 <= reading["spo2"] <= 100.0
        assert 40.0 <= reading["systolic_bp"] <= 260.0
        assert 20.0 <= reading["diastolic_bp"] <= 180.0
        assert 30.0 <= reading["temperature"] <= 43.0


def test_anomaly_episode_moves_vitals_and_then_ends():
    p = make_patient()
    rng = random.Random(1)
    scenario = p.maybe_start_episode(1.0, rng)  # force start
    assert scenario in {"tachycardia", "desaturation", "hypotension", "fever", "sepsis_pattern"}
    assert p.active_episode is not None

    baseline_hr = p.baseline_hr
    readings = [p.tick(rng) for _ in range(120)]  # longer than max episode duration (90)

    assert p.active_episode is None, "episode should have ended by tick 120"

    if scenario == "tachycardia":
        mid_episode_hr = readings[10]["heart_rate"]
        assert mid_episode_hr > baseline_hr + 15, "tachycardia should meaningfully raise HR"


def test_no_new_episode_while_one_is_active():
    p = make_patient()
    rng = random.Random(2)
    first = p.maybe_start_episode(1.0, rng)
    assert first is not None
    second = p.maybe_start_episode(1.0, rng)  # would also force-start if allowed
    assert second is None, "should not stack a second episode on top of an active one"


def test_zero_probability_never_starts_an_episode():
    p = make_patient()
    rng = random.Random(3)
    for _ in range(200):
        assert p.maybe_start_episode(0.0, rng) is None


# --------------------------------------------------------------------------
# SimClock
# --------------------------------------------------------------------------


def test_sim_clock_day_progression_and_persistence():
    state_path = os.path.join(tempfile.mkdtemp(), "sim_clock.json")
    c1 = SimClock.load_or_create("2026-01-01", sim_day_seconds=10, state_path=state_path)
    assert c1.sim_day() == "2026-01-01"

    # A second process loading the same state file must get the SAME origin
    # (this is what makes simulated time survive a container restart).
    c2 = SimClock.load_or_create("2026-01-01", sim_day_seconds=10, state_path=state_path)
    assert c1.origin_epoch == c2.origin_epoch


def test_sim_clock_previous_day_and_boundary():
    c = SimClock(
        sim_start_date=__import__("datetime").date(2026, 1, 1),
        sim_day_seconds=10,
        origin_epoch=__import__("time").time() - 25,
    )
    # 25s elapsed / 10s per day -> day index 2 ("2026-01-03")
    assert c.sim_day_index() == 2
    assert c.sim_day() == "2026-01-03"
    assert c.previous_sim_day() == "2026-01-02"
    assert 0 < c.seconds_until_next_day_boundary() <= 10


# --------------------------------------------------------------------------
# Lab batch producer
# --------------------------------------------------------------------------


def test_sepsis_episode_elevates_correlated_labs():
    scenarios_by_patient = {"P001": {"sepsis_pattern"}}
    rows = generate_rows(["P001", "P002"], "2026-01-01", scenarios_by_patient, random.Random(5))

    p1_wbc = [r for r in rows if r["patient_id"] == "P001" and r["test_type"] == "WBC"]
    assert p1_wbc, "routine panel should always include WBC"
    assert (
        p1_wbc[0]["result_value"] > 11.0
    ), "sepsis patient's WBC should be elevated above the normal reference range"


def test_non_anomalous_patient_gets_normal_labs():
    scenarios_by_patient: dict = {}
    rows = generate_rows(["P002"], "2026-01-01", scenarios_by_patient, random.Random(6))
    crp = [r for r in rows if r["test_type"] == "CRP"][0]
    assert crp["result_value"] <= 10.0, "non-anomalous patient's CRP should stay within the normal range"


def test_data_quality_injection_drops_duplicates_and_corrupts_one_value():
    scenarios_by_patient: dict = {}
    patient_ids = ["P001", "P002", "P003"]
    rows = generate_rows(patient_ids, "2026-01-01", scenarios_by_patient, random.Random(7))
    original_count = len(rows)

    corrupted = inject_data_quality_issues(list(rows), patient_ids, random.Random(8))

    remaining_patients = {r["patient_id"] for r in corrupted}
    assert len(remaining_patients) < len(patient_ids), "one patient's rows should be entirely dropped"

    error_rows = [r for r in corrupted if r["result_value"] == "ERROR"]
    assert len(error_rows) == 1, "exactly one row should be corrupted into an out-of-schema value"

    # duplicate injection appends one more row than would otherwise remain
    assert len(corrupted) != original_count


def test_elevated_tests_for_maps_scenarios_correctly():
    assert "Lactate" in elevated_tests_for({"sepsis_pattern"})
    assert "Troponin" in elevated_tests_for({"hypotension"})
    assert elevated_tests_for({"tachycardia"}) == set()


def test_read_episode_scenarios_filters_by_sim_day():
    tmp = tempfile.mkdtemp()
    gt_path = os.path.join(tmp, "ground_truth.jsonl")
    import json

    with open(gt_path, "w", encoding="utf-8") as fh:
        fh.write(
            json.dumps(
                {"event": "episode_start", "patient_id": "P001", "scenario": "fever", "sim_day": "2026-01-01"}
            )
            + "\n"
        )
        fh.write(
            json.dumps(
                {
                    "event": "episode_start",
                    "patient_id": "P001",
                    "scenario": "tachycardia",
                    "sim_day": "2026-01-02",
                }
            )
            + "\n"
        )
        fh.write(
            json.dumps(
                {"event": "episode_end", "patient_id": "P001", "scenario": "fever", "sim_day": "2026-01-01"}
            )
            + "\n"
        )

    result = read_episode_scenarios_for_day(gt_path, "2026-01-01")
    assert result == {"P001": {"fever"}}


def test_read_episode_scenarios_missing_file_returns_empty():
    result = read_episode_scenarios_for_day("/nonexistent/path.jsonl", "2026-01-01")
    assert result == {}
