"""
Unit tests for the pure-Python parts of processing/batch_job.py -- the
per-patient join/scoring logic that does NOT require a live Spark or
Postgres connection (only compute_daily_vitals() and write_results() need a
real runtime; PySpark is imported lazily inside those two functions
specifically so the rest of this module, including everything tested here,
works on a plain host install). Run with:
    pytest tests/test_batch_transforms.py -v
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd  # noqa: E402

from processing.batch_job import (  # noqa: E402
    build_risk_daily,
    flag_lab_results,
    vitals_reason_codes_from_components,
)


def test_vitals_reason_codes_picks_up_high_point_components():
    codes = vitals_reason_codes_from_components({"heart_rate": 3, "spo2": 0, "systolic_bp": 1, "temperature": 2})
    assert codes == ["abnormal_heart_rate", "abnormal_temperature"]


def test_vitals_reason_codes_empty_when_all_low_points():
    codes = vitals_reason_codes_from_components({"heart_rate": 1, "spo2": 0, "systolic_bp": 0, "temperature": 1})
    assert codes == []


def test_vitals_reason_codes_ignores_unknown_keys():
    codes = vitals_reason_codes_from_components({"heart_rate": 3, "some_other_field": 99})
    assert codes == ["abnormal_heart_rate"]


def test_build_risk_daily_combines_vitals_and_labs(monkeypatch):
    monkeypatch.setattr("processing.batch_job.previous_adjusted_score", lambda dsn, pid, sim_day: None)

    patients = [{"patient_id": "P001"}, {"patient_id": "P002"}]
    daily_vitals = [
        {
            "patient_id": "P001",
            "worst_score": 5,
            "worst_band": "medium",
            "vitals_reason_codes": ["abnormal_heart_rate"],
        }
    ]
    lab_flags_by_patient = {"P001": {"Lactate": "high", "WBC": "high"}}

    results = build_risk_daily(patients, daily_vitals, lab_flags_by_patient, "2026-01-01", dsn="unused")
    by_patient = {r["patient_id"]: r for r in results}

    p1 = by_patient["P001"]
    assert p1["vitals_score"] == 5
    assert p1["adjusted_risk_score"] == 5 + 3 + 1  # Lactate weight=3, WBC weight=1
    assert p1["risk_band"] == "high"
    assert set(p1["reason_codes"]) == {"abnormal_heart_rate", "lactate_high", "wbc_high"}
    assert p1["delta_direction"] == "new"

    p2 = by_patient["P002"]
    assert p2["vitals_score"] == 0  # no vitals data that day -> defaults to 0
    assert p2["adjusted_risk_score"] == 0
    assert p2["reason_codes"] == []


def test_build_risk_daily_computes_delta_against_previous_day(monkeypatch):
    monkeypatch.setattr("processing.batch_job.previous_adjusted_score", lambda dsn, pid, sim_day: 3)

    patients = [{"patient_id": "P001"}]
    daily_vitals = [{"patient_id": "P001", "worst_score": 6, "worst_band": "high", "vitals_reason_codes": []}]

    results = build_risk_daily(patients, daily_vitals, {}, "2026-01-02", dsn="unused")
    r = results[0]
    assert r["previous_day_score"] == 3
    assert r["score_delta"] == 6 - 3
    assert r["delta_direction"] == "deteriorating"


def test_flag_lab_results_flags_each_row_and_groups_by_patient():
    clean = pd.DataFrame(
        [
            {
                "patient_id": "P001",
                "test_type": "WBC",
                "result_value": 18.0,
                "unit": "10^9/L",
                "reference_low": 4.0,
                "reference_high": 11.0,
                "collected_at": "2026-01-01T06:00:00+00:00",
                "lab_batch_id": "2026-01-01-LAB",
            },
            {
                "patient_id": "P001",
                "test_type": "CRP",
                "result_value": 5.0,
                "unit": "mg/L",
                "reference_low": 0.0,
                "reference_high": 10.0,
                "collected_at": "2026-01-01T06:00:00+00:00",
                "lab_batch_id": "2026-01-01-LAB",
            },
            {
                "patient_id": "P002",
                "test_type": "Lactate",
                "result_value": 6.0,
                "unit": "mmol/L",
                "reference_low": 0.5,
                "reference_high": 2.0,
                "collected_at": "2026-01-01T06:00:00+00:00",
                "lab_batch_id": "2026-01-01-LAB",
            },
        ]
    )

    lab_flags_by_patient, rows_for_storage = flag_lab_results(clean)

    assert lab_flags_by_patient == {
        "P001": {"WBC": "high", "CRP": "normal"},
        "P002": {"Lactate": "high"},
    }
    assert len(rows_for_storage) == 3
    assert all(isinstance(r["result_value"], float) for r in rows_for_storage)
