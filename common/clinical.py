"""
Shared clinical scoring/validation logic -- imported by BOTH the speed layer
(processing/stream_job.py) and the batch layer (processing/batch_job.py).

This module is the project's concrete answer to Lambda architecture's
classic "two codebases for the same logic" criticism (see PLAN.md section
0.1): there is exactly ONE implementation of "is this reading valid?", "what
is this patient's early-warning score?", and "how do lab results change the
risk picture?" -- both execution contexts call the same pure functions here.

Deliberately dependency-light: no pyspark import, no hard requirement on
PyYAML being present (falls back to embedded defaults if config/thresholds.yml
cannot be found), so this module is trivially unit-testable and safe to use
as a plain Python function inside a PySpark UDF.

Scoring approach: a simplified NEWS2 (National Early Warning Score 2, Royal
College of Physicians UK) -- a real, widely used clinical deterioration
score -- adapted to the four vitals this project's simulated sensors produce
(no respiration rate sensor). See config/thresholds.yml for the exact bands.
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Optional

# --------------------------------------------------------------------------
# Threshold loading (YAML file, with an embedded fallback so this module
# never hard-fails if config/thresholds.yml isn't on the search path -- e.g.
# a unit test importing this module in isolation).
# --------------------------------------------------------------------------

_EMBEDDED_DEFAULTS = {
    "news2": {
        "heart_rate": {
            "bands": [
                {"min": 0, "max": 40, "points": 3},
                {"min": 41, "max": 50, "points": 1},
                {"min": 51, "max": 90, "points": 0},
                {"min": 91, "max": 110, "points": 1},
                {"min": 111, "max": 130, "points": 2},
                {"min": 131, "max": 999, "points": 3},
            ]
        },
        "spo2": {
            "bands": [
                {"min": 0, "max": 91, "points": 3},
                {"min": 92, "max": 93, "points": 2},
                {"min": 94, "max": 95, "points": 1},
                {"min": 96, "max": 100, "points": 0},
            ]
        },
        "systolic_bp": {
            "bands": [
                {"min": 0, "max": 90, "points": 3},
                {"min": 91, "max": 100, "points": 2},
                {"min": 101, "max": 110, "points": 1},
                {"min": 111, "max": 219, "points": 0},
                {"min": 220, "max": 999, "points": 3},
            ]
        },
        "temperature": {
            "bands": [
                {"min": 0.0, "max": 35.0, "points": 3},
                {"min": 35.1, "max": 36.0, "points": 1},
                {"min": 36.1, "max": 38.0, "points": 0},
                {"min": 38.1, "max": 39.0, "points": 1},
                {"min": 39.1, "max": 99.0, "points": 2},
            ]
        },
    },
    "risk_bands": [
        {"min": 0, "max": 2, "band": "low"},
        {"min": 3, "max": 5, "band": "medium"},
        {"min": 6, "max": 999, "band": "high"},
    ],
    "valid_ranges": {
        "heart_rate": {"min": 20, "max": 250},
        "spo2": {"min": 50, "max": 100},
        "systolic_bp": {"min": 40, "max": 260},
        "diastolic_bp": {"min": 20, "max": 180},
        "temperature": {"min": 30.0, "max": 43.0},
    },
    "lab_reference_defaults": {
        "WBC": {"low": 4.0, "high": 11.0},
        "CRP": {"low": 0.0, "high": 10.0},
        "Lactate": {"low": 0.5, "high": 2.0},
        "Creatinine": {"low": 60, "high": 110},
        "Hemoglobin": {"low": 120, "high": 170},
        "Platelets": {"low": 150, "high": 400},
        "Troponin": {"low": 0.0, "high": 0.04},
    },
    "lab_risk_weight": {
        "WBC": 1,
        "CRP": 2,
        "Lactate": 3,
        "Creatinine": 1,
        "Hemoglobin": 1,
        "Platelets": 1,
        "Troponin": 3,
    },
}

# Direction that is clinically "concerning" for each lab test -- e.g. HIGH
# WBC/CRP/Lactate/Creatinine/Troponin indicate inflammation/organ stress,
# while LOW Hemoglobin/Platelets indicate anemia/thrombocytopenia. Only a
# flag in the concerning direction contributes to adjust_risk_with_labs().
LAB_CONCERNING_DIRECTION = {
    "WBC": "high",
    "CRP": "high",
    "Lactate": "high",
    "Creatinine": "high",
    "Troponin": "high",
    "Hemoglobin": "low",
    "Platelets": "low",
}

_CANDIDATE_PATHS = [
    os.environ.get("THRESHOLDS_PATH"),
    "/opt/app/config/thresholds.yml",
    "/app/config/thresholds.yml",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config", "thresholds.yml"),
    "config/thresholds.yml",
]


@lru_cache(maxsize=1)
def _thresholds() -> dict:
    for path in _CANDIDATE_PATHS:
        if not path:
            continue
        if os.path.exists(path):
            try:
                import yaml

                with open(path, encoding="utf-8") as fh:
                    loaded = yaml.safe_load(fh)
                if loaded:
                    return loaded
            except Exception:  # noqa: BLE001 -- fall through to embedded defaults
                pass
    return _EMBEDDED_DEFAULTS


# --------------------------------------------------------------------------
# Reading validation
# --------------------------------------------------------------------------


def validate_reading(
    heart_rate: Optional[float],
    spo2: Optional[float],
    systolic_bp: Optional[float],
    diastolic_bp: Optional[float],
    temperature: Optional[float],
) -> tuple[bool, list[str]]:
    """
    Judge whether one vitals reading is clinically plausible (sensor fault
    detection), independent of whether the JSON itself parsed correctly.

    Returns (is_valid, reasons) -- is_valid is True only if every field is
    present AND within its physiologically-plausible range (config/
    thresholds.yml `valid_ranges`). `reasons` lists every problem found (a
    reading can fail for more than one field at once).
    """
    ranges = _thresholds()["valid_ranges"]
    values = {
        "heart_rate": heart_rate,
        "spo2": spo2,
        "systolic_bp": systolic_bp,
        "diastolic_bp": diastolic_bp,
        "temperature": temperature,
    }
    reasons: list[str] = []
    for field, value in values.items():
        if value is None:
            reasons.append(f"missing:{field}")
            continue
        bounds = ranges[field]
        if not (bounds["min"] <= value <= bounds["max"]):
            reasons.append(f"out_of_range:{field}")
    return (len(reasons) == 0, reasons)


# --------------------------------------------------------------------------
# NEWS2-style scoring
# --------------------------------------------------------------------------


def _points_for(vital: str, value: Optional[float]) -> int:
    if value is None:
        return 0
    for band in _thresholds()["news2"][vital]["bands"]:
        if band["min"] <= value <= band["max"]:
            return int(band["points"])
    return 0


def _band_for_score(score: int) -> str:
    for band in _thresholds()["risk_bands"]:
        if band["min"] <= score <= band["max"]:
            return band["band"]
    return "high"


def risk_band_for_score(score: int) -> str:
    """Public wrapper around the risk-band lookup -- used by batch_job.py to
    classify the lab-ADJUSTED score (news2_score/news2_score_from_window
    already return their own band for the unadjusted score; the adjusted
    score needs the same band table applied again after adjust_risk_with_labs)."""
    return _band_for_score(score)


def news2_score(
    heart_rate: Optional[float],
    spo2: Optional[float],
    systolic_bp: Optional[float],
    temperature: Optional[float],
) -> tuple[int, str, dict[str, int]]:
    """
    Simplified NEWS2 score from a single point-in-time reading. Returns
    (total_score, risk_band, per_vital_points).
    """
    components = {
        "heart_rate": _points_for("heart_rate", heart_rate),
        "spo2": _points_for("spo2", spo2),
        "systolic_bp": _points_for("systolic_bp", systolic_bp),
        "temperature": _points_for("temperature", temperature),
    }
    total = sum(components.values())
    return total, _band_for_score(total), components


def news2_score_from_window(
    min_hr: Optional[float],
    max_hr: Optional[float],
    min_spo2: Optional[float],
    min_sbp: Optional[float],
    max_sbp: Optional[float],
    min_temp: Optional[float],
    max_temp: Optional[float],
) -> tuple[int, str, dict[str, int]]:
    """
    Worst-case NEWS2 score across a windowed aggregate, rather than a single
    reading -- a brief severe spike inside a 1-minute window should still be
    caught, not averaged away. For each vital whose NEWS2 band is U-shaped
    (too high AND too low both score points -- heart_rate, systolic_bp,
    temperature) we score both the window's min and max and keep the worse
    (higher-points) result. SpO2's band is monotonic (lower is always worse
    or equal), so only the window minimum is needed.
    """
    hr_points = max(_points_for("heart_rate", min_hr), _points_for("heart_rate", max_hr))
    sbp_points = max(_points_for("systolic_bp", min_sbp), _points_for("systolic_bp", max_sbp))
    temp_points = max(_points_for("temperature", min_temp), _points_for("temperature", max_temp))
    spo2_points = _points_for("spo2", min_spo2)

    components = {
        "heart_rate": hr_points,
        "spo2": spo2_points,
        "systolic_bp": sbp_points,
        "temperature": temp_points,
    }
    total = sum(components.values())
    return total, _band_for_score(total), components


def trend_direction(current_score: int, previous_score: Optional[int]) -> str:
    """Compare successive scores for the same patient. No prior score yet
    (first window since stream start) is reported as 'stable'."""
    if previous_score is None:
        return "stable"
    if current_score > previous_score:
        return "deteriorating"
    if current_score < previous_score:
        return "improving"
    return "stable"


# --------------------------------------------------------------------------
# Lab flagging / risk adjustment
# --------------------------------------------------------------------------


def lab_flag(
    result_value,
    reference_low: Optional[float],
    reference_high: Optional[float],
    test_type: Optional[str] = None,
) -> str:
    """
    Flag one lab result as 'low' | 'normal' | 'high' | 'unknown'.

    'unknown' covers both a non-numeric result_value (the batch producer's
    injected out-of-schema corruption, e.g. the literal string "ERROR") and
    missing reference ranges -- falls back to config/thresholds.yml's
    lab_reference_defaults keyed by `test_type` when reference_low/high are
    not supplied by the source file.
    """
    try:
        value = float(result_value)
    except (TypeError, ValueError):
        return "unknown"

    if reference_low is None or reference_high is None:
        defaults = _thresholds().get("lab_reference_defaults", {})
        default_range = defaults.get(test_type or "", None)
        if default_range is None:
            return "unknown"
        reference_low = default_range["low"]
        reference_high = default_range["high"]

    if value < reference_low:
        return "low"
    if value > reference_high:
        return "high"
    return "normal"


def adjust_risk_with_labs(
    vitals_score: int,
    lab_flags: dict[str, str],
) -> tuple[int, list[str]]:
    """
    Combine the vitals-derived NEWS2 score with the prior day's flagged lab
    results. `lab_flags` maps test_type -> 'low'|'normal'|'high'|'unknown'.

    Only a flag in that test's clinically "concerning" direction (see
    LAB_CONCERNING_DIRECTION) adds weight -- e.g. high WBC counts against the
    patient, but low WBC does not (it isn't the sepsis-relevant direction).

    Returns (adjusted_score, reason_codes) where reason_codes are strings
    like "lactate_high" or "hemoglobin_low", suitable for direct display in
    the daily report. The caller (batch_job.py) is responsible for combining
    these with any vitals-side reason codes (e.g. "sustained_tachycardia")
    it derives separately from the full day's aggregates.
    """
    weights = _thresholds().get("lab_risk_weight", {})
    added = 0
    reason_codes: list[str] = []

    for test_type, flag in lab_flags.items():
        if flag in ("normal", "unknown"):
            continue
        concerning_direction = LAB_CONCERNING_DIRECTION.get(test_type)
        if concerning_direction is None or flag != concerning_direction:
            continue
        weight = weights.get(test_type, 1)
        added += weight
        reason_codes.append(f"{test_type.lower()}_{flag}")

    adjusted_score = vitals_score + added
    return adjusted_score, sorted(reason_codes)
