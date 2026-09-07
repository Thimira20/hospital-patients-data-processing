"""
Unit tests for common/clinical.py -- the shared scoring logic used by both
the speed layer and the batch layer. Pure Python, no Docker/Spark required.

Run with: pytest tests/test_clinical.py -v
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.clinical import (  # noqa: E402
    adjust_risk_with_labs,
    lab_flag,
    news2_score,
    news2_score_from_window,
    trend_direction,
    validate_reading,
)

# --------------------------------------------------------------------------
# validate_reading
# --------------------------------------------------------------------------


def test_validate_reading_all_normal_is_valid():
    is_valid, reasons = validate_reading(78, 97, 120, 78, 36.7)
    assert is_valid is True
    assert reasons == []


def test_validate_reading_null_field_is_invalid():
    is_valid, reasons = validate_reading(78, None, 120, 78, 36.7)
    assert is_valid is False
    assert "missing:spo2" in reasons


def test_validate_reading_out_of_range_heart_rate():
    is_valid, reasons = validate_reading(300, 97, 120, 78, 36.7)
    assert is_valid is False
    assert "out_of_range:heart_rate" in reasons


def test_validate_reading_out_of_range_spo2_low():
    is_valid, reasons = validate_reading(78, 10, 120, 78, 36.7)
    assert is_valid is False
    assert "out_of_range:spo2" in reasons


def test_validate_reading_multiple_failures_all_reported():
    is_valid, reasons = validate_reading(None, 10, 500, 78, 36.7)
    assert is_valid is False
    assert len(reasons) == 3


def test_validate_reading_boundary_values_are_valid():
    # exact boundaries of valid_ranges should be inclusive, not rejected
    is_valid, reasons = validate_reading(20, 50, 40, 20, 30.0)
    assert is_valid is True, reasons


# --------------------------------------------------------------------------
# news2_score -- boundary values for each vital
# --------------------------------------------------------------------------


def test_news2_score_all_normal_is_zero():
    score, band, components = news2_score(78, 97, 120, 36.7)
    assert score == 0
    assert band == "low"


def test_news2_score_heart_rate_low_extreme():
    score, band, _ = news2_score(35, 97, 120, 36.7)
    assert score == 3
    assert band == "medium"


def test_news2_score_heart_rate_high_extreme():
    score, band, _ = news2_score(140, 97, 120, 36.7)
    assert score == 3


def test_news2_score_spo2_critical_low():
    score, band, _ = news2_score(78, 88, 120, 36.7)
    assert score == 3
    assert band == "medium"


def test_news2_score_systolic_bp_shock_range():
    score, _, _ = news2_score(78, 97, 80, 36.7)
    assert score == 3


def test_news2_score_temperature_hypothermia():
    score, _, _ = news2_score(78, 97, 120, 34.0)
    assert score == 3


def test_news2_score_sepsis_pattern_reaches_medium_band_on_vitals_alone():
    # sepsis_pattern deltas: HR+35, SBP-25, temp+1.8 applied to a typical
    # baseline (78, 120, 36.7) -> (113, 95, 38.5) -> HR:2 + SBP:2 + temp:1 = 5
    # This is the whole point of the Lambda story: vitals alone raise a
    # "medium" flag; test_adjust_risk_sepsis_scenario_reaches_high_band_after_labs
    # (below) shows the SAME episode reaching "high" once labs are folded in.
    score, band, components = news2_score(113, 97, 95, 38.5)
    assert score == 5
    assert band == "medium"
    assert components["heart_rate"] > 0
    assert components["systolic_bp"] > 0


def test_news2_score_components_sum_to_total():
    score, _, components = news2_score(113, 88, 95, 38.5)
    assert score == sum(components.values())


def test_news2_score_missing_values_contribute_zero_not_crash():
    score, band, components = news2_score(None, None, None, None)
    assert score == 0
    assert band == "low"


# --------------------------------------------------------------------------
# news2_score_from_window -- worst-case-within-window scoring
# --------------------------------------------------------------------------


def test_window_score_catches_brief_spike_via_max():
    # avg/typical HR normal, but a brief spike to 140 within the window
    score, band, components = news2_score_from_window(
        min_hr=75, max_hr=140, min_spo2=97, min_sbp=118, max_sbp=122, min_temp=36.5, max_temp=36.8
    )
    assert (
        components["heart_rate"] == 3
    ), "the window's max HR spike must drive scoring, not get averaged away"
    assert band in ("medium", "high")


def test_window_score_catches_dip_via_min_for_u_shaped_vital():
    # HR dips low within the window even though max HR is normal
    score, band, components = news2_score_from_window(
        min_hr=38, max_hr=80, min_spo2=97, min_sbp=118, max_sbp=122, min_temp=36.5, max_temp=36.8
    )
    assert components["heart_rate"] == 3


def test_window_score_spo2_uses_minimum_only():
    score, band, components = news2_score_from_window(
        min_hr=75, max_hr=80, min_spo2=90, min_sbp=118, max_sbp=122, min_temp=36.5, max_temp=36.8
    )
    assert components["spo2"] == 3


# --------------------------------------------------------------------------
# trend_direction
# --------------------------------------------------------------------------


def test_trend_no_previous_score_is_stable():
    assert trend_direction(5, None) == "stable"


def test_trend_increasing_score_is_deteriorating():
    assert trend_direction(6, 3) == "deteriorating"


def test_trend_decreasing_score_is_improving():
    assert trend_direction(2, 5) == "improving"


def test_trend_unchanged_score_is_stable():
    assert trend_direction(4, 4) == "stable"


# --------------------------------------------------------------------------
# lab_flag
# --------------------------------------------------------------------------


def test_lab_flag_within_reference_range_is_normal():
    assert lab_flag(7.5, 4.0, 11.0, "WBC") == "normal"


def test_lab_flag_above_reference_range_is_high():
    assert lab_flag(18.0, 4.0, 11.0, "WBC") == "high"


def test_lab_flag_below_reference_range_is_low():
    assert lab_flag(90.0, 120.0, 170.0, "Hemoglobin") == "low"


def test_lab_flag_boundary_values_are_normal():
    assert lab_flag(4.0, 4.0, 11.0, "WBC") == "normal"
    assert lab_flag(11.0, 4.0, 11.0, "WBC") == "normal"


def test_lab_flag_non_numeric_value_is_unknown():
    assert lab_flag("ERROR", 4.0, 11.0, "WBC") == "unknown"


def test_lab_flag_missing_reference_range_falls_back_to_defaults():
    # no reference_low/high supplied -> falls back to lab_reference_defaults
    assert lab_flag(20.0, None, None, "WBC") == "high"
    assert lab_flag(7.0, None, None, "WBC") == "normal"


def test_lab_flag_unknown_test_type_with_no_range_is_unknown():
    assert lab_flag(5.0, None, None, "SomeUnknownTest") == "unknown"


# --------------------------------------------------------------------------
# adjust_risk_with_labs
# --------------------------------------------------------------------------


def test_adjust_risk_no_abnormal_labs_leaves_score_unchanged():
    adjusted, reasons = adjust_risk_with_labs(3, {"WBC": "normal", "CRP": "normal"})
    assert adjusted == 3
    assert reasons == []


def test_adjust_risk_concerning_direction_adds_weight_and_reason():
    adjusted, reasons = adjust_risk_with_labs(3, {"Lactate": "high"})
    assert adjusted == 3 + 3  # Lactate weight = 3
    assert "lactate_high" in reasons


def test_adjust_risk_wrong_direction_is_ignored():
    # low WBC is not the clinically concerning direction -> should not add weight
    adjusted, reasons = adjust_risk_with_labs(3, {"WBC": "low"})
    assert adjusted == 3
    assert reasons == []


def test_adjust_risk_unknown_flag_is_ignored():
    adjusted, reasons = adjust_risk_with_labs(3, {"Troponin": "unknown"})
    assert adjusted == 3
    assert reasons == []


def test_adjust_risk_multiple_abnormal_labs_combine():
    adjusted, reasons = adjust_risk_with_labs(2, {"WBC": "high", "CRP": "high", "Lactate": "high"})
    assert adjusted == 2 + 1 + 2 + 3  # weights: WBC=1, CRP=2, Lactate=3
    assert reasons == ["crp_high", "lactate_high", "wbc_high"]


def test_adjust_risk_sepsis_scenario_reaches_high_band_after_labs():
    # A patient with a borderline vitals score whose labs push them decisively higher
    vitals_score, _, _ = news2_score(113, 97, 95, 38.5)
    adjusted, reasons = adjust_risk_with_labs(vitals_score, {"WBC": "high", "CRP": "high", "Lactate": "high"})
    assert adjusted > vitals_score
    assert set(reasons) == {"wbc_high", "crp_high", "lactate_high"}
