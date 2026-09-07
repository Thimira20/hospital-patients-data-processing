"""
Pure, unit-testable simulation logic for one bedside monitor: baseline +
mean-reverting random walk, plus injected sustained anomaly episodes.

Kept separate from vitals_producer.py (which owns Kafka I/O, dirty-data
injection, and process lifecycle) so the *simulation* logic itself -- the
part that determines whether an anomaly detector SHOULD fire -- can be
exercised by pytest without any Kafka/Docker dependency.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Optional

# Anomaly scenario deltas applied on top of a patient's baseline for the
# duration of the episode. Mirrors config/thresholds.yml `anomaly_scenarios`
# (kept in Python here, not re-parsed from YAML, so this module has zero
# file-I/O dependencies and is trivially unit-testable).
ANOMALY_SCENARIOS = {
    "tachycardia": {"duration_min": 30, "duration_max": 90, "hr_delta": 45},
    "desaturation": {"duration_min": 30, "duration_max": 90, "spo2_delta": -10},
    "hypotension": {"duration_min": 30, "duration_max": 90, "sbp_delta": -35, "dbp_delta": -20},
    "fever": {"duration_min": 30, "duration_max": 90, "temp_delta": 2.2},
    "sepsis_pattern": {
        "duration_min": 45,
        "duration_max": 90,
        "hr_delta": 35,
        "sbp_delta": -25,
        "dbp_delta": -15,
        "temp_delta": 1.8,
    },
}

# Mean-reverting random-walk step sizes (per tick) and reversion strength,
# tuned so a patient's trace looks like a real correlated physiological
# signal rather than independent white noise from tick to tick.
_WALK_STEP = {"heart_rate": 1.5, "spo2": 0.4, "systolic_bp": 2.0, "diastolic_bp": 1.5, "temperature": 0.05}
_REVERSION = 0.15  # fraction of the gap-to-baseline pulled back per tick


@dataclass
class AnomalyEpisode:
    scenario: str
    ticks_remaining: int
    deltas: dict


@dataclass
class PatientState:
    patient_id: str
    baseline_hr: float
    baseline_spo2: float
    baseline_sbp: float
    baseline_dbp: float
    baseline_temp: float

    heart_rate: float = field(init=False)
    spo2: float = field(init=False)
    systolic_bp: float = field(init=False)
    diastolic_bp: float = field(init=False)
    temperature: float = field(init=False)
    active_episode: Optional[AnomalyEpisode] = field(default=None, init=False)

    def __post_init__(self) -> None:
        self.heart_rate = self.baseline_hr
        self.spo2 = self.baseline_spo2
        self.systolic_bp = self.baseline_sbp
        self.diastolic_bp = self.baseline_dbp
        self.temperature = self.baseline_temp

    # ---- episode lifecycle --------------------------------------------

    def maybe_start_episode(self, probability: float, rng: random.Random) -> Optional[str]:
        """With `probability` per tick, start a new sustained anomaly episode
        (only if none is currently active). Returns the scenario name if one
        was started, else None."""
        if self.active_episode is not None:
            return None
        if rng.random() >= probability:
            return None
        scenario = rng.choice(list(ANOMALY_SCENARIOS.keys()))
        spec = ANOMALY_SCENARIOS[scenario]
        duration_ticks = rng.randint(spec["duration_min"], spec["duration_max"])
        self.active_episode = AnomalyEpisode(scenario=scenario, ticks_remaining=duration_ticks, deltas=spec)
        return scenario

    def _episode_deltas(self) -> dict:
        if self.active_episode is None:
            return {}
        return {
            "heart_rate": self.active_episode.deltas.get("hr_delta", 0.0),
            "spo2": self.active_episode.deltas.get("spo2_delta", 0.0),
            "systolic_bp": self.active_episode.deltas.get("sbp_delta", 0.0),
            "diastolic_bp": self.active_episode.deltas.get("dbp_delta", 0.0),
            "temperature": self.active_episode.deltas.get("temp_delta", 0.0),
        }

    def tick(self, rng: random.Random) -> dict:
        """Advance the random walk by one tick and return the current
        (baseline + walk + active episode delta) reading as a dict of the
        five vital fields. Consumes one tick of any active episode."""
        deltas = self._episode_deltas()

        targets = {
            "heart_rate": self.baseline_hr + deltas.get("heart_rate", 0.0),
            "spo2": self.baseline_spo2 + deltas.get("spo2", 0.0),
            "systolic_bp": self.baseline_sbp + deltas.get("systolic_bp", 0.0),
            "diastolic_bp": self.baseline_dbp + deltas.get("diastolic_bp", 0.0),
            "temperature": self.baseline_temp + deltas.get("temperature", 0.0),
        }

        for field_name, step in _WALK_STEP.items():
            current = getattr(self, field_name)
            target = targets[field_name]
            # mean-revert toward the (baseline + episode delta) target, plus noise
            current += (target - current) * _REVERSION + rng.gauss(0, step)
            setattr(self, field_name, current)

        # physiologically-plausible clamps (loose -- validate_reading() in
        # common/clinical.py applies the strict sensor-fault ranges later)
        self.spo2 = min(100.0, max(50.0, self.spo2))
        self.heart_rate = min(260.0, max(20.0, self.heart_rate))
        self.systolic_bp = min(260.0, max(40.0, self.systolic_bp))
        self.diastolic_bp = min(180.0, max(20.0, self.diastolic_bp))
        self.temperature = min(43.0, max(30.0, self.temperature))

        if self.active_episode is not None:
            self.active_episode.ticks_remaining -= 1
            if self.active_episode.ticks_remaining <= 0:
                self.active_episode = None

        return {
            "heart_rate": round(self.heart_rate, 1),
            "spo2": round(self.spo2, 1),
            "systolic_bp": round(self.systolic_bp, 1),
            "diastolic_bp": round(self.diastolic_bp, 1),
            "temperature": round(self.temperature, 2),
        }
