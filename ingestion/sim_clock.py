"""
Shared simulated-clock helper.

IMPORTANT DESIGN DECISION (documented here and in the report's Assumptions
section): this project uses TWO separate notions of time, deliberately kept
independent:

  1. **Real wall-clock time** (`event_ts` on every vital reading). Vitals are
     produced roughly every VITALS_INTERVAL_SECONDS of *real* time, and the
     speed layer's windows/watermarks (1-minute windows, 45s watermark) are
     defined in real seconds too. If event timestamps were instead expressed
     on the compressed simulated calendar (see below), a 5-minute "day"
     would make consecutive 2-second-apart readings appear ~9.6 simulated
     minutes apart -- blowing past any sane watermark and leaving most
     windows empty. So: streaming/windowing operates on REAL time.

  2. **The simulated calendar** (`sim_day`, e.g. "2026-01-03"), which moves
     SIM_DAY_SECONDS real seconds per simulated day (default 300s = 5 real
     minutes per "day"). This is the label attached to every vitals event and
     every lab-result row for BATCH purposes only: it drives the Parquet
     master-dataset partition column, the daily lab file's cadence and
     filename, and which "day" the Airflow DAG/backfill processes. It is
     what "1 simulated day = 5 real minutes" (stated in the README/report)
     actually refers to.

Every process that needs the simulated calendar (both producers, the batch
job, and the Airflow DAGs) constructs a `SimClock` pointed at the same
persisted state file (`{DATA_ROOT}/sim_clock.json`) so a container restart
does not reset simulated time -- the clock's origin (a real wall-clock
epoch) is written once, on first use, and read thereafter.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone


@dataclass
class SimClock:
    sim_start_date: date
    sim_day_seconds: float
    origin_epoch: float  # real time.time() when sim day 0 began

    # ---- construction -----------------------------------------------------

    @classmethod
    def load_or_create(
        cls,
        sim_start_date_str: str,
        sim_day_seconds: float,
        state_path: str = "/data/sim_clock.json",
    ) -> "SimClock":
        """
        Load the shared clock origin from `state_path` if it already exists
        (written by whichever process started first), otherwise create it.
        Uses O_CREAT|O_EXCL for a simple cross-process race-safe first write.
        """
        sim_start = date.fromisoformat(sim_start_date_str)

        os.makedirs(os.path.dirname(state_path) or ".", exist_ok=True)

        origin_epoch = None
        for _ in range(20):
            if os.path.exists(state_path):
                try:
                    with open(state_path, "r", encoding="utf-8") as fh:
                        data = json.load(fh)
                    origin_epoch = float(data["origin_epoch"])
                    break
                except (json.JSONDecodeError, KeyError, OSError):
                    time.sleep(0.1)
                    continue
            else:
                try:
                    fd = os.open(state_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                    origin_epoch = time.time()
                    with os.fdopen(fd, "w", encoding="utf-8") as fh:
                        json.dump(
                            {
                                "origin_epoch": origin_epoch,
                                "sim_start_date": sim_start_date_str,
                                "sim_day_seconds": sim_day_seconds,
                            },
                            fh,
                        )
                    break
                except FileExistsError:
                    time.sleep(0.1)
                    continue

        if origin_epoch is None:
            # Extremely unlikely fallback: proceed with "now" rather than crash.
            origin_epoch = time.time()

        return cls(
            sim_start_date=sim_start,
            sim_day_seconds=float(sim_day_seconds),
            origin_epoch=origin_epoch,
        )

    # ---- real-time helpers --------------------------------------------

    def elapsed_real_seconds(self) -> float:
        return time.time() - self.origin_epoch

    # ---- simulated-calendar helpers ---------------------------------------

    def elapsed_sim_days(self) -> float:
        return self.elapsed_real_seconds() / self.sim_day_seconds

    def sim_day_index(self) -> int:
        """0-based index of the current simulated day (can grow unbounded)."""
        return max(0, int(self.elapsed_sim_days()))

    def sim_day(self) -> str:
        """Current simulated calendar day, e.g. '2026-01-03'."""
        return self.sim_day_for_index(self.sim_day_index())

    def sim_day_for_index(self, index: int) -> str:
        return (self.sim_start_date + timedelta(days=index)).isoformat()

    def previous_sim_day(self) -> str:
        """The simulated day before the current one (what the daily batch DAG
        processes when it runs "today", per Lambda batch-layer convention of
        recomputing yesterday's complete day)."""
        return self.sim_day_for_index(max(0, self.sim_day_index() - 1))

    def day_progress(self) -> float:
        """Fraction (0..1) of the current simulated day elapsed."""
        return self.elapsed_sim_days() - self.sim_day_index()

    def seconds_until_next_day_boundary(self) -> float:
        """Real seconds remaining until sim_day_index() increments by one."""
        next_boundary_days = self.sim_day_index() + 1
        next_boundary_real_seconds = next_boundary_days * self.sim_day_seconds
        return max(0.0, next_boundary_real_seconds - self.elapsed_real_seconds())

    def sim_calendar_now(self) -> datetime:
        """
        The accelerated 'fictional' calendar timestamp -- useful for
        cosmetic fields like a lab result's collected_at, NOT for stream
        windowing (see module docstring).
        """
        sim_start_dt = datetime.combine(self.sim_start_date, datetime.min.time(), tzinfo=timezone.utc)
        return sim_start_dt + timedelta(seconds=self.elapsed_sim_days() * 86400)


def real_now_iso() -> str:
    """Real wall-clock UTC timestamp, ISO-8601 -- used for event_ts."""
    return datetime.now(timezone.utc).isoformat()
