"""Pure seeded MTBF/MTTR breakdown model (FAIL-01, D-01, D-02, D-05).

No broker, asyncio or wall-clock imports. Three independent streams per station
(time-between-failures, time-to-repair, manual-repair) so operator actions never
perturb the seeded random schedule.
"""

import math
import random

from pizza.common.rng import stream_rng

FLOOR_S = 0.05


def draw_exp(rng: random.Random, mean_s: float, floor_s: float = FLOOR_S) -> float:
    """Exponential draw via inverse CDF, floored so a draw is never ~0."""
    return max(floor_s, -mean_s * math.log(1.0 - rng.random()))


class FailureSchedule:
    """Seeded busy-time-to-failure budgets and repair times for one station."""

    def __init__(self, seed: int, station: str, mtbf_s: float | None, mttr_s: float) -> None:
        self._mtbf_s = mtbf_s
        self._mttr_s = mttr_s
        self._tbf = stream_rng(seed, f"fail:{station}:tbf")
        self._ttr = stream_rng(seed, f"fail:{station}:ttr")
        self._manual = stream_rng(seed, f"fail:{station}:manual_ttr")
        self._budget: float | None = None

    @property
    def enabled(self) -> bool:
        return self._mtbf_s is not None

    def budget_left(self) -> float | None:
        """Busy seconds until the next failure; None when disabled."""
        if self._mtbf_s is None:
            return None
        if self._budget is None:
            self._budget = draw_exp(self._tbf, self._mtbf_s)
        return self._budget

    def consume(self, busy_s: float) -> None:
        """Spend busy time against the pending budget (no-op when disabled)."""
        if self._mtbf_s is None:
            return
        left = self.budget_left()
        assert left is not None
        self._budget = max(0.0, left - busy_s)

    def trip(self) -> float:
        """The budget is exhausted: draw a repair time and clear the budget."""
        self._budget = None
        return draw_exp(self._ttr, self._mttr_s)

    def manual_repair_s(self) -> float:
        """Repair time for a manual breakdown; independent of the seeded schedule."""
        return draw_exp(self._manual, self._mttr_s)


def plan_schedule(
    seed: int, station: str, mtbf_s: float | None, mttr_s: float, n: int
) -> list[tuple[float, float]]:
    """First n (busy_seconds_until_fail, repair_s) pairs; empty when disabled."""
    sched = FailureSchedule(seed, station, mtbf_s, mttr_s)
    out: list[tuple[float, float]] = []
    while sched.enabled and len(out) < n:
        budget = sched.budget_left()
        assert budget is not None
        sched.consume(budget)
        out.append((budget, sched.trip()))
    return out
