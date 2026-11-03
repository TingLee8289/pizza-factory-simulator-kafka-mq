"""Pure due-date KPI statistics (Phase 7, KPI-07, D-09, D-11).

Lateness and tardiness metrics computed from completed orders and their due_ts.
Pure function: no I/O, no clock, no randomness.
"""

from collections.abc import Sequence
from dataclasses import dataclass

from pizza.kpi.stats import mean


@dataclass(frozen=True)
class DueStats:
    """Due-date statistics: late count, late percentage, tardiness mean/max.

    All fields are non-negative; None indicates insufficient data.
    """

    late_count: int  # Number of orders with tardiness > 0
    n_completed: int  # Total completed orders in the input list
    late_pct: float | None  # late_count / n_completed or None
    tardiness_mean_s: float | None  # Mean of tardiness_s list or None
    tardiness_max_s: float | None  # Max of tardiness_s list or None


def due_stats(tardiness_s: Sequence[float]) -> DueStats:
    """Compute due-date KPI metrics from a list of tardiness values.

    Args:
        tardiness_s: Sequence of tardiness in seconds, one per completed order.
                     tardiness = max(0, completed_ts - due_ts).
                     A value > 0 means the order was late.

    Returns:
        DueStats with late_count, n_completed, late_pct, tardiness_mean_s, tardiness_max_s.
        For empty input, n_completed is 0 and other metrics are None.
    """
    n = len(tardiness_s)

    if n == 0:
        return DueStats(
            late_count=0,
            n_completed=0,
            late_pct=None,
            tardiness_mean_s=None,
            tardiness_max_s=None,
        )

    # late = tardiness > 0 (strict inequality)
    late_count = sum(1 for t in tardiness_s if t > 0)
    late_pct = late_count / n if n > 0 else None

    # Mean and max of tardiness
    mean_val = mean(tardiness_s)
    max_val = max(tardiness_s) if tardiness_s else None

    return DueStats(
        late_count=late_count,
        n_completed=n,
        late_pct=late_pct,
        tardiness_mean_s=mean_val,
        tardiness_max_s=max_val,
    )
