"""Pure ETA error statistics (Phase 7.1, D-05, KPI-08).

ETA error = completed_ts - initial ETA; positive means later than estimated.
Pure function: no I/O, no clock, no randomness.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

from pizza.kpi.stats import mean


@dataclass(frozen=True)
class EtaErrorStats:
    """ETA error summary; None fields indicate insufficient data."""

    n: int
    mean_error_s: float | None
    mean_abs_error_s: float | None
    max_late_s: float | None  # largest positive error, 0.0 if none late


def eta_error_s(completed_ts: datetime | None, eta_ts: datetime | None) -> float | None:
    """Seconds by which completion missed the initial ETA (positive = later)."""
    if completed_ts is None or eta_ts is None:
        return None
    return (completed_ts - eta_ts).total_seconds()


def eta_error_stats(
    pairs: Mapping[str, tuple[datetime | None, datetime | None]],
) -> EtaErrorStats:
    """Aggregate ETA errors over order_id -> (completed_ts, eta_ts) pairs.

    Pairs with a missing timestamp are skipped. Keying by order_id makes
    duplicate deliveries collapse upstream.
    """
    errors = [
        e for c, t in pairs.values() if (e := eta_error_s(c, t)) is not None
    ]
    if not errors:
        return EtaErrorStats(n=0, mean_error_s=None, mean_abs_error_s=None, max_late_s=None)
    return EtaErrorStats(
        n=len(errors),
        mean_error_s=mean(errors),
        mean_abs_error_s=mean([abs(e) for e in errors]),
        max_late_s=max(0.0, max(errors)),
    )
