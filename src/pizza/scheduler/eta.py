"""Pure ETA recurrence for lot release (Phase 7.1, D-01).

Pure module: no I/O, no clock, no randomness, no broker imports.

All values are simulated seconds relative to "now". The recurrence per route
station s is::

    finish[s] = max(finish[s-1], free_at[s]) + proc[s]

with finish[-1] starting at 0.0. Inputs are already-resolved processing seconds
(live recipe x qty), so this module stays recipe-agnostic. Simulated-to-wall
conversion is NOT done here; the adapter divides by the timescale (Pitfall 2).
"""

import math
from collections.abc import Sequence
from dataclasses import dataclass

# Upper clamp (7 days) for every finish offset; guards inf/overflow (T-07.1-05/06).
MAX_ETA_SIM_S: float = 604800.0


@dataclass(frozen=True)
class EtaEstimate:
    """Per-route-station finish offsets plus the overall lead time."""

    finish_sim_s: tuple[float | None, ...]
    lead_sim_s: float
    degraded: bool


def _sanitize(value: float) -> tuple[float, bool]:
    """Return (clamped value in [0, MAX_ETA_SIM_S], was_non_finite)."""
    if not math.isfinite(value):
        # -inf is treated like any other bad input: clamp to the maximum.
        return MAX_ETA_SIM_S, True
    return min(max(value, 0.0), MAX_ETA_SIM_S), False


def _offsets(
    free_at_s: Sequence[float], proc_s: Sequence[float], start_index: int
) -> tuple[list[float | None], bool]:
    out: list[float | None] = []
    prev = 0.0
    bad = False
    for idx, (free, proc) in enumerate(zip(free_at_s, proc_s, strict=True)):
        if idx < start_index:
            out.append(None)
            continue
        free_c, b1 = _sanitize(free)
        proc_c, b2 = _sanitize(proc)
        bad = bad or b1 or b2
        prev = min(max(prev, free_c) + proc_c, MAX_ETA_SIM_S)
        out.append(prev)
    return out, bad


def finish_offsets(
    free_at_s: Sequence[float], proc_s: Sequence[float], start_index: int = 0
) -> list[float | None]:
    """Finish offset per station; entries before start_index are None.

    Raises ValueError when the two sequences differ in length.
    """
    return _offsets(free_at_s, proc_s, start_index)[0]


def build_estimate(
    free_at_s: Sequence[float],
    proc_s: Sequence[float],
    start_index: int = 0,
    degraded: bool = False,
) -> EtaEstimate:
    """Build an EtaEstimate; degraded if the caller flagged it or any input was non-finite."""
    out, bad = _offsets(free_at_s, proc_s, start_index)
    last = out[-1] if out else None
    return EtaEstimate(
        finish_sim_s=tuple(out),
        lead_sim_s=last if last is not None else 0.0,
        degraded=degraded or bad,
    )
