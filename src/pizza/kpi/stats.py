"""Pure statistics helpers for KPI folding. Stdlib only."""

from collections.abc import Sequence


def percentile(sorted_vals: Sequence[float], q: float) -> float | None:
    """Linear-interpolation percentile (numpy 'linear'). Caller passes SORTED values.

    q in [0, 1]; returns None for empty input.
    """
    if not 0.0 <= q <= 1.0:
        raise ValueError(f"q must be in [0, 1], got {q!r}")
    n = len(sorted_vals)
    if n == 0:
        return None
    if n == 1:
        return float(sorted_vals[0])
    pos = q * (n - 1)
    lo = int(pos)
    hi = min(lo + 1, n - 1)
    frac = pos - lo
    return float(sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * frac)


def mean(vals: Sequence[float]) -> float | None:
    """Arithmetic mean, None for empty input."""
    if not vals:
        return None
    return sum(vals) / len(vals)
