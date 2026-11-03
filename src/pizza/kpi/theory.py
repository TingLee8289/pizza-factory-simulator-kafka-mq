"""IE theory values for the configured recipes and product mix (D-08/D-09). Pure."""

from collections.abc import Mapping
from dataclasses import dataclass

from pizza.common.constants import QTY_MAX, QTY_MIN, STATIONS


@dataclass(frozen=True)
class Theory:
    expected_station_time_s: dict[str, float]
    bottleneck_station: str
    capacity_per_s: float
    raw_process_time_s: float
    critical_wip: float


def expected_qty() -> float:
    """E[qty] for the generator's uniform draw in [QTY_MIN, QTY_MAX]."""
    return (QTY_MIN + QTY_MAX) / 2


def compute_theory(
    recipes: Mapping[str, Mapping[str, float]],
    weights: Mapping[str, float],
    e_qty: float,
) -> Theory:
    """Expected per-order station times, capacity, raw process time, critical WIP."""
    if any(w < 0 for w in weights.values()):
        raise ValueError("weights must be non-negative")
    total = sum(weights.values())
    if total <= 0:
        raise ValueError("weights must have a positive sum")
    active = {p: w / total for p, w in weights.items() if w > 0}
    times: dict[str, float] = {}
    for station in STATIONS:
        table = recipes.get(station)
        if table is None:
            raise ValueError(f"missing recipe table for station {station!r}")
        acc = 0.0
        for product, share in active.items():
            if product not in table:
                raise ValueError(f"station {station!r} has no recipe for {product!r}")
            acc += share * table[product]
        times[station] = e_qty * acc
    max_time = max(times.values())
    if max_time <= 0:
        raise ValueError("station time must be positive")
    bottleneck = next(s for s in STATIONS if times[s] == max_time)
    capacity = 1.0 / max_time
    raw = sum(times.values())
    return Theory(
        expected_station_time_s=times,
        bottleneck_station=bottleneck,
        capacity_per_s=capacity,
        raw_process_time_s=raw,
        critical_wip=raw * capacity,
    )
