"""Bottleneck detection with hysteresis (D-05..D-07). Pure.

Cumulative utilization (D-03) is heavily smoothed, so tests drive
``update_bottleneck`` directly with scripted utilization dicts. The state
machine advances once per call (one call per snapshot tick, D-06).
The incumbent persists through low-utilization ticks (WR-01); only the
challenger counter is reset.
"""

from collections.abc import Mapping
from dataclasses import dataclass

from pizza.common.constants import STATIONS


@dataclass(frozen=True)
class BottleneckCfg:
    margin: float
    confirm_n: int
    min_util: float


@dataclass(frozen=True)
class BottleneckState:
    bottleneck: str | None = None
    candidate: str | None = None
    candidate_count: int = 0


def update_bottleneck(
    prev: BottleneckState,
    util: Mapping[str, float | None],
    cfg: BottleneckCfg,
) -> BottleneckState:
    top: str | None = None
    top_u = -1.0
    for s in STATIONS:
        u = util.get(s)
        if u is None:
            continue
        if u > top_u:
            top, top_u = s, u
    if top is None or top_u < cfg.min_util:
        # WR-01: a low-utilization tick keeps the incumbent but breaks the
        # challenger run (candidate defaults to None/0).
        return BottleneckState(bottleneck=prev.bottleneck)

    inc = prev.bottleneck
    if inc is None:
        # First adoption: same top station for confirm_n consecutive ticks.
        count = prev.candidate_count + 1 if prev.candidate == top else 1
        if count >= cfg.confirm_n:
            return BottleneckState(bottleneck=top)
        return BottleneckState(bottleneck=None, candidate=top, candidate_count=count)

    if top == inc:
        return BottleneckState(bottleneck=inc)
    inc_u = util.get(inc)
    gap_ok = inc_u is None or (top_u - inc_u) >= cfg.margin
    if not gap_ok:
        return BottleneckState(bottleneck=inc)
    count = prev.candidate_count + 1 if prev.candidate == top else 1
    if count >= cfg.confirm_n:
        return BottleneckState(bottleneck=top)
    return BottleneckState(bottleneck=inc, candidate=top, candidate_count=count)
