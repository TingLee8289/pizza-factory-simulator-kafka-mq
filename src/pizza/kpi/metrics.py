"""Pure KPI derivation: ``advance(state, as_of, recipes, cfg) -> KpiSnapshot``.

Everything is derived from the stored fact records (cumulative since T0, D-03).
No I/O, no clock: ``as_of`` is passed in. The only state mutation is the
bottleneck hysteresis write-back (once per call, D-06).

Durations come from event ``ts`` differences only; ``OpCompleted.duration_s``
is never used (D-04).
"""

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pizza.common.constants import PRODUCTS, STATIONS
from pizza.common.envelope import (
    CycleTimeKpi,
    DueKpi,
    KpiSnapshot,
    LittleCheck,
    StationKpi,
    TheoryKpi,
)
from pizza.kpi.bottleneck import BottleneckCfg, BottleneckState, update_bottleneck
from pizza.kpi.due import due_stats
from pizza.kpi.fold import KpiState, OpRec, OrderRec
from pizza.kpi.frozen import (
    ActiveClock,
    Interval,
    active_seconds,
    frozen_intervals,
    scale_segments,
)
from pizza.kpi.stats import mean, percentile
from pizza.kpi.theory import compute_theory, expected_qty


@dataclass(frozen=True)
class KpiConfig:
    warmup_orders: int = 10
    little_tolerance: float = 0.10
    bottleneck_margin: float = 0.05
    bottleneck_confirm_n: int = 3
    bottleneck_min_util: float = 0.80


def _clean(x: float | None) -> float | None:
    """Map NaN/inf/None to None."""
    if x is None:
        return None
    if math.isnan(x) or math.isinf(x):
        return None
    return float(x)


def is_drained(state: KpiState) -> bool:
    """No incomplete orders and no station whose last status is busy."""
    if any(o.completed_ts is None for o in state.orders.values()):
        return False
    for log in state.status_logs.values():
        if log and max(log, key=lambda e: (e[0], e[2]))[1] == "busy":
            return False
    return True


def effective_as_of(state: KpiState, now: datetime) -> datetime:
    last = state.last_event_ts
    if last is None:
        return now
    if is_drained(state):
        return last
    return max(now, last)


# A plain interval list (legacy callers) or an ActiveClock (frozen + time scale).
Frozen = Sequence[Interval] | ActiveClock


def _secs(a: datetime, b: datetime, frozen: Frozen = ()) -> float:
    """Active simulation seconds between two instants (frozen time excluded, D-04)."""
    if isinstance(frozen, ActiveClock):
        return frozen.seconds(a, b)
    return active_seconds(a, b, frozen)


def _unpaused_secs(a: datetime, b: datetime, frozen: Frozen = ()) -> float:
    """Simulation seconds between two instants with pauses NOT removed."""
    if isinstance(frozen, ActiveClock):
        return frozen.unpaused_seconds(a, b)
    return (b - a).total_seconds()


def _tardiness(created: datetime, due: datetime, completed: datetime, frozen: Frozen) -> float:
    """Pause-compensated tardiness: active(created, completed) - unpaused(created, due).

    A pause after ``created`` extends the effective due by the overlapping paused
    time, so pause-only lateness vanishes and a pause is never double-counted.
    Falls back to ``active(due, completed)`` when ``due`` precedes ``created``.
    """
    if due < created:
        return max(0.0, _secs(due, completed, frozen))
    tard = _secs(created, completed, frozen) - _unpaused_secs(created, due, frozen)
    if tard < 0 and tard > -1e-9:
        tard = 0.0
    return max(0.0, tard)


def _ops_by_order(
    state: KpiState,
) -> dict[str, dict[str, list[tuple[int, OpRec]]]]:
    out: dict[str, dict[str, list[tuple[int, OpRec]]]] = {}
    for (oid, step, visit), rec in state.ops.items():
        out.setdefault(oid, {}).setdefault(step, []).append((visit, rec))
    for steps in out.values():
        for lst in steps.values():
            lst.sort(key=lambda p: p[0])
    return out


def _classify(
    order: OrderRec, steps: Mapping[str, list[tuple[int, OpRec]]]
) -> tuple[str, str] | None:
    """Return (station or '', kind); kind is wait, track_in, run or pre_release."""
    for idx, station in enumerate(STATIONS):
        recs = steps.get(station, [])
        if any(r.completed_ts is not None for _, r in recs):
            continue
        if not recs:
            if idx == 0 and order.released_ts is None:
                return ("", "pre_release")
            return (station, "wait")
        live = [r for _, r in recs if r.failed_ts is None]
        if not live:
            # Every attempt failed: the rework command is (about to be) waiting.
            return (station, "wait")
        rec = live[-1]
        if rec.started_ts is not None:
            return (station, "run")
        if rec.dispatched_ts is not None:
            return (station, "track_in")
        return (station, "wait")
    return None


def _status_seconds(
    log: Sequence[tuple[datetime, str, int]],
    t0: datetime,
    as_of: datetime,
    frozen: Frozen = (),
    wanted: str = "busy",
) -> float:
    total = 0.0
    start: datetime | None = None
    for ts, status, _seq in sorted(log, key=lambda e: (e[0], e[2])):
        if status == wanted:
            if start is None:
                start = ts
        elif start is not None:
            total += _overlap(start, ts, t0, as_of, frozen)
            start = None
    if start is not None:
        total += _overlap(start, as_of, t0, as_of, frozen)
    return total


def _busy_seconds(
    log: Sequence[tuple[datetime, str, int]],
    t0: datetime,
    as_of: datetime,
    frozen: Frozen = (),
) -> float:
    return _status_seconds(log, t0, as_of, frozen, "busy")


def _down_seconds(
    log: Sequence[tuple[datetime, str, int]],
    t0: datetime,
    as_of: datetime,
    frozen: Frozen = (),
) -> float:
    """Seconds spent ``down`` (a trailing open interval runs to ``as_of``)."""
    return _status_seconds(log, t0, as_of, frozen, "down")


def _excluded_bank_seconds(
    order: OrderRec,
    lo: datetime,
    hi: datetime,
    as_of: datetime,
    frozen: Frozen,
) -> float:
    """Customer/schedule BANK dwell inside [lo, hi] (D-22/D-23); capacity stays in."""
    total = 0.0
    for iv in order.bank_intervals:
        if iv.reason == "capacity":
            continue
        end = iv.end if iv.end is not None else as_of
        total += _overlap(iv.start, end, lo, hi, frozen)
    return total


def _overlap(a: datetime, b: datetime, lo: datetime, hi: datetime, frozen: Frozen = ()) -> float:
    s = max(a, lo)
    e = min(b, hi)
    return max(0.0, _secs(s, e, frozen))


def _little(
    area_s: float | None,
    window_s: float | None,
    completed_cts: Sequence[float],
    completed_qtys: Sequence[int],
    n_in_system: int,
    tolerance: float,
    warming_up: bool,
) -> LittleCheck:
    """Little's Law consistency check, L = lambda * W.

    Everything is in pizzas: ``area_s`` is the qty-weighted order-seconds area,
    L the time-averaged pizza WIP over [T0, as_of] (included orders), lambda the
    pizza completion rate (pizzas/s) and W the pizza-weighted mean cycle time
    (each completed order counts qty times). ``n_completed`` stays in lots.
    rel_error = |L - lambda*W| / L equals the fraction of order-seconds still
    in flight, so a drained run passes (~0) while a growing backlog (sustained
    overload) can legitimately fail: this is a boundary/consistency check, not a
    steady-state proof.
    """
    n_done = len(completed_cts)
    insufficient = LittleCheck(
        status="insufficient_data",
        tolerance=tolerance,
        n_completed=n_done,
        n_in_system=n_in_system,
    )
    if warming_up or n_done == 0 or area_s is None or window_s is None or window_s <= 0:
        return insufficient
    big_l = _clean(area_s / window_s)
    pizzas_done = sum(completed_qtys)
    if pizzas_done <= 0:
        return insufficient
    lam = _clean(pizzas_done / window_s)
    w = _clean(sum(c * q for c, q in zip(completed_cts, completed_qtys, strict=True)) / pizzas_done)
    if big_l is None or lam is None or w is None or big_l <= 0:
        return insufficient
    lam_w = lam * w
    rel = abs(big_l - lam_w) / big_l
    return LittleCheck(
        status="pass" if rel <= tolerance else "fail",
        l=big_l,
        lambda_per_s=lam,
        w_s=w,
        lambda_w=lam_w,
        rel_error=_clean(rel),
        tolerance=tolerance,
        n_completed=n_done,
        n_in_system=n_in_system,
    )


def _weights(state: KpiState) -> tuple[dict[str, float], str]:
    if state.weights:
        return dict(sorted(state.weights.items())), state.weights_source
    if state.orders:
        counts: dict[str, float] = {}
        for o in state.orders.values():
            counts[o.product] = counts.get(o.product, 0.0) + 1.0
        # Sorted keys: float sums must not depend on event arrival order.
        return dict(sorted(counts.items())), "observed"
    return dict.fromkeys(PRODUCTS, 1.0), "uniform"


def advance(
    state: KpiState,
    as_of: datetime,
    recipes: Mapping[str, Mapping[str, float]],
    cfg: KpiConfig,
) -> KpiSnapshot:
    frozen = ActiveClock(
        frozen_intervals(state.pause_marks, as_of), scale_segments(state.scale_marks, as_of)
    )
    ordered = sorted(state.orders.items(), key=lambda kv: (kv[1].created_ts, kv[0]))
    included = ordered[cfg.warmup_orders :]
    warming_up = len(included) == 0
    t0 = included[0][1].created_ts if included else None
    window = _secs(t0, as_of, frozen) if t0 is not None else None
    valid = (not warming_up) and window is not None and window > 0

    ops_idx = _ops_by_order(state)

    # WIP over all orders (D-12).
    wip_now = 0
    wip_pizzas = 0
    wip_pre = 0
    wip_bank = 0
    wip_bank_capacity = 0
    wip_failed = 0
    wip_split: dict[str, dict[str, int]] = {
        s: {"wait": 0, "track_in": 0, "run": 0} for s in STATIONS
    }
    for oid, order in state.orders.items():
        if order.completed_ts is not None:
            continue
        if order.failed_ts is not None:
            wip_failed += 1
            continue
        if order.bank_intervals and order.bank_intervals[-1].end is None:
            wip_bank += 1
            if order.bank_intervals[-1].reason == "capacity":
                wip_bank_capacity += 1
            continue
        wip_now += 1
        wip_pizzas += order.qty
        cls = _classify(order, ops_idx.get(oid, {}))
        if cls is None:
            continue
        station, kind = cls
        if kind == "pre_release":
            wip_pre += 1
        else:
            wip_split[station][kind] += 1

    negatives = 0
    cts: list[float] = []
    cts_qty: list[int] = []
    qty_done = 0
    area = 0.0
    area_pz = 0.0
    n_in_system_inc = 0
    tardiness_s: list[float] = []
    n_failed_inc = 0
    if t0 is not None:
        for _oid, o in included:
            if o.completed_ts is not None:
                end = o.completed_ts
            elif o.failed_ts is not None:
                end = o.failed_ts
            else:
                end = as_of
            o_area = _overlap(o.created_ts, end, t0, as_of, frozen)
            lo = max(o.created_ts, t0)
            hi = min(end, as_of)
            if hi > lo:
                o_area -= _excluded_bank_seconds(o, lo, hi, as_of, frozen)
            area += o_area
            area_pz += o_area * o.qty
            if o.completed_ts is None and o.failed_ts is None:
                n_in_system_inc += 1
            if o.failed_ts is not None and o.completed_ts is None:
                n_failed_inc += 1
            if o.completed_ts is not None and o.completed_ts <= as_of:
                ct = _secs(o.created_ts, o.completed_ts, frozen)
                ct -= _excluded_bank_seconds(o, o.created_ts, o.completed_ts, as_of, frozen)
                if ct < 0 and ct > -1e-9:
                    ct = 0.0
                if ct < 0:
                    negatives += 1
                else:
                    cts.append(ct)
                    cts_qty.append(o.qty)
                    qty_done += o.qty
                # Compute tardiness if due_ts is set (D-09, D-11)
                if o.due_ts is not None:
                    tard = _tardiness(o.created_ts, o.due_ts, o.completed_ts, frozen)
                    if tard >= 0:
                        tardiness_s.append(tard)

    # Pizza-weighted cycle time: each completed order counts qty times. ``n`` stays
    # the lot (order) count.
    sorted_cts = sorted(c for c, q in zip(cts, cts_qty, strict=True) for _ in range(q))
    # Per-pizza cycle time: ct/qty repeated qty times (total time spread over pizzas).
    sorted_pp = sorted(c / q for c, q in zip(cts, cts_qty, strict=True) for _ in range(q))
    if valid:
        cycle = CycleTimeKpi(
            n=len(cts),
            per_pizza_mean_s=_clean(mean(sorted_pp)),
            per_pizza_p50_s=_clean(percentile(sorted_pp, 0.5)),
            per_pizza_p95_s=_clean(percentile(sorted_pp, 0.95)),
            mean_s=_clean(mean(sorted_cts)),
            p50_s=_clean(percentile(sorted_cts, 0.5)),
            p95_s=_clean(percentile(sorted_cts, 0.95)),
        )
        assert window is not None
        throughput = _clean(len(cts) / window)
        pizzas = _clean(qty_done / window)
    else:
        cycle = CycleTimeKpi(n=len(cts))
        throughput = None
        pizzas = None

    # Due-date KPI (D-09, D-11, KPI-07)
    due_kpi_result = due_stats(tardiness_s)
    due_kpi = DueKpi(
        late_count=due_kpi_result.late_count,
        n_completed=due_kpi_result.n_completed,
        n_failed=n_failed_inc if valid else None,
        late_pct=_clean(due_kpi_result.late_pct) if valid else None,
        tardiness_mean_s=_clean(due_kpi_result.tardiness_mean_s) if valid else None,
        tardiness_max_s=_clean(due_kpi_result.tardiness_max_s) if valid else None,
    )

    # Per-station utilization and queue statistics.
    util: dict[str, float | None] = {}
    stations: dict[str, StationKpi] = {}
    for s in STATIONS:
        if valid and t0 is not None and window is not None:
            busy = _busy_seconds(state.status_logs.get(s, []), t0, as_of, frozen)
            u = _clean(min(1.0, busy / window))
            down = _down_seconds(state.status_logs.get(s, []), t0, as_of, frozen)
            avail = _clean(busy / (busy + down)) if busy + down > 0 else None
            down_s: float | None = _clean(down)
        else:
            u = None
            avail = None
            down_s = None
        n_done = 0
        qty_done_s = 0
        util[s] = u
        waits: list[float] = []
        tracks: list[float] = []
        queues: list[float] = []
        for oid, _o in included:
            for _visit, rec in ops_idx.get(oid, {}).get(s, []):
                w: float | None = None
                t: float | None = None
                if rec.dispatched_ts is not None and rec.ready_ts is not None:
                    w = _secs(rec.ready_ts, rec.dispatched_ts, frozen)
                    if w < 0:
                        negatives += 1
                        w = None
                    else:
                        waits.append(w)
                if rec.started_ts is not None and rec.dispatched_ts is not None:
                    t = _secs(rec.dispatched_ts, rec.started_ts, frozen)
                    if t < 0:
                        negatives += 1
                        t = None
                    else:
                        tracks.append(t)
                if w is not None and t is not None:
                    queues.append(w + t)
                if rec.completed_ts is not None and rec.completed_ts <= as_of:
                    n_done += 1
                    qty_done_s += _o.qty
        wp = wip_split[s]
        stations[s] = StationKpi(
            utilization=u,
            availability=avail,
            down_s=down_s,
            throughput_per_s=_clean(n_done / window) if valid and window else None,
            pizzas_per_s=_clean(qty_done_s / window) if valid and window else None,
            wip_wait=wp["wait"],
            wip_track_in=wp["track_in"],
            wip_run=wp["run"],
            wait_mean_s=_clean(mean(waits)),
            wait_p95_s=_clean(percentile(sorted(waits), 0.95)),
            track_in_mean_s=_clean(mean(tracks)),
            track_in_p95_s=_clean(percentile(sorted(tracks), 0.95)),
            queue_mean_s=_clean(mean(queues)),
            queue_p95_s=_clean(percentile(sorted(queues), 0.95)),
            queue_samples=len(queues),
        )

    # Theory next to measured values.
    weights, wsrc = _weights(state)
    capacity: float | None = None
    try:
        th = compute_theory(recipes, weights, expected_qty())
        capacity = th.capacity_per_s
        theory = TheoryKpi(
            weights_source=wsrc,  # type: ignore[arg-type]
            expected_station_time_s=dict(th.expected_station_time_s),
            bottleneck_station=th.bottleneck_station,
            capacity_per_s=_clean(th.capacity_per_s),
            capacity_pizzas_per_s=_clean(th.capacity_per_s * expected_qty()),
            raw_process_time_s=_clean(th.raw_process_time_s),
            critical_wip=_clean(th.critical_wip),
        )
    except ValueError:
        theory = TheoryKpi(weights_source=wsrc, expected_station_time_s={})  # type: ignore[arg-type]
    capacity_ratio = (
        _clean(throughput / capacity)
        if throughput is not None and capacity is not None and capacity > 0
        else None
    )

    # Bottleneck hysteresis: exactly one update per advance (D-06).
    bcfg = BottleneckCfg(
        margin=cfg.bottleneck_margin,
        confirm_n=cfg.bottleneck_confirm_n,
        min_util=cfg.bottleneck_min_util,
    )
    nb = update_bottleneck(
        BottleneckState(state.bottleneck, state.challenger, state.challenger_n), util, bcfg
    )
    state.bottleneck = nb.bottleneck
    state.challenger = nb.candidate
    state.challenger_n = nb.candidate_count

    little = _little(
        area_pz if valid else None,
        window,
        cts,
        cts_qty,
        n_in_system_inc,
        cfg.little_tolerance,
        warming_up,
    )
    wip_avg = _clean(area / window) if valid and window else None
    wip_avg_pizzas = _clean(area_pz / window) if valid and window else None

    # Only RunStarted-sourced, valid weights are persisted; observed ones are derived.
    run_weights: dict[str, float] | None = None
    if (
        state.weights_source == "run_started"
        and state.weights
        and all(v >= 0 for v in state.weights.values())
        and sum(state.weights.values()) > 0
    ):
        run_weights = dict(sorted(state.weights.items()))

    fields: dict[str, Any] = {
        "warming_up": warming_up,
        "warmup_orders": cfg.warmup_orders,
        "window_start_ts": t0,
        "window_s": _clean(window) if valid else None,
        "orders_total": len(state.orders),
        "orders_completed": sum(1 for o in state.orders.values() if o.completed_ts is not None),
        "throughput_per_s": throughput,
        "pizzas_per_s": pizzas,
        "capacity_ratio": capacity_ratio,
        "wip_now": wip_now,
        "wip_pizzas": wip_pizzas,
        "wip_pre_release": wip_pre,
        "wip_bank": wip_bank,
        "wip_bank_capacity": wip_bank_capacity,
        "wip_failed": wip_failed,
        "wip_avg": wip_avg,
        "wip_avg_pizzas": wip_avg_pizzas,
        "cycle_time": cycle,
        "due": due_kpi,
        "stations": stations,
        "bottleneck": nb.bottleneck,
        "bottleneck_candidate": nb.candidate,
        "bottleneck_candidate_count": nb.candidate_count,
        "theory": theory,
        "little": little,
        "negative_durations": negatives,
        "run_weights": run_weights,
        "run_started_ts": state.run_started_ts if run_weights is not None else None,
    }
    return KpiSnapshot.make(
        run_id=state.run_id or "unknown", machine_id="kpi-service", ts=as_of, **fields
    )
