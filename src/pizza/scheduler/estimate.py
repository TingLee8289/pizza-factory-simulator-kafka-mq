"""Read-only ETA adapter over SchedulerCore (Phase 7.1, D-01..D-03, D-06).

Turns scheduler state into per-station finish offsets using the pure recurrence
in ``eta.py``. The adapter never mutates the core and never reads a clock: ``now``
and ``scale`` (simulated seconds per wall second) are parameters.

"Ahead" is decided with the dispatch comparator itself (``core.ordering_key``):
a lot counts at a station only if its key sorts strictly before the key of the
lot being estimated. There is no second comparator here.
"""

import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from pizza.scheduler import eta
from pizza.scheduler.core import (
    BacklogEntry,
    OrderContext,
    SchedulerCore,
    ordering_key,
)

_NEW_ID = "￿-new"  # sorts after every real order id: existing lots win ties
_TERMINAL = {"failed", "completed"}


@dataclass(frozen=True)
class NewEstimate:
    lead_wall_s: float
    station_lead_wall_s: dict[str, float]
    degraded: bool


@dataclass
class _Lot:
    order_id: str
    position: int
    entered_ts: datetime
    visit: int
    running: bool
    keys: dict[int, Any] = field(default_factory=dict)
    procs: dict[int, float] = field(default_factory=dict)


@dataclass
class _View:
    lots: list[_Lot]
    repair_sim: list[float]
    running_rem: list[float]
    degraded: bool


def _new_lot_context(
    core: SchedulerCore,
    station: str,
    product: str,
    qty: int,
    provisional_due: datetime,
    priority: int,
) -> OrderContext:
    """The ONLY place mapping request fields to OrderContext (phase 07.2 changes this seam)."""
    proc_s = core.config.recipes.get(station, {}).get(product, math.inf) * qty
    return OrderContext(due_ts=provisional_due, priority=priority, proc_s=proc_s)


def _in_flight(core: SchedulerCore) -> list[_Lot]:
    """Every live lot once: backlog, outstanding and cooling. BANK/failed/completed excluded."""
    route = core.route
    lots: list[_Lot] = []

    def live(oid: str) -> bool:
        st = core.orders.get(oid)
        return st is not None and oid not in core.banked and not (_TERMINAL & st.emitted)

    for idx, station in enumerate(route):
        for e in core.backlog[station]:
            if live(e.order_id):
                lots.append(_Lot(e.order_id, idx, e.entered_ts, e.visit, False))
        out = core.outstanding[station]
        if out is not None and live(out.order_id):
            lots.append(_Lot(out.order_id, idx, out.entered_ts, out.visit, True))
    for oid, rec in core.cooling.items():
        if live(oid) and rec.station in route:
            lots.append(_Lot(oid, route.index(rec.station), rec.entered_ts, rec.visit, False))
    return lots


def _view(core: SchedulerCore, now: datetime, scale: float) -> _View:
    n = len(core.route)
    lots = _in_flight(core)
    degraded = False
    for lot in lots:
        for s in range(lot.position, n):
            entry = BacklogEntry(lot.order_id, s, lot.entered_ts, lot.visit)
            ctx = core._ctx(core.route[s], entry)
            lot.keys[s] = ordering_key(core.rule, entry, ctx)
            lot.procs[s] = ctx.proc_s
    repair = [0.0] * n
    running_rem = [0.0] * n
    for s, station in enumerate(core.route):
        h = core.health[station]
        if h.offline or h.reported is None or h.reported == "offline":
            degraded = True
        elif h.reported == "down":
            if h.repair_until is None:
                degraded = True
            else:
                repair[s] = max(0.0, (h.repair_until - now).total_seconds()) * scale
    for lot in lots:
        if not lot.running:
            continue
        proc = lot.procs[lot.position]
        if not math.isfinite(proc):
            degraded = True
            continue
        started = core.outstanding_started[core.route[lot.position]]
        if started is None:
            running_rem[lot.position] = proc
        else:
            elapsed = max(0.0, (now - started).total_seconds()) * scale
            running_rem[lot.position] = max(0.0, proc - elapsed)
    return _View(lots, repair, running_rem, degraded)


def _ahead(view: _View, s: int, key: Any, exclude_id: str | None) -> list[_Lot]:
    """Lots whose work at station index s sorts strictly before ``key``."""
    out: list[_Lot] = []
    for lot in view.lots:
        if lot.order_id == exclude_id or lot.position > s:
            continue
        if lot.running and lot.position == s:
            continue  # accounted for as remaining running time
        if lot.keys[s] < key:
            out.append(lot)
    return out


def _free_at(view: _View, s: int, key: Any, exclude_id: str | None) -> tuple[float, bool]:
    """Simulated seconds until station s can start the lot with ``key``."""
    total = view.repair_sim[s] + view.running_rem[s]
    bad = False
    for lot in _ahead(view, s, key, exclude_id):
        proc = lot.procs[s]
        if math.isfinite(proc):
            total += proc
        else:
            bad = True
    return total, bad


def estimate_new(
    core: SchedulerCore,
    *,
    product: str,
    qty: int,
    priority: int,
    due_ts: datetime | None,
    buffer_k: float,
    now: datetime,
    scale: float,
) -> NewEstimate:
    if not scale > 0 or not math.isfinite(scale):
        raise ValueError(f"scale must be positive and finite, got {scale!r}")
    route = core.route
    recipes = core.config.recipes
    proc_sim: list[float] = []
    for station in route:
        unit = recipes.get(station, {}).get(product)
        if unit is None or not math.isfinite(unit):
            raise ValueError(f"unknown product or non-finite recipe for {product!r} at {station!r}")
        proc_sim.append(unit * qty)
    if due_ts is not None:
        provisional_due = due_ts
    else:
        provisional_due = now + timedelta(seconds=sum(proc_sim) / scale * buffer_k)
    view = _view(core, now, scale)
    degraded = view.degraded
    free_at: list[float] = []
    for s, station in enumerate(route):
        entry = BacklogEntry(_NEW_ID, s, now)
        ctx = _new_lot_context(core, station, product, qty, provisional_due, priority)
        key = ordering_key(core.rule, entry, ctx)
        free, bad = _free_at(view, s, key, None)
        free_at.append(free)
        degraded = degraded or bad
    est = eta.build_estimate(free_at, proc_sim, 0, degraded)
    per_station = {
        station: (off if off is not None else 0.0) / scale
        for station, off in zip(route, est.finish_sim_s, strict=True)
    }
    return NewEstimate(est.lead_sim_s / scale, per_station, est.degraded)


def _trunc_ms(t: datetime) -> datetime:
    return t.replace(microsecond=(t.microsecond // 1000) * 1000)


def _lot_offsets(view: _View, lot: _Lot, n: int) -> tuple[list[float | None], list[float]]:
    """Finish offsets (sim s) and per-station proc for one lot; shared by wip/lot estimates.

    Raises ValueError for a non-finite recipe on the lot's remaining route.
    """
    j = lot.position
    free_at = [0.0] * n
    proc = [0.0] * n
    for s in range(j, n):
        p = lot.procs[s]
        if not math.isfinite(p):
            raise ValueError(f"non-finite recipe for {lot.order_id!r}")
        if lot.running and s == j:
            proc[s] = view.running_rem[s]
            continue
        proc[s] = p
        free_at[s], _bad = _free_at(view, s, lot.keys[s], lot.order_id)
    return eta.finish_offsets(free_at, proc, start_index=j), proc


def estimate_wip(
    core: SchedulerCore, *, now: datetime, scale: float, limit: int = 200
) -> dict[str, datetime]:
    """Predicted completion per in-flight lot (D-06); first ``limit`` by (entered_ts, order_id)."""
    if not scale > 0 or not math.isfinite(scale):
        raise ValueError(f"scale must be positive and finite, got {scale!r}")
    view = _view(core, now, scale)
    lots = sorted(view.lots, key=lambda lot: (lot.entered_ts, lot.order_id))[:limit]
    n = len(core.route)
    result: dict[str, datetime] = {}
    for lot in lots:
        try:
            offsets, _proc = _lot_offsets(view, lot, n)
            last = offsets[-1]
            if last is None:
                continue
            result[lot.order_id] = _trunc_ms(now + timedelta(seconds=last / scale))
        except ValueError:
            continue
    return result


def estimate_lot(
    core: SchedulerCore, order_id: str, *, now: datetime, scale: float
) -> dict[str, tuple[datetime | None, datetime]] | None:
    """Per-station (start, finish) for one in-flight lot; None when unknown/terminal/banked.

    The running station has no start (the real start is already a Kafka fact).
    """
    if not scale > 0 or not math.isfinite(scale):
        raise ValueError(f"scale must be positive and finite, got {scale!r}")
    view = _view(core, now, scale)
    lot = next((x for x in view.lots if x.order_id == order_id), None)
    if lot is None:
        return None
    n = len(core.route)
    try:
        offsets, proc = _lot_offsets(view, lot, n)
    except ValueError:
        return None
    result: dict[str, tuple[datetime | None, datetime]] = {}
    for s in range(lot.position, n):
        off = offsets[s]
        if off is None:
            continue
        finish = _trunc_ms(now + timedelta(seconds=off / scale))
        if lot.running and s == lot.position:
            start = None
        else:
            start = _trunc_ms(now + timedelta(seconds=(off - proc[s]) / scale))
        result[core.route[s]] = (start, finish)
    return result
