"""Pure KPI state store: ``fold(state, event) -> state`` (KPI-05, KPI-06).

"Pure" here means no I/O, no clock and no randomness: the store records facts
only, as an order-insensitive upsert (events arrive interleaved across Kafka
partitions). Metrics are derived from the stored facts elsewhere, so shuffling
or duplicating events never changes the state.

Durations are never read from ``OpCompleted.duration_s`` (D-04); only ``ts``
differences are used downstream.

Run pause/resume facts are recorded as marks keyed by event_id; the frozen
intervals are derived later (``pizza.kpi.frozen``) so KPIs use active time.
"""

import uuid
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from pizza.common.constants import MAX_ORDERS_CAP, PRODUCTS, STATIONS
from pizza.common.dedupe import Deduper
from pizza.common.envelope import (
    LotBanked,
    LotReleased,
    Message,
    OpCompleted,
    OpDispatched,
    OpFailed,
    OpStarted,
    OrderLifecycle,
    RunPaused,
    RunResumed,
    RunStarted,
    StationStatus,
    TimeScaleChanged,
)

WeightsSource = Literal["run_started", "observed", "uniform"]

# DoS bounds (T-04-09).
MAX_OPS_CAP = MAX_ORDERS_CAP * len(STATIONS) * 2
MAX_SEEN_IDS_CAP = MAX_OPS_CAP * 2
MAX_SCALE_MARKS = 10_000


@dataclass
class BankInterval:
    """A BANK dwell interval; ``end is None`` while the lot is still banked."""

    start: datetime
    end: datetime | None
    reason: str


@dataclass
class OrderRec:
    product: str
    qty: int
    created_ts: datetime
    released_ts: datetime | None = None
    completed_ts: datetime | None = None
    failed_ts: datetime | None = None
    due_ts: datetime | None = None
    bank_intervals: list[BankInterval] = field(default_factory=list)


@dataclass
class OpRec:
    ready_ts: datetime | None = None
    dispatched_ts: datetime | None = None
    started_ts: datetime | None = None
    completed_ts: datetime | None = None
    failed_ts: datetime | None = None


@dataclass
class StationLog:
    """Station status log: (ts, status, seq) entries in arrival order."""

    entries: list[tuple[datetime, str, int]] = field(default_factory=list)


@dataclass
class KpiState:
    run_id: str | None
    orders: dict[str, OrderRec] = field(default_factory=dict)
    ops: dict[tuple[str, str, int], OpRec] = field(default_factory=dict)
    status_logs: dict[str, list[tuple[datetime, str, int]]] = field(default_factory=dict)
    seen_event_ids: set[uuid.UUID] = field(default_factory=set)
    dedup_dispatched: Deduper = field(default_factory=lambda: Deduper())
    dedup_started: Deduper = field(default_factory=lambda: Deduper())
    dedup_completed: Deduper = field(default_factory=lambda: Deduper())
    dedup_failed: Deduper = field(default_factory=lambda: Deduper())
    # order_id -> {event_id: (ts, is_banked, reason)}; intervals are derived from
    # the SET of marks so arrival order never matters (T-06-14).
    lot_marks: dict[str, dict[uuid.UUID, tuple[datetime, bool, str]]] = field(default_factory=dict)
    lot_mark_count: int = 0
    bottleneck: str | None = None
    challenger: str | None = None
    challenger_n: int = 0
    weights: dict[str, float] | None = None
    weights_source: WeightsSource = "observed"
    run_started_ts: datetime | None = None
    last_event_ts: datetime | None = None
    seq: int = 0
    # event_id -> (ts, is_pause); frozen intervals are derived from these (D-04).
    pause_marks: dict[uuid.UUID, tuple[datetime, bool]] = field(default_factory=dict)
    # event_id -> (ts, scale); the simulation speed is global, so these survive run resets
    # (quick-et6). Segments are derived from the SET of marks, never from arrival order.
    scale_marks: dict[uuid.UUID, tuple[datetime, float]] = field(default_factory=dict)


def new_state(
    run_id: str | None,
    weights: dict[str, float] | None = None,
    weights_source: WeightsSource = "observed",
    run_started_ts: datetime | None = None,
    scale_marks: dict[uuid.UUID, tuple[datetime, float]] | None = None,
) -> KpiState:
    return KpiState(
        run_id=run_id,
        weights=weights,
        weights_source=weights_source,
        run_started_ts=run_started_ts,
        scale_marks=dict(scale_marks) if scale_marks else {},
    )


def newest_run_id(facts: Iterable[Message]) -> str | None:
    """Run id of the lifecycle fact with the greatest ts."""
    best: OrderLifecycle | None = None
    for f in facts:
        if isinstance(f, OrderLifecycle) and (best is None or f.ts > best.ts):
            best = f
    return best.run_id if best is not None else None


def _min_ts(cur: datetime | None, new: datetime) -> datetime:
    return new if cur is None or new < cur else cur


def _touch(state: KpiState, ts: datetime) -> None:
    if state.last_event_ts is None or ts > state.last_event_ts:
        state.last_event_ts = ts


def derive_bank_intervals(
    marks: Iterable[tuple[datetime, bool, str]],
) -> list[BankInterval]:
    """Deterministic intervals from a set of (ts, is_banked, reason) marks.

    Sorted by (ts, banked-before-released); a banked mark opens an interval
    unless one is already open, a release closes the open one.
    """
    out: list[BankInterval] = []
    open_iv: BankInterval | None = None
    for ts, is_banked, reason in sorted(marks, key=lambda m: (m[0], not m[1], m[2])):
        if is_banked:
            if open_iv is None:
                open_iv = BankInterval(ts, None, reason)
                out.append(open_iv)
        elif open_iv is not None:
            open_iv.end = ts
            open_iv = None
    return out


def _op_rec(state: KpiState, key: tuple[str, str, int]) -> OpRec | None:
    rec = state.ops.get(key)
    if rec is None:
        if len(state.ops) >= MAX_OPS_CAP:
            return None
        rec = state.ops[key] = OpRec()
    return rec


def apply_run_params(
    state: KpiState, weights: dict[str, float] | None, run_started_ts: datetime | None
) -> KpiState:
    """Set RunStarted params on the state (pure). Invalid weights are ignored."""
    if not weights or any(v < 0 for v in weights.values()) or sum(weights.values()) <= 0:
        return state
    state.weights = dict(weights)
    state.weights_source = "run_started"
    state.run_started_ts = run_started_ts
    return state


def _fold_run_started(state: KpiState, ev: RunStarted) -> KpiState:
    weights = dict(ev.params.weights) if ev.params is not None else dict.fromkeys(PRODUCTS, 1.0)
    if ev.run_id != state.run_id:
        new = new_state(ev.run_id, weights, "run_started", ev.ts, state.scale_marks)
        _touch(new, ev.ts)
        return new
    return apply_run_params(state, weights, ev.ts)


def _fold_lifecycle(state: KpiState, ev: OrderLifecycle) -> KpiState:
    if ev.event_id in state.seen_event_ids:
        return state
    rec = state.orders.get(ev.order_id)
    if rec is None:
        if len(state.orders) >= MAX_ORDERS_CAP:
            return state
        rec = state.orders[ev.order_id] = OrderRec(ev.product, ev.qty, ev.created_ts)
        marks = state.lot_marks.get(ev.order_id)
        if marks:
            rec.bank_intervals = derive_bank_intervals(marks.values())
        # Set due_ts from event (constant per order, min-upsert style)
        if ev.due_ts is not None:
            rec.due_ts = _min_ts(rec.due_ts, ev.due_ts)
    else:
        rec.created_ts = _min_ts(rec.created_ts, ev.created_ts)
        # Set due_ts on existing rec (min-upsert: constant but may appear in later events)
        if ev.due_ts is not None:
            rec.due_ts = _min_ts(rec.due_ts, ev.due_ts)
    if ev.state == "released":
        rec.released_ts = _min_ts(rec.released_ts, ev.ts)
    elif ev.state == "completed":
        rec.completed_ts = _min_ts(rec.completed_ts, ev.ts)
    elif ev.state == "failed":
        rec.failed_ts = _min_ts(rec.failed_ts, ev.ts)
    if len(state.seen_event_ids) < MAX_SEEN_IDS_CAP:
        state.seen_event_ids.add(ev.event_id)
    _touch(state, ev.ts)
    return state


def _fold_dispatched(state: KpiState, ev: OpDispatched) -> KpiState:
    if ev.step not in STATIONS:
        return state
    # Bookkeeping only: the min-upsert below is what guarantees order-insensitivity.
    state.dedup_dispatched.first_time_op(ev)
    rec = _op_rec(state, (ev.order_id, ev.step, ev.visit))
    if rec is None:
        return state
    rec.dispatched_ts = _min_ts(rec.dispatched_ts, ev.ts)
    rec.ready_ts = _min_ts(rec.ready_ts, ev.ready_ts)
    _touch(state, ev.ts)
    return state


def _fold_started(state: KpiState, ev: OpStarted) -> KpiState:
    if ev.step not in STATIONS:
        return state
    state.dedup_started.first_time_op(ev)
    rec = _op_rec(state, (ev.order_id, ev.step, ev.visit))
    if rec is None:
        return state
    # First start in time order is what Track-In uses (D-13, SYS-17).
    rec.started_ts = _min_ts(rec.started_ts, ev.ts)
    _touch(state, ev.ts)
    return state


def _fold_completed(state: KpiState, ev: OpCompleted) -> KpiState:
    if ev.step not in STATIONS:
        return state
    state.dedup_completed.first_time_op(ev)
    rec = _op_rec(state, (ev.order_id, ev.step, ev.visit))
    if rec is None:
        return state
    # duration_s is informational and deliberately not read (D-04).
    rec.completed_ts = _min_ts(rec.completed_ts, ev.ts)
    _touch(state, ev.ts)
    return state


def _fold_failed(state: KpiState, ev: OpFailed) -> KpiState:
    if ev.step not in STATIONS:
        return state
    state.dedup_failed.first_time_op(ev)
    rec = _op_rec(state, (ev.order_id, ev.step, ev.visit))
    if rec is None:
        return state
    rec.failed_ts = _min_ts(rec.failed_ts, ev.ts)
    _touch(state, ev.ts)
    return state


def _fold_lot(state: KpiState, ev: LotBanked | LotReleased) -> KpiState:
    """Record a BANK mark keyed by event_id; intervals are re-derived (T-06-13/14)."""
    marks = state.lot_marks.get(ev.order_id)
    if marks is not None and ev.event_id in marks:
        return state
    if state.lot_mark_count >= MAX_SEEN_IDS_CAP:
        return state
    if marks is None:
        if len(state.lot_marks) >= MAX_ORDERS_CAP:
            return state
        marks = state.lot_marks[ev.order_id] = {}
    is_banked = isinstance(ev, LotBanked)
    marks[ev.event_id] = (ev.ts, is_banked, ev.reason if isinstance(ev, LotBanked) else "")
    state.lot_mark_count += 1
    rec = state.orders.get(ev.order_id)
    if rec is not None:
        rec.bank_intervals = derive_bank_intervals(marks.values())
    _touch(state, ev.ts)
    return state


def _fold_status(state: KpiState, ev: StationStatus) -> KpiState:
    if ev.machine_id not in STATIONS or ev.event_id in state.seen_event_ids:
        return state
    if len(state.seen_event_ids) >= MAX_SEEN_IDS_CAP:
        return state
    state.seen_event_ids.add(ev.event_id)
    state.seq += 1
    state.status_logs.setdefault(ev.machine_id, []).append((ev.ts, ev.status, state.seq))
    _touch(state, ev.ts)
    return state


def _fold_pause(state: KpiState, ev: RunPaused | RunResumed) -> KpiState:
    """Record a pause/resume mark keyed by event_id (idempotent, bounded)."""
    if ev.event_id in state.pause_marks:
        return state
    if len(state.pause_marks) >= MAX_SEEN_IDS_CAP:
        return state
    state.pause_marks[ev.event_id] = (ev.ts, isinstance(ev, RunPaused))
    _touch(state, ev.ts)
    return state


def _fold_scale(state: KpiState, ev: TimeScaleChanged) -> KpiState:
    """Record a speed mark keyed by event_id (idempotent, bounded).

    Deliberately does not touch ``last_event_ts``: the scale is not part of a run.
    """
    if ev.event_id in state.scale_marks:
        return state
    if len(state.scale_marks) >= MAX_SCALE_MARKS:
        return state
    state.scale_marks[ev.event_id] = (ev.ts, ev.scale)
    return state


def fold(state: KpiState, event: Message) -> KpiState:
    """Fold one event into the state (mutates in place; may return a new state).

    Returns a new object only on a run reset or run adoption.
    """
    if isinstance(event, TimeScaleChanged):
        return _fold_scale(state, event)  # global: not filtered by run_id
    if isinstance(event, RunStarted):
        return _fold_run_started(state, event)
    if event.run_id != state.run_id:
        # Adopt a strictly newer unknown run on its created lifecycle.
        if (
            isinstance(event, OrderLifecycle)
            and event.state == "created"
            and (
                state.run_id is None
                or (state.last_event_ts is not None and event.created_ts > state.last_event_ts)
            )
        ):
            return fold(
                new_state(event.run_id, weights_source="observed", scale_marks=state.scale_marks),
                event,
            )
        return state
    if isinstance(event, RunPaused | RunResumed):
        return _fold_pause(state, event)
    if isinstance(event, OrderLifecycle):
        return _fold_lifecycle(state, event)
    if isinstance(event, OpDispatched):
        return _fold_dispatched(state, event)
    if isinstance(event, OpStarted):
        return _fold_started(state, event)
    if isinstance(event, OpCompleted):
        return _fold_completed(state, event)
    if isinstance(event, OpFailed):
        return _fold_failed(state, event)
    if isinstance(event, LotBanked | LotReleased):
        return _fold_lot(state, event)
    if isinstance(event, StationStatus):
        return _fold_status(state, event)
    return state
