"""Pure line-state fold for the dashboard (D-01, DASH-01).

``fold(state, msg) -> LineState`` records facts only, as an order-insensitive
upsert (min-ts per field), so shuffling or duplicating the same facts never
changes ``line_snapshot``. Everything the browser shows (station status, queue
length, current lot, lot state, paused flag) is *derived* from those facts at
snapshot time, never accumulated incrementally and never computed in the
browser.

No clock, asyncio or broker imports: ``ts`` values are taken from the facts.
Timestamps are typed ``Any`` (tz-aware datetimes) to keep this module free of
time-library imports, mirroring the kpi fold discipline.
"""

import uuid
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from pizza.common.constants import MAX_ORDERS_CAP, PRIORITY_DEFAULT, STATIONS
from pizza.common.envelope import (
    LotBanked,
    LotHold,
    LotReleased,
    LotStocked,
    LotUnstocked,
    Message,
    OpCompleted,
    OpDispatched,
    OpFailed,
    OpStarted,
    OrderLifecycle,
    PriorityChanged,
    RuleChanged,
    RunPaused,
    RunResumed,
    StationStatus,
)
from pizza.common.topology import RETRY_TTL_MS
from pizza.kpi.eta import eta_error_s
from pizza.kpi.frozen import Interval, frozen_intervals, frozen_overlap

Ts = Any  # tz-aware datetime

# DoS bounds (T-05-21).
MAX_COMPLETED_LOTS = 300
MAX_DONE_IN_SNAPSHOT = 50
MAX_STEP_RECS = MAX_ORDERS_CAP * len(STATIONS) * 2
MAX_SEEN_IDS_CAP = MAX_STEP_RECS * 2
MAX_STATUS_LOG = 50
MAX_PAUSE_MARKS = 200
MAX_LOT_MARKS = 8
# Per-lot failure history in the snapshot; newest entries are kept.
MAX_LOT_FAILURES = 20
# A reworked lot reads as "cooling" for the hold-queue TTL plus this margin (D-09/D-11).
COOLING_MARGIN_S = 2.0
COOLING_WINDOW = timedelta(milliseconds=RETRY_TTL_MS) + timedelta(seconds=COOLING_MARGIN_S)

_LAST_STATION = STATIONS[-1]


@dataclass
class LotRec:
    product: str
    qty: int
    created_ts: Ts
    due_ts: Ts
    priority: int = PRIORITY_DEFAULT
    # Phase 07.2: ts of the fact that set ``priority`` (newest wins; change fact wins ties).
    priority_ts: Ts | None = None
    # (ts, previous, new) manual priority changes, for the inspector's Process Detail notes.
    priority_marks: list[tuple[Ts, int, int]] = field(default_factory=list)
    source: str | None = None
    released_ts: Ts | None = None
    completed_ts: Ts | None = None
    # (ts, kind | None, station); kind None = unstocked.
    stock_marks: list[tuple[Ts, str | None, str]] = field(default_factory=list)
    # (ts, is_banked, reason, source, station); is_banked False = released.
    bank_marks: list[tuple[Ts, bool, str | None, str | None, str]] = field(default_factory=list)
    # (ts, rank, kind, reason, station, state); rank 0 = requested, 1 = cleared (cancelled/applied).
    hold_marks: list[tuple[Ts, int, str, str, str, str]] = field(default_factory=list)
    failed: tuple[Ts, str, int] | None = None
    # Phase 7.1 D-04: frozen initial ETA, min-upserted from lifecycle facts.
    eta_ts: Ts | None = None
    station_eta: dict[str, Ts] | None = None


@dataclass
class StepRec:
    dispatched_ts: Ts | None = None
    started_ts: Ts | None = None
    completed_ts: Ts | None = None
    failed_ts: Ts | None = None
    failed_cause: str | None = None


@dataclass
class LineState:
    run_id: str | None
    adopt: bool = True
    lots: dict[str, LotRec] = field(default_factory=dict)
    steps: dict[str, dict[tuple[str, int], StepRec]] = field(default_factory=dict)
    step_count: int = 0
    status_logs: dict[str, list[tuple[Ts, str, int]]] = field(default_factory=dict)
    station_meta: dict[str, tuple[Ts, str | None, Ts | None]] = field(default_factory=dict)
    pause_marks: dict[uuid.UUID, tuple[Ts, bool]] = field(default_factory=dict)
    seen_event_ids: set[uuid.UUID] = field(default_factory=set)
    pruned_ids: set[str] = field(default_factory=set)
    pruned_completed: int = 0
    pruned_completed_pizzas: int = 0
    last_event_ts: Ts | None = None
    seq: int = 0
    # Phase 7 D-04: active dispatch rule, folded from RuleChanged (newest ts wins).
    # None until the first RuleChanged fact is seen; the scheduler always emits one
    # on RunStarted, so this becomes "edd" (or the chosen rule) almost immediately
    # once a run starts. Before that (no run, or mid-replay) the UI shows a dash.
    rule: str | None = None
    rule_ts: Ts | None = None


def new_line_state(run_id: str | None, adopt: bool = True) -> LineState:
    return LineState(run_id=run_id, adopt=adopt)


def new_run_state(run_id: str, early: tuple[Message, ...] | list[Message] = ()) -> LineState:
    """State for a run just started, replaying facts that arrived before its start callback.

    Station reports for the new run can reach the dashboard before the start callback
    resets the state; folding them again keeps those idle/busy reports instead of
    dropping them. ``fold`` ignores facts for other runs.
    """
    state = new_line_state(run_id, adopt=False)
    for msg in early:
        state = fold(state, msg)
    return state


def _min_ts(cur: Ts | None, new: Ts) -> Ts:
    return new if cur is None or new < cur else cur


def _max_ts(cur: Ts | None, new: Ts) -> Ts:
    return new if cur is None or new > cur else cur


def _iso(ts: Ts | None) -> str | None:
    if ts is None:
        return None
    utc = (ts - ts.utcoffset()).replace(tzinfo=None)
    return utc.isoformat(timespec="milliseconds") + "Z"


def _touch(state: LineState, ts: Ts) -> None:
    state.last_event_ts = _max_ts(state.last_event_ts, ts)


def _remember(state: LineState, event_id: uuid.UUID) -> bool:
    """True if the id is new (and now remembered); False on a duplicate or full set."""
    if event_id in state.seen_event_ids or len(state.seen_event_ids) >= MAX_SEEN_IDS_CAP:
        return False
    state.seen_event_ids.add(event_id)
    return True


# ---- fold -----------------------------------------------------------------


def _lot_done_ts(state: LineState, order_id: str, lot: LotRec) -> Ts | None:
    """Completion time of a lot (lifecycle completed or package op completed).

    A lot parked (or scrapped / about to be held) after Package has all 7 op.completed but
    no ``completed`` lifecycle: the Package completion is not a lot completion then.
    """
    if lot.completed_ts is None and (
        lot.failed is not None or _latest_bank(lot) is not None or _latest_hold(lot) is not None
    ):
        return None
    pkg_done = _step_done_ts(state, order_id, _LAST_STATION)
    candidates = [t for t in (lot.completed_ts, pkg_done) if t is not None]
    return min(candidates) if candidates else None


def _step_recs(state: LineState, order_id: str, station: str) -> list[tuple[int, StepRec]]:
    return sorted(
        (
            (visit, rec)
            for (st, visit), rec in state.steps.get(order_id, {}).items()
            if st == station
        ),
        key=lambda p: p[0],
    )


def _step_done_ts(state: LineState, order_id: str, station: str) -> Ts | None:
    recs = _step_recs(state, order_id, station)
    if not recs:
        return None
    return recs[-1][1].completed_ts


def _prune(state: LineState) -> None:
    done = []
    for oid, lot in state.lots.items():
        dts = _lot_done_ts(state, oid, lot)
        if dts is not None:
            done.append((dts, oid))
    excess = len(done) - MAX_COMPLETED_LOTS
    if excess <= 0:
        return
    done.sort()
    drop = {oid for _, oid in done[:excess]}
    state.pruned_completed_pizzas += sum(state.lots[oid].qty or 0 for oid in drop)
    for oid in drop:
        del state.lots[oid]
    for oid in drop:
        state.step_count -= len(state.steps.pop(oid, {}))
    state.pruned_completed += len(drop)
    if len(state.pruned_ids) < MAX_ORDERS_CAP * 2:
        state.pruned_ids |= drop


def _fold_lifecycle(state: LineState, ev: OrderLifecycle) -> LineState:
    if ev.order_id in state.pruned_ids or not _remember(state, ev.event_id):
        return state
    lot = state.lots.get(ev.order_id)
    if lot is None:
        if len(state.lots) >= MAX_ORDERS_CAP:
            return state
        lot = state.lots[ev.order_id] = LotRec(ev.product, ev.qty, ev.created_ts, ev.due_ts)
    else:
        lot.created_ts = _min_ts(lot.created_ts, ev.created_ts)
    if lot.priority_ts is None or ev.ts > lot.priority_ts:
        lot.priority = ev.priority
        lot.priority_ts = ev.ts
    if ev.eta_ts is not None:
        lot.eta_ts = _min_ts(lot.eta_ts, ev.eta_ts)
    if ev.station_eta is not None:
        merged = dict(lot.station_eta or {})
        for station, ts in ev.station_eta.items():
            merged[station] = _min_ts(merged.get(station), ts)
        lot.station_eta = merged
    if ev.source is not None:
        lot.source = ev.source if lot.source is None else min(lot.source, ev.source)
    if ev.state == "released":
        lot.released_ts = _min_ts(lot.released_ts, ev.ts)
    elif ev.state == "completed":
        lot.completed_ts = _min_ts(lot.completed_ts, ev.ts)
        _prune(state)
    elif ev.state == "failed":
        step = ev.step if ev.step is not None else ""
        cand = (ev.ts, step, ev.visit if ev.visit is not None else 1)
        if lot.failed is None or cand[0] < lot.failed[0]:
            lot.failed = cand
    _touch(state, ev.ts)
    return state


def _fold_priority(state: LineState, ev: PriorityChanged) -> LineState:
    """Newest ts wins; a change fact beats a lifecycle fact on an equal ts."""
    lot = state.lots.get(ev.order_id)
    if ev.order_id in state.pruned_ids or lot is None or not _remember(state, ev.event_id):
        return state
    lot.priority_marks.append((ev.ts, ev.previous_priority, ev.new_priority))
    if lot.priority_ts is None or ev.ts >= lot.priority_ts:
        lot.priority = ev.new_priority
        lot.priority_ts = ev.ts
    _touch(state, ev.ts)
    return state


def _step_rec(state: LineState, key: tuple[str, str, int]) -> StepRec | None:
    order_id, step, visit = key
    if order_id in state.pruned_ids:
        return None
    recs = state.steps.setdefault(order_id, {})
    rec = recs.get((step, visit))
    if rec is None:
        if state.step_count >= MAX_STEP_RECS:
            return None
        rec = recs[(step, visit)] = StepRec()
        state.step_count += 1
    return rec


def _fold_op(state: LineState, ev: OpDispatched | OpStarted | OpCompleted | OpFailed) -> LineState:
    if ev.step not in STATIONS:
        return state
    rec = _step_rec(state, (ev.order_id, ev.step, ev.visit))
    if rec is None:
        return state
    if isinstance(ev, OpDispatched):
        rec.dispatched_ts = _min_ts(rec.dispatched_ts, ev.ts)
    elif isinstance(ev, OpStarted):
        rec.started_ts = _min_ts(rec.started_ts, ev.ts)
    elif isinstance(ev, OpFailed):
        if rec.failed_ts is None or ev.ts < rec.failed_ts:
            rec.failed_cause = ev.cause or "quality"
        rec.failed_ts = _min_ts(rec.failed_ts, ev.ts)
    else:
        rec.completed_ts = _min_ts(rec.completed_ts, ev.ts)
        if ev.step == _LAST_STATION:
            _prune(state)
    _touch(state, ev.ts)
    return state


def _fold_status(state: LineState, ev: StationStatus) -> LineState:
    if ev.machine_id not in STATIONS or not _remember(state, ev.event_id):
        return state
    state.seq += 1
    log = state.status_logs.setdefault(ev.machine_id, [])
    log.append((ev.ts, ev.status, state.seq))
    if len(log) > MAX_STATUS_LOG:
        log.sort(key=lambda e: (e[0], e[2]))
        del log[: len(log) - MAX_STATUS_LOG]
    prev = state.station_meta.get(ev.machine_id)
    if prev is None or ev.ts >= prev[0]:
        down = ev.status in ("down", "offline")
        eta = None
        if down and ev.repair_in_s is not None:
            eta = ev.ts + timedelta(seconds=ev.repair_in_s)
        state.station_meta[ev.machine_id] = (ev.ts, ev.cause if down else None, eta)
    _touch(state, ev.ts)
    return state


def _cap_marks(marks: list[Any]) -> None:
    if len(marks) > MAX_LOT_MARKS * 2:
        marks.sort(key=lambda m: m[0])
        del marks[: len(marks) - MAX_LOT_MARKS]


def _fold_lot(
    state: LineState, ev: LotStocked | LotUnstocked | LotBanked | LotReleased | LotHold
) -> LineState:
    lot = state.lots.get(ev.order_id)
    if lot is None or not _remember(state, ev.event_id):
        return state
    if isinstance(ev, LotStocked):
        lot.stock_marks.append((ev.ts, ev.kind, ev.station))
        _cap_marks(lot.stock_marks)
    elif isinstance(ev, LotUnstocked):
        lot.stock_marks.append((ev.ts, None, ev.station))
        _cap_marks(lot.stock_marks)
    elif isinstance(ev, LotBanked):
        lot.bank_marks.append((ev.ts, True, ev.reason, ev.source, ev.station))
        _cap_marks(lot.bank_marks)
    elif isinstance(ev, LotHold):
        rank = 0 if ev.state == "requested" else 1
        lot.hold_marks.append((ev.ts, rank, ev.kind, ev.reason, ev.station, ev.state))
        _cap_marks(lot.hold_marks)
    else:
        lot.bank_marks.append((ev.ts, False, None, None, ev.station))
        _cap_marks(lot.bank_marks)
    _touch(state, ev.ts)
    return state


def _fold_pause(state: LineState, ev: RunPaused | RunResumed) -> LineState:
    if ev.event_id in state.pause_marks:
        return state
    state.pause_marks[ev.event_id] = (ev.ts, isinstance(ev, RunPaused))
    if len(state.pause_marks) > MAX_PAUSE_MARKS * 2:
        keep = sorted(state.pause_marks.items(), key=lambda kv: _pause_key(kv[1]))
        state.pause_marks = dict(keep[-MAX_PAUSE_MARKS:])
    _touch(state, ev.ts)
    return state


def _pause_key(mark: tuple[Ts, bool]) -> tuple[Ts, int]:
    # Ties resolve to "resumed" (the safe, running state).
    return (mark[0], 0 if mark[1] else 1)


def _fold_rule(state: LineState, ev: RuleChanged) -> LineState:
    """Newest ts wins; other run ids are already filtered out by ``fold``."""
    if state.rule_ts is None or ev.ts >= state.rule_ts:
        state.rule = ev.rule
        state.rule_ts = ev.ts
    _touch(state, ev.ts)
    return state


def _adopts_run(state: LineState, msg: Message) -> bool:
    """True when ``msg`` opens a newer run that this state should switch to.

    A station's idle report for a fresh run (sent on RunStarted, before any order
    exists) also opens it; otherwise that idle is dropped and the station shows
    offline until its first job arrives.
    """
    if not state.adopt:
        return False
    if isinstance(msg, OrderLifecycle) and msg.state == "created":
        ts = msg.created_ts
    elif isinstance(msg, StationStatus) and msg.run_id != "unknown":
        ts = msg.ts
    else:
        return False
    return state.run_id is None or (state.last_event_ts is not None and ts > state.last_event_ts)


def fold(state: LineState, msg: Message) -> LineState:
    """Fold one message into the state (mutates; may return a new state on adoption)."""
    if msg.run_id != state.run_id:
        if _adopts_run(state, msg):
            return fold(new_line_state(msg.run_id, adopt=state.adopt), msg)
        return state
    if isinstance(msg, OrderLifecycle):
        return _fold_lifecycle(state, msg)
    if isinstance(msg, OpDispatched | OpStarted | OpCompleted | OpFailed):
        return _fold_op(state, msg)
    if isinstance(msg, LotStocked | LotUnstocked | LotBanked | LotReleased | LotHold):
        return _fold_lot(state, msg)
    if isinstance(msg, StationStatus):
        return _fold_status(state, msg)
    if isinstance(msg, RunPaused | RunResumed):
        return _fold_pause(state, msg)
    if isinstance(msg, RuleChanged):
        return _fold_rule(state, msg)
    if isinstance(msg, PriorityChanged):
        return _fold_priority(state, msg)
    return state


# ---- derivation -------------------------------------------------------------


def _latest_bank(lot: LotRec) -> dict[str, Any] | None:
    if not lot.bank_marks:
        return None
    # Ties resolve to released (the safe state).
    ts, banked, reason, source, station = max(lot.bank_marks, key=lambda m: (m[0], not m[1]))
    if not banked:
        return None
    return {"reason": reason, "source": source, "station": station, "since_ts": _iso(ts)}


def _latest_hold(lot: LotRec) -> dict[str, Any] | None:
    """Pending deferred bank/scrap request, or None. Ties resolve to cleared."""
    if not lot.hold_marks:
        return None
    ts, rank, kind, reason, station, _state = max(lot.hold_marks, key=lambda m: (m[0], m[1]))
    if rank != 0:
        return None
    return {"kind": kind, "reason": reason, "station": station, "since_ts": _iso(ts)}


def _latest_stock(lot: LotRec) -> dict[str, Any] | None:
    if not lot.stock_marks:
        return None
    ts, kind, station = max(lot.stock_marks, key=lambda m: (m[0], m[1] is None))
    if kind is None:
        return None
    return {"kind": kind, "station": station, "since_ts": _iso(ts)}


def _presumed_cooling(state: LineState, lot: LotRec, station: str, failed_ts: Ts) -> bool:
    """True while a reworked lot is presumably still in the hold queue (D-09: no fact).

    The hold returns the lot after RETRY_TTL_MS; past that plus a margin (measured on
    the folded fact clock) the lot is treated as back in the backlog. A BANK / stocker
    fact for this station after the failure proves it is back already. This also stops
    a scrapped lot whose lifecycle ``failed`` fact was lost from reading as cooling.
    """
    if any(m[0] > failed_ts and m[-1] == station for m in (*lot.bank_marks, *lot.stock_marks)):
        return False
    now = state.last_event_ts if state.last_event_ts is not None else failed_ts
    return now - failed_ts < COOLING_WINDOW


def _effective_due(lot: LotRec, intervals: list[Interval], as_of: Ts | None) -> Ts | None:
    """Planned ``due_ts`` pushed later by pause seconds overlapping the lot's life.

    Derived only (the Kafka ``due_ts`` fact and EDD stay untouched). Wall seconds,
    no time scale: ``due_ts`` is a wall timestamp. An open pause grows with ``as_of``.
    """
    if lot.due_ts is None:
        return None
    end = lot.completed_ts if lot.completed_ts is not None else as_of
    if end is None or not intervals:
        return lot.due_ts
    if as_of is not None and end > as_of:
        end = as_of
    return lot.due_ts + timedelta(seconds=frozen_overlap(lot.created_ts, end, intervals))


def _derive_lot(
    state: LineState,
    order_id: str,
    lot: LotRec,
    intervals: list[Interval] | None = None,
    as_of: Ts | None = None,
) -> dict[str, Any]:
    if intervals is None:
        as_of = as_of if as_of is not None else state.last_event_ts
        intervals = frozen_intervals(state.pause_marks, as_of) if as_of is not None else []
    per_station: dict[str, StepRec | None] = {}
    steps_out: dict[str, dict[str, str | None]] = {}
    current: str | None = None
    done_count = 0
    has_ops = False
    rework_count = 0
    failures: list[dict[str, Any]] = []
    visits_out: list[dict[str, Any]] = []
    for station in STATIONS:
        recs = _step_recs(state, order_id, station)
        for v, r in recs:
            visits_out.append(
                {
                    "station": station,
                    "visit": v,
                    "started_ts": _iso(r.started_ts),
                    "completed_ts": _iso(r.completed_ts),
                    "failed_ts": _iso(r.failed_ts),
                }
            )
            if v > 1:
                rework_count += 1
            if r.failed_ts is not None:
                failures.append(
                    {
                        "station": station,
                        "visit": v,
                        "cause": r.failed_cause or "quality",
                        "ts": _iso(r.failed_ts),
                        "_ts": r.failed_ts,
                    }
                )
        rec = recs[-1][1] if recs else None
        per_station[station] = rec
        if rec is not None:
            has_ops = True
            steps_out[station] = {
                "started_ts": _iso(rec.started_ts),
                "completed_ts": _iso(rec.completed_ts),
            }
        if rec is not None and rec.completed_ts is not None:
            done_count += 1
        elif current is None:
            current = station
    bank = _latest_bank(lot)
    # A lot banked (or scrapped) after Package has every step completed but no
    # ``completed`` lifecycle: it is banked / failed, not done.
    done = lot.completed_ts is not None or (current is None and bank is None and lot.failed is None)
    stocker = _latest_stock(lot)
    if done:
        status, station_name = "done", None
    elif lot.failed is not None:
        status, station_name = "failed", None
    elif bank is not None:
        status, station_name = "banked", current if current is not None else _LAST_STATION
    elif lot.released_ts is None and not has_ops:
        status, station_name = "pre_release", None
    else:
        cur_rec = per_station[current] if current is not None else None
        running = cur_rec is not None and cur_rec.started_ts is not None
        status, station_name = ("running" if running else "queued"), current
    visit, dispatched, retry_since, cooling = 1, False, None, False
    if current is not None:
        recs = _step_recs(state, order_id, current)
        if recs:
            visit, cur = recs[-1]
            dispatched = cur.dispatched_ts is not None and cur.started_ts is None
            if visit > 1 and cur.started_ts is None:
                for v, prev in recs[:-1]:
                    if v == visit - 1:
                        retry_since = prev.failed_ts
            # Rework WAIT (D-09/D-11): latest visit failed and nothing re-dispatched yet.
            # The lot shows as visit+1 in the station queue. It is "cooling" (in the
            # hold queue, not BANK-able) only until the hold has had time to come back.
            if (
                cur.failed_ts is not None
                and cur.completed_ts is None
                and lot.failed is None
                and bank is None
                and not done
            ):
                visit, retry_since, dispatched = visit + 1, cur.failed_ts, False
                status = "queued"
                cooling = _presumed_cooling(state, lot, current, cur.failed_ts)
    if done or lot.failed is not None or bank is not None:
        stocker = None
    hold = None if (done or lot.failed is not None or bank is not None) else _latest_hold(lot)
    failures.sort(key=lambda f: f["_ts"])
    failures_out = [{k: v for k, v in f.items() if k != "_ts"} for f in failures]
    return {
        "order_id": order_id,
        "product": lot.product,
        "qty": lot.qty,
        "priority": lot.priority,
        "source": lot.source,
        "due_ts": _iso(lot.due_ts),
        "due_eff_ts": _iso(_effective_due(lot, intervals, as_of)),
        "created_ts": _iso(lot.created_ts),
        "completed_ts": _iso(lot.completed_ts),
        "eta_ts": _iso(lot.eta_ts),
        "station_eta": (
            {k: _iso(v) for k, v in lot.station_eta.items()}
            if lot.station_eta is not None
            else None
        ),
        "eta_error_s": (eta_error_s(lot.completed_ts, lot.eta_ts) if status == "done" else None),
        "eta_now_ts": None,
        "state": status,
        "station": station_name,
        "steps_done": done_count,
        "steps": steps_out,
        "visits": visits_out,
        "priority_changes": [
            {"ts": _iso(ts), "from": a, "to": b}
            for ts, a, b in sorted(lot.priority_marks, key=lambda m: m[0])[-MAX_LOT_FAILURES:]
        ],
        "visit": visit,
        "dispatched": dispatched,
        "cooling": cooling,
        "retry_since_ts": _iso(retry_since),
        "rework_count": rework_count,
        "failures": failures_out[-MAX_LOT_FAILURES:],
        "stocker": stocker,
        "bank": bank,
        "hold": hold,
        "failed": (
            {"station": lot.failed[1], "visit": lot.failed[2], "ts": _iso(lot.failed[0])}
            if lot.failed is not None
            else None
        ),
        "_done_ts": _lot_done_ts(state, order_id, lot),
        "_started_ts": per_station[station_name].started_ts if status == "running" else None,
    }


def lot_status(state: LineState, order_id: str) -> str | None:
    """Derived lot state (``queued``, ``done``, ``pre_release``, ...) or None if unknown."""
    lot = state.lots.get(order_id)
    return None if lot is None else _derive_lot(state, order_id, lot)["state"]


def _paused(state: LineState) -> bool:
    if not state.pause_marks:
        return False
    newest = max(state.pause_marks.values(), key=_pause_key)
    return newest[1]


LOT_DETAIL_KEYS = ("visits", "steps", "station_eta", "failures", "priority_changes")


def lot_summary(full: dict[str, Any]) -> dict[str, Any]:
    """Slim per-lot record for the stream: no heavy history, no null/False keys."""
    out = {
        k: v
        for k, v in full.items()
        if k not in LOT_DETAIL_KEYS and v is not None and v is not False
    }
    if out.get("due_eff_ts") == out.get("due_ts"):
        out.pop("due_eff_ts", None)
    station = full.get("station")
    step = (full.get("steps") or {}).get(station) if station else None
    started = step.get("started_ts") if step else None
    if started:
        out["step_started_ts"] = started
    return out


def lot_detail(state: LineState, order_id: str, as_of: Ts | None = None) -> dict[str, Any] | None:
    """Heavy per-lot history for one lot (None if unknown)."""
    lot = state.lots.get(order_id)
    if lot is None:
        return None
    if as_of is None:
        as_of = state.last_event_ts
    intervals = frozen_intervals(state.pause_marks, as_of) if as_of is not None else []
    full = _derive_lot(state, order_id, lot, intervals, as_of)
    return {"order_id": order_id, **{k: full[k] for k in LOT_DETAIL_KEYS}}


def line_snapshot(
    state: LineState, as_of: Ts | None = None, *, summary: bool = False
) -> dict[str, Any]:
    """Derived snapshot. ``as_of`` closes an open pause (default: newest fact ts)."""
    if as_of is None:
        as_of = state.last_event_ts
    intervals = frozen_intervals(state.pause_marks, as_of) if as_of is not None else []
    lots = [_derive_lot(state, oid, lot, intervals, as_of) for oid, lot in state.lots.items()]
    stations: dict[str, dict[str, Any]] = {}
    for name in STATIONS:
        log = state.status_logs.get(name)
        if log:
            ts, status, _ = max(log, key=lambda e: (e[0], e[2]))
            meta = state.station_meta.get(name)
            down = status in ("down", "offline") and meta is not None
            stations[name] = {
                "status": status,
                "queue": 0,
                "current_lot": None,
                "since_ts": _iso(ts),
                "cause": meta[1] if down else None,
                "repair_eta_ts": _iso(meta[2]) if down and meta[2] is not None else None,
                "blocked": 0,
            }
        else:
            stations[name] = {
                "status": "offline",
                "queue": 0,
                "current_lot": None,
                "since_ts": None,
                "cause": None,
                "repair_eta_ts": None,
                "blocked": 0,
            }

    running: dict[str, list[tuple[Ts, str]]] = {}
    stocker_overflow = stocker_down = 0
    for lot in lots:
        stock = lot["stocker"]
        if stock is not None and lot["state"] == "queued":
            if stock["kind"] == "overflow":
                stocker_overflow += 1
            else:
                stocker_down += 1
                if stock["station"] in stations:
                    stations[stock["station"]]["blocked"] += 1
        if lot["state"] == "queued":
            # Overflow and station_down lots are still waiting for this station,
            # so they count toward its queue badge too.
            stations[lot["station"]]["queue"] += 1
        elif lot["state"] == "running":
            running.setdefault(lot["station"], []).append((lot["_started_ts"], lot["order_id"]))
    for name, items in running.items():
        stations[name]["current_lot"] = min(items)[1]

    done = sorted(
        (lt for lt in lots if lt["state"] == "done"),
        key=lambda lt: (lt["_done_ts"] is None, lt["_done_ts"], lt["order_id"]),
    )
    completed_total = len(done) + state.pruned_completed
    active = [lt for lt in lots if lt["state"] != "done"]
    completed_pizzas = sum(lt["qty"] or 0 for lt in done) + state.pruned_completed_pizzas
    failed_pizzas = sum(lt["qty"] or 0 for lt in lots if lt["state"] == "failed")
    bank_count = sum(1 for lt in lots if lt["state"] == "banked")
    failed_count = sum(1 for lt in lots if lt["state"] == "failed")
    shown = active + done[max(0, len(done) - MAX_DONE_IN_SNAPSHOT) :]
    shown.sort(key=lambda lt: (lt["created_ts"], lt["order_id"]))
    for lt in shown:
        lt.pop("_done_ts")
        lt.pop("_started_ts")
    if summary:
        shown = [lot_summary(lt) for lt in shown]
    wip = sum(1 for lt in active if lt["state"] in ("queued", "running"))
    return {
        "run_id": state.run_id,
        "paused": _paused(state),
        "rule": state.rule,
        "stations": stations,
        "lots": shown,
        "counts": {
            "wip": wip,
            "completed": completed_total,
            "completed_pizzas": completed_pizzas,
            "failed_pizzas": failed_pizzas,
            "bank": bank_count,
            "failed": failed_count,
            "stocker_overflow": stocker_overflow,
            "stocker_down": stocker_down,
        },
    }


def active_lots(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    return [lt for lt in snapshot["lots"] if lt["state"] != "done"]


def recent_done(snapshot: dict[str, Any], n: int = 20) -> list[dict[str, Any]]:
    """Up to ``n`` most recently completed lots (newest first, tie by order_id)."""
    done = [lt for lt in snapshot["lots"] if lt["state"] == "done"]
    done.sort(key=lambda lt: lt["order_id"])
    done.sort(key=lambda lt: lt.get("completed_ts") or "", reverse=True)
    return done[:n]


_KINDS: dict[type, str] = {
    OpDispatched: "op_dispatched",
    OpStarted: "op_started",
    OpCompleted: "op_completed",
    StationStatus: "station_status",
    OrderLifecycle: "order_lifecycle",
    RunPaused: "run_paused",
    RunResumed: "run_resumed",
    OpFailed: "op_failed",
    LotStocked: "lot_stocked",
    LotUnstocked: "lot_unstocked",
    LotBanked: "lot_banked",
    LotReleased: "lot_released",
    LotHold: "lot_hold",
    RuleChanged: "rule_changed",
}


def event_payload(msg: Message) -> dict[str, Any] | None:
    """Payload for the SSE event stream; None for message types the UI ignores."""
    kind = _KINDS.get(type(msg))
    if kind is None:
        return None
    out: dict[str, Any] = {
        "kind": kind,
        "ts": _iso(msg.ts),
        "station": None,
        "order_id": None,
        "product": None,
        "qty": None,
        "step": None,
        "visit": None,
        "status": None,
        "state": None,
        "due_ts": None,
        "created_ts": None,
        "reason": None,
        "cause": None,
        "repair_in_s": None,
        "stock_kind": None,
        "source": None,
        "by": None,
        "redelivered": None,
        "rule": None,
        "hold_kind": None,
    }
    if isinstance(msg, OpDispatched | OpStarted | OpCompleted | OpFailed):
        out.update(
            station=msg.step,
            order_id=msg.order_id,
            product=msg.product,
            qty=msg.qty,
            step=msg.step,
            visit=msg.visit,
        )
        if isinstance(msg, OpStarted):
            out["redelivered"] = msg.redelivered
        elif isinstance(msg, OpFailed):
            out["reason"] = msg.reason
            out["cause"] = msg.cause or "quality"
    elif isinstance(msg, LotStocked | LotUnstocked | LotBanked | LotReleased):
        out.update(station=msg.station, order_id=msg.order_id)
        if isinstance(msg, LotStocked):
            out["stock_kind"] = msg.kind
        elif isinstance(msg, LotBanked):
            out.update(reason=msg.reason, source=msg.source)
        elif isinstance(msg, LotReleased):
            out["by"] = msg.by
    elif isinstance(msg, StationStatus):
        out.update(
            station=msg.machine_id,
            status=msg.status,
            cause=msg.cause,
            repair_in_s=msg.repair_in_s,
        )
    elif isinstance(msg, OrderLifecycle):
        out.update(
            order_id=msg.order_id,
            product=msg.product,
            qty=msg.qty,
            state=msg.state,
            due_ts=_iso(msg.due_ts),
            created_ts=_iso(msg.created_ts),
        )
        if msg.state == "failed":
            out.update(step=msg.step, visit=msg.visit)
    elif isinstance(msg, LotHold):
        out.update(
            station=msg.station,
            order_id=msg.order_id,
            state=msg.state,
            reason=msg.reason,
            hold_kind=msg.kind,
        )
    elif isinstance(msg, RuleChanged):
        out["rule"] = msg.rule
    return out
