"""Pure scheduler state machine (SCHED-01/02, STATION-05, D-11..D-18, Phase 6 D-07..D-14).

No broker or asyncio imports: the shell (plan 03-05) feeds events in and
performs the returned actions. Per station there is one FIFO backlog and at
most one outstanding job (credit = 1, D-12). The station credit is returned on
``op.completed`` and on every ``op.failed``. Only an ``op.completed`` releases
an order to the next station (D-11/D-13); an ``op.failed`` either dead-letters
the order or sends it to rework: the lot cools outside the backlog until the
hold queue returns it (``on_rework_ready``) and then re-enters the backlog.
"""

import math
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from pizza.common.constants import PRIORITY_DEFAULT, STATIONS
from pizza.common.dedupe import Deduper
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
    StationHeartbeat,
    StationStatus,
)
from pizza.common.ids import job_event_id
from pizza.common.topology import RETRY_TTL_MS
from pizza.scheduler import rework as rw
from pizza.scheduler import stocker as sk


@dataclass(frozen=True)
class CoreConfig:
    rework_limits: Mapping[str, int] = field(default_factory=lambda: dict(rw.DEFAULT_REWORK_LIMITS))
    queue_limit: int | None = None
    bank_after_s: float = 60.0
    bank_wait_threshold: float | None = None
    recipes: Mapping[str, Mapping[str, float]] = field(default_factory=dict)
    # 3 x RETRY_TTL_MS: sweep fallback if the rework hold message is lost (D-13).
    # Derived from the hold-queue TTL so the sweep never beats the TTL hop.
    rework_hold_grace_s: float = 3 * RETRY_TTL_MS / 1000


@dataclass(frozen=True)
class OrderRecord:
    order_id: str
    product: str
    qty: int
    due_ts: datetime
    created_ts: datetime
    fail_step: str | None = None
    fail_times: int | None = None
    poison: bool = False
    priority: int = PRIORITY_DEFAULT
    eta_ts: datetime | None = None
    station_eta: Mapping[str, datetime] | None = None


@dataclass(frozen=True)
class BacklogEntry:
    order_id: str
    step_index: int
    entered_ts: datetime
    visit: int = 1
    # Time the lot became ready to run; ordering uses entered_ts only (D-05).
    # None means same as entered_ts.
    ready_ts: datetime | None = None


@dataclass(frozen=True)
class CoolingRec:
    """A reworked lot waiting outside the backlog for its hold message (D-06)."""

    station: str
    step_index: int
    visit: int
    entered_ts: datetime
    failed_ts: datetime
    # When the scheduler started the cooling (handled op_failed). The sweep grace
    # counts from max(failed_ts, cooling_since) so a lagging scheduler still gives
    # the hold queue its full TTL. None = failed_ts.
    cooling_since: datetime | None = None


@dataclass(frozen=True)
class HoldRecord:
    """A pending deferred manual bank / scrap request (quick 261023-pfp).

    ``step`` / ``visit`` identify the outstanding job the request is waiting on.
    """

    kind: Literal["bank", "scrap"]
    reason: str
    step: str
    visit: int
    since: datetime


@dataclass(frozen=True)
class OrderContext:
    """Context for rule-aware ordering (D-01..D-03)."""
    due_ts: datetime
    priority: int
    proc_s: float


@dataclass(frozen=True)
class EmitLifecycle:
    state: str
    order_id: str
    ts: datetime
    step: str | None = None
    visit: int | None = None


@dataclass(frozen=True)
class Dispatch:
    order_id: str
    step: str
    visit: int
    product: str
    qty: int
    ready_ts: datetime
    command_event_id: Any


@dataclass(frozen=True)
class PublishRework:
    order_id: str
    step: str
    visit: int
    product: str
    qty: int
    entered_ts: datetime
    command_event_id: Any
    failed_visit: int
    failed_ts: datetime


@dataclass(frozen=True)
class PublishDeadLetter:
    order_id: str
    step: str
    visit: int
    product: str
    qty: int
    command_event_id: Any
    reason: str


@dataclass(frozen=True)
class EmitStationStatus:
    station: str
    status: str
    ts: datetime


@dataclass(frozen=True)
class EmitLot:
    fact: Literal["stocked", "unstocked", "banked", "released"]
    order_id: str
    station: str
    ts: datetime
    kind: str | None = None
    reason: str | None = None
    source: str | None = None
    by: str | None = None


@dataclass(frozen=True)
class EmitHold:
    state: Literal["requested", "cancelled", "applied"]
    order_id: str
    station: str
    ts: datetime
    kind: Literal["bank", "scrap"]
    reason: str


Action = (
    EmitLifecycle
    | Dispatch
    | PublishRework
    | PublishDeadLetter
    | EmitStationStatus
    | EmitLot
    | EmitHold
)


def ordering_key(
    rule: str, entry: BacklogEntry, ctx: OrderContext
) -> tuple[int, float, datetime, str]:
    """Pure ordering key supporting FIFO/EDD/SPT with priority-first and deterministic
    tie-breaking (07.2 D-01..D-03).

    Returns (priority, rule_key, entered_ts, order_id) where:
    - Smaller priority numbers (more urgent) always sort ahead
    - rule_key is 0.0 for "fifo", ctx.due_ts.timestamp() for "edd", ctx.proc_s for "spt"
    - Tie-break by entered_ts, then order_id
    - Unknown rule raises ValueError
    """
    if rule == "fifo":
        rule_key = 0.0
    elif rule == "edd":
        rule_key = ctx.due_ts.timestamp()
    elif rule == "spt":
        rule_key = ctx.proc_s
    else:
        raise ValueError(f"unknown dispatch rule {rule!r}")
    return (ctx.priority, rule_key, entry.entered_ts, entry.order_id)


def _entry_key(entry: BacklogEntry) -> tuple[datetime, str]:
    return (entry.entered_ts, entry.order_id)


def select_next(
    backlog: Sequence[BacklogEntry], key: Callable[[BacklogEntry], Any] | None = None
) -> BacklogEntry:
    """Pick the next entry to dispatch (D-15): FIFO by (entered_ts, order_id) or custom key.

    Isolated at module level so Phase 7 can swap in EDD / SPT via the key parameter.
    """
    return min(backlog, key=key or _entry_key)


@dataclass
class BankRecord:
    entry: BacklogEntry
    station: str
    reason: str
    source: str
    since: datetime


@dataclass
class _OrderState:
    record: OrderRecord
    next_index: int = 0
    emitted: set[str] = field(default_factory=set)


@dataclass
class _Health:
    # None = never reported. An unreported station is not dispatchable (no work
    # goes to RabbitMQ until it reports idle or busy).
    reported: str | None = None
    reported_ts: datetime | None = None
    offline: bool = False
    unavailable_since: datetime | None = None
    # When a down station is expected back (status ts + repair_in_s); None = unknown.
    repair_until: datetime | None = None


class SchedulerCore:
    """Live state: backlogs, credits (outstanding), orders, health, dedupers."""

    def __init__(self, route: tuple[str, ...] = STATIONS, config: CoreConfig | None = None) -> None:
        self.route = route
        self.config = config if config is not None else CoreConfig()
        self.current_run_id: str | None = None
        self.paused: bool = False
        self.rule: str = "edd"  # default dispatch rule; SetRule overrides it, dispatch_ready applies it
        self._reset()

    def _reset(self) -> None:
        self.orders: dict[str, _OrderState] = {}
        self.backlog: dict[str, list[BacklogEntry]] = {s: [] for s in self.route}
        self.outstanding: dict[str, BacklogEntry | None] = dict.fromkeys(self.route)
        # When the outstanding job of a station started (op.started ts); None = unknown.
        self.outstanding_started: dict[str, datetime | None] = dict.fromkeys(self.route)
        self._started = Deduper()
        self._completed = Deduper()
        self._dispatched = Deduper()
        self._failed = Deduper()
        self.health: dict[str, _Health] = {s: _Health() for s in self.route}
        self.banked: dict[str, BankRecord] = {}
        self.stock_kind: dict[str, tuple[str, str]] = {}
        self.cooling: dict[str, CoolingRec] = {}
        # Deferred manual bank / scrap requests waiting for the current op to end.
        self.holds: dict[str, HoldRecord] = {}

    def start_run(self, run_id: str) -> None:
        self.current_run_id = run_id
        self.paused = False
        self._reset()

    def set_rule(self, rule: str) -> None:
        """Change the active dispatch rule (D-04). Affects only next dispatch, not outstanding."""
        if rule not in ("fifo", "edd", "spt"):
            raise ValueError(f"unknown dispatch rule {rule!r}")
        self.rule = rule

    def _ctx(self, station: str, entry: BacklogEntry) -> OrderContext:
        """Compute processing context for rule-aware ordering (D-01)."""
        rec = self.orders[entry.order_id].record
        recipes = self.config.recipes or {}
        station_recipes = recipes.get(station, {})
        proc_s = station_recipes.get(rec.product, math.inf) * rec.qty
        return OrderContext(due_ts=rec.due_ts, priority=rec.priority, proc_s=proc_s)

    def _rule_key(self, station: str) -> Callable[[BacklogEntry], Any]:
        """Return a key function for rule-aware select_next at a given station."""
        return lambda e: ordering_key(self.rule, e, self._ctx(station, e))

    def is_terminal(self, order_id: str) -> bool:
        """True when the order failed or completed (no longer reorderable)."""
        st = self.orders.get(order_id)
        return st is not None and bool({"failed", "completed"} & st.emitted)

    def set_priority(self, order_id: str, new: int) -> bool:
        """Change an unfinished order's priority (07.2 D-04/D-10).

        Returns False for unknown or terminal orders. Backlog order is computed on
        demand from ordering_key, so the next settle() sees the new priority;
        outstanding jobs are never re-dispatched or reordered.
        """
        st = self.orders.get(order_id)
        if st is None or self.is_terminal(order_id):
            return False
        st.record = replace(st.record, priority=new)
        return True

    # -- intake ---------------------------------------------------------
    def on_order(self, record: OrderRecord, now: datetime) -> list[Action]:
        if record.order_id in self.orders:
            return []
        state = _OrderState(record=record)
        state.emitted.add("released")
        self.orders[record.order_id] = state
        actions: list[Action] = [EmitLifecycle("released", record.order_id, now)]
        actions.extend(
            self._enter_backlog(self.route[0], BacklogEntry(record.order_id, 0, now), now)
        )
        actions.extend(self.settle(now))
        return actions

    def _enter_backlog(self, station: str, entry: BacklogEntry, now: datetime) -> list[Action]:
        """Append to the station backlog, or auto-bank when the WAIT threshold is hit (D-20)."""
        threshold = self.config.bank_wait_threshold
        if sk.should_wait_bank(len(self.backlog[station]), threshold):  # type: ignore[arg-type]
            return self._bank_entry(entry, station, "capacity", "auto_wait", now)
        self.backlog[station].append(entry)
        return []

    def _bank_entry(
        self, entry: BacklogEntry, station: str, reason: str, source: str, now: datetime
    ) -> list[Action]:
        self.banked[entry.order_id] = BankRecord(entry, station, reason, source, now)
        self.stock_kind.pop(entry.order_id, None)  # BANK supersedes stocked
        return [EmitLot("banked", entry.order_id, station, now, reason=reason, source=source)]

    # -- health ---------------------------------------------------------
    def dispatchable(self, station: str) -> bool:
        h = self.health.get(station)
        # Needs an explicit idle/busy report; a never-reported station (None) is not dispatchable.
        return h is not None and h.reported in ("idle", "busy") and not h.offline

    def _availability(self, station: str) -> sk.Availability:
        h = self.health[station]
        if h.offline or h.reported == "offline":
            return "offline"
        if h.reported == "down":
            return "down"
        return "ok"

    def _availability_changed(self, station: str, ts: datetime) -> list[Action]:
        h = self.health[station]
        was_unavailable = h.unavailable_since is not None
        if not self.dispatchable(station):
            if not was_unavailable:
                h.unavailable_since = ts
            return list(self.reclassify(ts))
        if was_unavailable:
            h.unavailable_since = None
            actions: list[Action] = list(self._release_auto(station, "auto_down", ts))
            actions.extend(self.settle(ts))
            return actions
        return []

    def on_station_status(self, ev: StationStatus | StationHeartbeat) -> list[Action]:
        if self.current_run_id is not None and ev.run_id not in (self.current_run_id, "unknown"):
            return []
        station = ev.machine_id
        h = self.health.get(station)
        if h is None:
            return []
        if h.reported_ts is not None and ev.ts < h.reported_ts:
            return []
        first_report = h.reported_ts is None
        was_dispatchable = self.dispatchable(station)
        h.reported = ev.status
        h.reported_ts = ev.ts
        if ev.status != "down":
            h.repair_until = None
        elif isinstance(ev, StationStatus) and ev.repair_in_s is not None:
            h.repair_until = ev.ts + timedelta(seconds=ev.repair_in_s)
        actions = self._availability_changed(station, ev.ts)
        # First report from a never-reported station: backlog queued while it was
        # unknown has not been settled yet, so dispatch it now.
        if first_report and not was_dispatchable and self.dispatchable(station):
            actions.extend(self.settle(ev.ts))
        return actions

    def set_offline(self, station: str, offline: bool, now: datetime) -> list[Action]:
        h = self.health.get(station)
        if h is None or h.offline == offline:
            return []
        h.offline = offline
        actions: list[Action] = []
        if offline:
            actions.append(EmitStationStatus(station, "offline", now))
        elif h.reported is not None:
            # Never echo an idle on behalf of a station that has never reported:
            # the echo would re-enter on_station_status and make it dispatchable.
            status = h.reported if h.reported in ("idle", "busy", "down") else "idle"
            actions.append(EmitStationStatus(station, status, now))
        actions.extend(self._availability_changed(station, now))
        return actions

    def fail_flags(self, order_id: str, step: str) -> tuple[int | None, bool]:
        st = self.orders.get(order_id)
        if st is None or st.record.fail_step != step:
            return (None, False)
        return (st.record.fail_times, st.record.poison)

    # -- facts ----------------------------------------------------------
    def _relevant(self, ev: OpStarted | OpCompleted | OpFailed) -> _OrderState | None:
        if self.current_run_id is not None and ev.run_id != self.current_run_id:
            return None
        if ev.step not in self.route:
            return None
        return self.orders.get(ev.order_id)

    def on_op_started(self, ev: OpStarted) -> list[Action]:
        state = self._relevant(ev)
        if state is None or not self._started.first_time_op(ev):
            return []
        out = self.outstanding[ev.step]
        if out is not None and out.order_id == ev.order_id and out.visit == ev.visit:
            self.outstanding_started[ev.step] = ev.ts
        if ev.step == self.route[0] and "in_process" not in state.emitted:
            state.emitted.add("in_process")
            return [EmitLifecycle("in_process", ev.order_id, ev.ts)]
        return []

    def on_op_completed(self, ev: OpCompleted) -> list[Action]:
        state = self._relevant(ev)
        if state is None or not self._completed.first_time_op(ev):
            return []
        idx = self.route.index(ev.step)
        out = self.outstanding[ev.step]
        if (
            out is None
            or out.order_id != ev.order_id
            or out.step_index != idx
            or out.visit != ev.visit
        ):
            return []
        self.outstanding[ev.step] = None
        self.outstanding_started[ev.step] = None
        state.next_index = idx + 1
        actions: list[Action] = []
        hold = self.holds.pop(ev.order_id, None)
        if hold is not None:
            # A manual deferred request wins over auto_wait banking and, at the
            # last station, over the ``completed`` lifecycle (D-B).
            if idx + 1 < len(self.route):
                station, entry = self.route[idx + 1], BacklogEntry(ev.order_id, idx + 1, ev.ts)
            else:
                station = self.route[-1]
                entry = BacklogEntry(ev.order_id, len(self.route), ev.ts)
            actions.extend(self._apply_hold(hold, entry, station, ev.ts))
        elif idx + 1 < len(self.route):
            actions.extend(
                self._enter_backlog(
                    self.route[idx + 1], BacklogEntry(ev.order_id, idx + 1, ev.ts), ev.ts
                )
            )
        elif "completed" not in state.emitted:
            state.emitted.add("completed")
            actions.append(EmitLifecycle("completed", ev.order_id, ev.ts))
        actions.extend(self.settle(ev.ts))
        return actions

    def on_op_failed(self, ev: OpFailed, now: datetime | None = None) -> list[Action]:
        state = self._relevant(ev)
        if state is None or "failed" in state.emitted:
            return []
        out = self.outstanding[ev.step]
        if out is None or out.order_id != ev.order_id or out.visit != ev.visit:
            return []
        if not self._failed.first_time_op(ev):
            return []
        rec = state.record
        run_id = self.current_run_id or ""
        decision = rw.decide(ev.step, ev.visit, self.config.rework_limits)
        hold = self.holds.pop(ev.order_id, None)
        if isinstance(decision, rw.Rework):
            nxt = decision.next_visit
            self.outstanding[ev.step] = None
            self.outstanding_started[ev.step] = None
            if hold is not None:
                # The lot is now held by the scheduler: apply the deferred request
                # instead of cooling (never silently lost).
                entry = BacklogEntry(
                    ev.order_id, out.step_index, out.entered_ts, nxt, ready_ts=ev.ts
                )
                apply_ts = max(now or ev.ts, ev.ts)
                actions_h = self._apply_hold(hold, entry, ev.step, apply_ts)
                actions_h.extend(self.settle(apply_ts))
                return actions_h
            self.cooling[ev.order_id] = CoolingRec(
                ev.step, out.step_index, nxt, out.entered_ts, ev.ts, cooling_since=now
            )
            actions_r: list[Action] = [
                PublishRework(
                    order_id=ev.order_id,
                    step=ev.step,
                    visit=nxt,
                    product=rec.product,
                    qty=rec.qty,
                    entered_ts=out.entered_ts,
                    command_event_id=job_event_id(run_id, ev.order_id, ev.step, nxt),
                    failed_visit=ev.visit,
                    failed_ts=ev.ts,
                )
            ]
            actions_r.extend(self.settle(ev.ts))
            return actions_r
        self.outstanding[ev.step] = None
        self.outstanding_started[ev.step] = None
        state.emitted.add("failed")
        actions: list[Action] = []
        if hold is not None:
            actions.append(
                EmitHold("cancelled", ev.order_id, ev.step, now or ev.ts, hold.kind, hold.reason)
            )
        actions += [
            PublishDeadLetter(
                order_id=ev.order_id,
                step=ev.step,
                visit=ev.visit,
                product=rec.product,
                qty=rec.qty,
                command_event_id=job_event_id(run_id, ev.order_id, ev.step, ev.visit),
                reason=decision.reason,
            ),
            EmitLifecycle("failed", ev.order_id, ev.ts, step=ev.step, visit=ev.visit),
        ]
        actions.extend(self.settle(ev.ts))
        return actions

    def on_rework_ready(
        self, order_id: str, step: str, visit: int, run_id: str, now: datetime
    ) -> list[Action]:
        """A rework hold message came back: re-enter the cooling lot (D-04)."""
        if run_id != self.current_run_id:
            return []
        rec = self.cooling.get(order_id)
        if rec is None or rec.station != step or rec.visit != visit:
            return []
        state = self.orders.get(order_id)
        if state is None or {"failed", "completed"} & state.emitted:
            return []
        return self._return_from_cooling(order_id, now)

    def _return_from_cooling(self, order_id: str, now: datetime) -> list[Action]:
        rec = self.cooling.pop(order_id)
        entry = BacklogEntry(
            order_id, rec.step_index, rec.entered_ts, rec.visit, ready_ts=rec.failed_ts
        )
        actions = self._enter_backlog(rec.station, entry, now)  # D-07
        actions.extend(self.settle(now))
        return actions

    # -- stocker / BANK ---------------------------------------------------
    def reclassify(self, ts: datetime) -> list[EmitLot]:
        """Diff stocker classification of every backlog against ``stock_kind``."""
        want: dict[str, tuple[str, str]] = {}
        for station in self.route:
            # Use rule-aware ordering for stocker classification (Pitfall 4)
            ids = [e.order_id for e in sorted(self.backlog[station], key=self._rule_key(station))]
            kinds = sk.classify_backlog(ids, self._availability(station), self.config.queue_limit)
            for oid in ids:
                if oid in kinds:
                    want[oid] = (station, kinds[oid])
        out: list[EmitLot] = []
        order = {s: i for i, s in enumerate(self.route)}
        gone = sorted(
            (order.get(st, len(order)), oid, st)
            for oid, (st, _k) in self.stock_kind.items()
            if oid not in want or want[oid][0] != st
        )
        for _i, oid, st in gone:
            out.append(EmitLot("unstocked", oid, st, ts))
            del self.stock_kind[oid]
        for station in self.route:
            for e in sorted(self.backlog[station], key=self._rule_key(station)):
                w = want.get(e.order_id)
                if w is not None and w[0] == station and self.stock_kind.get(e.order_id) != w:
                    out.append(EmitLot("stocked", e.order_id, station, ts, kind=w[1]))
                    self.stock_kind[e.order_id] = w
        return out

    def settle(self, now: datetime) -> list[Action]:
        actions: list[Action] = list(self.dispatch_ready())
        released = self._auto_release_wait(now)
        if released:
            actions.extend(released)
            actions.extend(self.dispatch_ready())
        actions.extend(self.reclassify(now))
        return actions

    def _find_entry(self, order_id: str) -> tuple[str, BacklogEntry] | None:
        for station in self.route:
            for e in self.backlog[station]:
                if e.order_id == order_id:
                    return station, e
        return None

    def _outstanding_at(self, order_id: str) -> tuple[str, BacklogEntry] | None:
        for station in self.route:
            out = self.outstanding[station]
            if out is not None and out.order_id == order_id:
                return station, out
        return None

    def _request_hold(
        self, kind: Literal["bank", "scrap"], order_id: str, reason: str, now: datetime
    ) -> list[Action]:
        """Record a deferred request on a dispatched / running lot (D-A)."""
        if order_id in self.holds:
            return []  # same kind = idempotent, other kind rejected: one hold per lot
        found = self._outstanding_at(order_id)
        if found is None:
            return []
        station, out = found
        self.holds[order_id] = HoldRecord(kind, reason, station, out.visit, now)
        return [EmitHold("requested", order_id, station, now, kind, reason)]

    def cancel_hold(self, order_id: str, now: datetime) -> list[Action]:
        """Withdraw a pending deferred request."""
        hold = self.holds.pop(order_id, None)
        if hold is None:
            return []
        return [EmitHold("cancelled", order_id, hold.step, now, hold.kind, hold.reason)]

    def _apply_hold(
        self, hold: HoldRecord, entry: BacklogEntry, station: str, now: datetime
    ) -> list[Action]:
        """Apply a deferred request to a lot the scheduler now holds (no settle)."""
        actions: list[Action] = [
            EmitHold("applied", entry.order_id, station, now, hold.kind, hold.reason)
        ]
        if hold.kind == "bank":
            actions.extend(self._bank_entry(entry, station, hold.reason, "manual", now))
        else:
            actions.extend(
                self._scrap_entry(self.orders[entry.order_id], station, entry, hold.reason, now)
            )
        return actions

    def _scrap_entry(
        self, st: _OrderState, station: str, entry: BacklogEntry, reason: str, now: datetime
    ) -> list[Action]:
        """Dead-letter the job and emit the ``failed`` lifecycle (like rework exhausted)."""
        rec = st.record
        order_id = entry.order_id
        run_id = self.current_run_id or ""
        st.emitted.add("failed")
        return [
            PublishDeadLetter(
                order_id=order_id,
                step=station,
                visit=entry.visit,
                product=rec.product,
                qty=rec.qty,
                command_event_id=job_event_id(run_id, order_id, station, entry.visit),
                reason=f"manual_scrap:{reason}",
            ),
            EmitLifecycle("failed", order_id, now, step=station, visit=entry.visit),
        ]

    def _cooling_entry(self, order_id: str) -> tuple[str, BacklogEntry] | None:
        rec = self.cooling.pop(order_id, None)
        if rec is None:
            return None
        entry = BacklogEntry(
            order_id, rec.step_index, rec.entered_ts, rec.visit, ready_ts=rec.failed_ts
        )
        return rec.station, entry

    def bank(self, order_id: str, reason: str, now: datetime) -> list[Action]:
        """Manual bank. WAIT and cooling lots bank now; dispatched / running lots
        record a deferred request (applied when the op ends). Terminal, banked,
        pre-release and unknown lots are ignored."""
        st = self.orders.get(order_id)
        if st is None or order_id in self.banked or {"failed", "completed"} & st.emitted:
            return []
        found = self._find_entry(order_id)
        if found is None:
            cooled = self._cooling_entry(order_id)
            if cooled is None:
                return self._request_hold("bank", order_id, reason, now)
            found = cooled
        else:
            self.backlog[found[0]].remove(found[1])
        station, entry = found
        actions = self._bank_entry(entry, station, reason, "manual", now)
        actions.extend(self.settle(now))
        return actions

    def scrap(self, order_id: str, reason: str, now: datetime) -> list[Action]:
        """Manual scrap; ends the order as ``failed``.

        Mirrors the rework-exhausted branch of ``on_op_failed``: dead-letter the job,
        emit the ``failed`` lifecycle fact. WAIT, BANK and cooling lots scrap now;
        dispatched / running lots record a deferred request. Terminal and unknown
        lots are ignored (empty action list).
        """
        st = self.orders.get(order_id)
        if st is None or {"failed", "completed"} & st.emitted:
            return []
        brec = self.banked.get(order_id)
        if brec is not None:
            del self.banked[order_id]
            station, entry = brec.station, brec.entry
        else:
            found = self._find_entry(order_id)
            if found is not None:
                station, entry = found
                self.backlog[station].remove(entry)
            else:
                cooled = self._cooling_entry(order_id)
                if cooled is None:
                    return self._request_hold("scrap", order_id, reason, now)
                station, entry = cooled
        self.holds.pop(order_id, None)
        actions = self._scrap_entry(st, station, entry, reason, now)
        actions.extend(self.settle(now))  # also emits "unstocked" for a stocked lot
        return actions

    def release(self, order_id: str, now: datetime) -> list[Action]:
        """Manual release; only manually banked lots leave BANK this way."""
        rec = self.banked.get(order_id)
        if rec is None or rec.source != "manual":
            return []
        del self.banked[order_id]
        actions: list[Action] = [EmitLot("released", order_id, rec.station, now, by="manual")]
        if rec.entry.step_index >= len(self.route):
            # Banked after the last station: releasing ships the lot (D-B).
            st = self.orders[order_id]
            if "completed" not in st.emitted:
                st.emitted.add("completed")
                actions.append(EmitLifecycle("completed", order_id, now))
        else:
            self.backlog[rec.station].append(rec.entry)
        actions.extend(self.settle(now))
        return actions

    def _release_auto(
        self, station: str, source: str, now: datetime, limit: int | None = None
    ) -> list[EmitLot]:
        recs = sorted(
            (r for r in self.banked.values() if r.station == station and r.source == source),
            key=lambda r: _entry_key(r.entry),
        )
        if limit is not None:
            recs = recs[:limit]
        out: list[EmitLot] = []
        for r in recs:
            del self.banked[r.entry.order_id]
            self.backlog[station].append(r.entry)
            out.append(EmitLot("released", r.entry.order_id, station, now, by="auto"))
        return out

    def _auto_release_wait(self, now: datetime) -> list[EmitLot]:
        threshold = self.config.bank_wait_threshold
        if threshold is None:
            return []
        out: list[EmitLot] = []
        for station in self.route:
            n_banked = sum(
                1 for r in self.banked.values() if r.station == station and r.source == "auto_wait"
            )
            k = sk.wait_release_count(n_banked, len(self.backlog[station]), threshold)  # type: ignore[arg-type]
            if k > 0:
                out.extend(self._release_auto(station, "auto_wait", now, k))
        return out

    def sweep(self, now: datetime) -> list[Action]:
        """Time-driven BANK moves: long outage -> auto_down, wait threshold release."""
        actions: list[Action] = []
        for station in self.route:
            h = self.health[station]
            if h.unavailable_since is None:
                continue
            ids = [e.order_id for e in sorted(self.backlog[station], key=_entry_key)]
            due = sk.due_for_down_bank(ids, h.unavailable_since, now, self.config.bank_after_s)
            by_id = {e.order_id: e for e in self.backlog[station]}
            for oid in due:
                entry = by_id[oid]
                self.backlog[station].remove(entry)
                actions.extend(self._bank_entry(entry, station, "capacity", "auto_down", now))
        for oid, crec in sorted(self.cooling.items()):
            since = max(crec.failed_ts, crec.cooling_since or crec.failed_ts)
            if (now - since).total_seconds() > self.config.rework_hold_grace_s:
                actions.extend(self._return_from_cooling(oid, now))
        actions.extend(self.settle(now))
        return actions

    # -- dispatch -------------------------------------------------------
    def dispatch_ready(self) -> list[Dispatch]:
        if self.paused:
            return []
        out: list[Dispatch] = []
        run_id = self.current_run_id or ""
        for station in self.route:
            queue = self.backlog[station]
            if self.outstanding[station] is not None or not queue:
                continue
            if not self.dispatchable(station):
                continue
            entry = select_next(queue, key=self._rule_key(station))
            queue.remove(entry)
            self.outstanding[station] = entry
            rec = self.orders[entry.order_id].record
            out.append(
                Dispatch(
                    order_id=entry.order_id,
                    step=station,
                    visit=entry.visit,
                    product=rec.product,
                    qty=rec.qty,
                    ready_ts=entry.ready_ts or entry.entered_ts,
                    command_event_id=job_event_id(run_id, entry.order_id, station, entry.visit),
                )
            )
        return out

    def release_dispatch(self, station: str) -> None:
        """Undo an in-memory dispatch whose publish failed: restore backlog + credit."""
        entry = self.outstanding.get(station)
        if entry is None:
            return
        self.outstanding[station] = None
        self.outstanding_started[station] = None
        self.backlog[station].append(entry)

    def rollback_order(self, order_id: str) -> None:
        """Forget an order whose intake could not be completed."""
        self.orders.pop(order_id, None)
        self.banked.pop(order_id, None)
        self.stock_kind.pop(order_id, None)
        self.cooling.pop(order_id, None)
        self.holds.pop(order_id, None)
        for station in self.route:
            self.backlog[station] = [e for e in self.backlog[station] if e.order_id != order_id]
            out = self.outstanding[station]
            if out is not None and out.order_id == order_id:
                self.outstanding[station] = None
                self.outstanding_started[station] = None

    # -- introspection --------------------------------------------------
    def backlog_lengths(self) -> dict[str, int]:
        return {s: len(self.backlog[s]) for s in self.route}

    def snapshot(self) -> dict[str, Any]:
        return {
            "run_id": self.current_run_id,
            "backlog": {
                s: [e.order_id for e in sorted(self.backlog[s], key=self._rule_key(s))]
                for s in self.route
            },
            "outstanding": {
                s: (o.order_id if o is not None else None) for s, o in self.outstanding.items()
            },
            "orders": {
                oid: (st.next_index, frozenset(st.emitted))
                for oid, st in sorted(self.orders.items())
            },
            "health": {s: (h.reported, h.offline) for s, h in self.health.items()},
            "banked": {oid: r.reason for oid, r in sorted(self.banked.items())},
            "stock": {oid: k for oid, (_s, k) in sorted(self.stock_kind.items())},
            "cooling": {oid: (r.station, r.visit) for oid, r in sorted(self.cooling.items())},
            "holds": {oid: (h.kind, h.step) for oid, h in sorted(self.holds.items())},
        }

    # -- rebuild (D-14) -------------------------------------------------
    @classmethod
    def rebuild(
        cls,
        run_id: str,
        facts: Iterable[Message],
        route: tuple[str, ...] = STATIONS,
        config: CoreConfig | None = None,
        now: datetime | None = None,
    ) -> tuple["SchedulerCore", list[Action]]:
        """Derive state from the SET of facts; input order never matters.

        Lot facts (LotBanked/LotReleased, LotStocked/LotUnstocked) resolve per order
        by greatest ts, ties going to the safe state (released / unstocked). BANK
        records and stocker kinds are restored, then auto_down releases for currently
        dispatchable stations and ``reclassify`` corrections are appended to the
        reconcile list. ``now`` defaults to the newest fact ts (never wall-clock).
        """
        core = cls(route, config)
        core.start_run(run_id)

        records: dict[str, OrderRecord] = {}
        lifecycle_states: dict[str, set[str]] = {}
        lifecycle_ts: dict[str, dict[str, datetime]] = {}
        dispatched: dict[tuple[str, str, int], datetime] = {}
        started: dict[tuple[str, str, int], datetime] = {}
        eta_best: dict[str, tuple[datetime, Mapping[str, datetime] | None]] = {}
        failed: dict[tuple[str, str, int], datetime] = {}
        completed: dict[tuple[str, str], datetime] = {}
        seen_started: list[OpStarted] = []
        seen_completed: list[OpCompleted] = []
        seen_dispatched: list[OpDispatched] = []
        seen_failed: list[OpFailed] = []
        status_best: dict[str, StationStatus | StationHeartbeat] = {}
        newest_pause: tuple[datetime, bool] | None = None  # (ts, is_resumed)
        newest_rule: tuple[datetime, str] | None = None  # (ts, rule_name) for rule restore (D-04)
        # per order: (ts, kind_rank, event_id, priority); change fact outranks lifecycle on ties
        prio_marks: dict[str, list[tuple[datetime, int, str, int]]] = {}
        bank_marks: dict[str, list[tuple[datetime, int, str, str, str]]] = {}
        stock_marks: dict[str, list[tuple[datetime, int, str, str]]] = {}
        # (ts, rank, kind, reason, station, state); rank 0 requested, 1 cancelled/applied
        hold_marks: dict[str, list[tuple[datetime, int, str, str, str, str]]] = {}
        max_ts: datetime | None = None

        for f in facts:
            if getattr(f, "run_id", None) in (run_id, "unknown"):
                max_ts = f.ts if max_ts is None or f.ts > max_ts else max_ts
            if isinstance(f, StationStatus | StationHeartbeat):
                if f.run_id in (run_id, "unknown") and f.machine_id in route:
                    best = status_best.get(f.machine_id)
                    if best is None or f.ts > best.ts:
                        status_best[f.machine_id] = f
                continue
            if getattr(f, "run_id", None) != run_id:
                continue
            if isinstance(f, LotBanked):
                bank_marks.setdefault(f.order_id, []).append(
                    (f.ts, 0, f.reason, f.source, f.station)
                )
                continue
            if isinstance(f, LotReleased):
                bank_marks.setdefault(f.order_id, []).append((f.ts, 1, "", f.by, f.station))
                continue
            if isinstance(f, LotHold):
                hold_marks.setdefault(f.order_id, []).append(
                    (
                        f.ts,
                        0 if f.state == "requested" else 1,
                        f.kind,
                        f.reason,
                        f.station,
                        f.state,
                    )
                )
                continue
            if isinstance(f, LotStocked):
                stock_marks.setdefault(f.order_id, []).append((f.ts, 0, f.kind, f.station))
                continue
            if isinstance(f, LotUnstocked):
                stock_marks.setdefault(f.order_id, []).append((f.ts, 1, "", f.station))
                continue
            if isinstance(f, RunPaused | RunResumed):
                cand = (f.ts, isinstance(f, RunResumed))
                if newest_pause is None or cand > newest_pause:
                    newest_pause = cand
                continue
            if isinstance(f, RuleChanged):
                # Track the newest RuleChanged by ts to restore the active rule (D-04)
                if newest_rule is None or f.ts > newest_rule[0]:
                    newest_rule = (f.ts, f.rule)
                continue
            if isinstance(f, PriorityChanged):
                prio_marks.setdefault(f.order_id, []).append(
                    (f.ts, 1, str(f.event_id), f.new_priority)
                )
                continue
            if isinstance(f, OrderLifecycle):
                prio_marks.setdefault(f.order_id, []).append(
                    (f.ts, 0, str(f.event_id), f.priority)
                )
                if f.eta_ts is not None:
                    cur_eta = eta_best.get(f.order_id)
                    if cur_eta is None or f.eta_ts < cur_eta[0]:
                        eta_best[f.order_id] = (f.eta_ts, f.station_eta)
                if f.state == "created":
                    continue
                if f.order_id not in records or f.state == "released":
                    records[f.order_id] = OrderRecord(
                        f.order_id,
                        f.product,
                        f.qty,
                        f.due_ts,
                        f.created_ts,
                        fail_step=f.fail_step,
                        fail_times=f.fail_times,
                        poison=f.poison,
                    )
                lifecycle_states.setdefault(f.order_id, set()).add(f.state)
                prev = lifecycle_ts.setdefault(f.order_id, {}).get(f.state)
                if prev is None or f.ts < prev:
                    lifecycle_ts[f.order_id][f.state] = f.ts
            elif isinstance(f, OpDispatched):
                seen_dispatched.append(f)
                dkey = (f.order_id, f.step, f.visit)
                if dkey not in dispatched or f.ready_ts < dispatched[dkey]:
                    dispatched[dkey] = f.ready_ts
            elif isinstance(f, OpStarted):
                seen_started.append(f)
                skey = (f.order_id, f.step, f.visit)
                if skey not in started or f.ts < started[skey]:
                    started[skey] = f.ts
            elif isinstance(f, OpFailed):
                seen_failed.append(f)
                fkey = (f.order_id, f.step, f.visit)
                if fkey not in failed or f.ts < failed[fkey]:
                    failed[fkey] = f.ts
            elif isinstance(f, OpCompleted):
                seen_completed.append(f)
                ckey = (f.order_id, f.step)
                if ckey not in completed or f.ts < completed[ckey]:
                    completed[ckey] = f.ts

        # Pre-populate dedupers so the live tail cannot double-apply.
        for s in seen_started:
            core._started.first_time_op(s)
        for c in seen_completed:
            core._completed.first_time_op(c)
        for d in seen_dispatched:
            core._dispatched.first_time_op(d)
        for fl in seen_failed:
            core._failed.first_time_op(fl)

        visits: dict[tuple[str, str], set[int]] = {}
        for oid_, step_, v_ in [*dispatched, *started, *failed]:
            visits.setdefault((oid_, step_), set()).add(v_)

        limits = core.config.rework_limits
        candidates: dict[str, list[tuple[datetime, BacklogEntry]]] = {s: [] for s in route}
        extra: list[tuple[str, list[Action]]] = []
        for oid, rec in records.items():
            if oid in eta_best:
                rec = replace(rec, eta_ts=eta_best[oid][0], station_eta=eta_best[oid][1])
            if oid in prio_marks:
                rec = replace(rec, priority=max(prio_marks[oid])[3])
            states = lifecycle_states[oid]
            st = _OrderState(record=rec, emitted=set(states))
            core.orders[oid] = st
            released_ts = lifecycle_ts[oid].get("released") or min(lifecycle_ts[oid].values())
            idx = 0
            entered = released_ts
            while idx < len(route) and (oid, route[idx]) in completed:
                entered = completed[(oid, route[idx])]
                idx += 1
            st.next_index = idx
            if idx >= len(route) or "failed" in states:
                continue
            station = route[idx]
            vs = visits.get((oid, station))
            if not vs:
                core.backlog[station].append(BacklogEntry(oid, idx, entered))
                continue
            dv = max(vs)
            if (oid, station, dv) in failed:
                fts = failed[(oid, station, dv)]
                decision = rw.decide(station, dv, limits)
                if isinstance(decision, rw.Rework):
                    nv = decision.next_visit
                    # A lot fact (BANK / stocker) at this station after the failure
                    # proves the hold message already came back: the lot sits in the
                    # backlog, and the bank / stock restore below must find it there.
                    # A bank mark may sit exactly at fts: a deferred hold applied on the
                    # failure banks the lot at the failure ts (>=); stock marks stay (>).
                    returned = any(
                        m[0] >= fts and m[-1] == station for m in bank_marks.get(oid, ())
                    ) or any(m[0] > fts and m[-1] == station for m in stock_marks.get(oid, ()))
                    if returned:
                        core.backlog[station].append(
                            BacklogEntry(oid, idx, entered, nv, ready_ts=fts)
                        )
                        continue
                    # D-09: credit is released on op_failed; the lot cools outside
                    # the backlog and the hold message is re-published (dup = no-op).
                    core.cooling[oid] = CoolingRec(station, idx, nv, entered, fts)
                    extra.append(
                        (
                            oid,
                            [
                                PublishRework(
                                    order_id=oid,
                                    step=station,
                                    visit=nv,
                                    product=rec.product,
                                    qty=rec.qty,
                                    entered_ts=entered,
                                    command_event_id=job_event_id(run_id, oid, station, nv),
                                    failed_visit=dv,
                                    failed_ts=fts,
                                )
                            ],
                        )
                    )
                else:
                    st.emitted.add("failed")
                    extra.append(
                        (
                            oid,
                            [
                                PublishDeadLetter(
                                    order_id=oid,
                                    step=station,
                                    visit=dv,
                                    product=rec.product,
                                    qty=rec.qty,
                                    command_event_id=job_event_id(run_id, oid, station, dv),
                                    reason=decision.reason,
                                ),
                                EmitLifecycle("failed", oid, fts, step=station, visit=dv),
                            ],
                        )
                    )
                continue
            ready = dispatched.get((oid, station, dv), entered)
            candidates[station].append((ready, BacklogEntry(oid, idx, entered, dv)))

        for station, cands in candidates.items():
            if not cands:
                continue
            cands.sort(key=lambda c: (c[0], c[1].order_id))
            core.outstanding[station] = cands[0][1]
            first = cands[0][1]
            core.outstanding_started[station] = started.get((first.order_id, station, first.visit))
            for _ready, entry in cands[1:]:
                core.backlog[station].append(entry)
        for station in route:
            core.backlog[station].sort(key=_entry_key)

        for oid in sorted(bank_marks):
            # max over (ts, flag): on a tie flag 1 (released) wins, the safe state.
            mts, flag, reason, source, _st = max(bank_marks[oid])
            if flag != 0 or oid not in core.orders:
                continue
            found = core._find_entry(oid)
            if found is None:
                # Banked after the last station: no queue entry exists, the lot is
                # parked with an end-of-route entry (step_index == len(route)).
                ost = core.orders[oid]
                if (
                    ost.next_index >= len(route)
                    and "completed" not in ost.emitted
                    and _st == route[-1]
                    and (oid, route[-1]) in completed
                ):
                    end = BacklogEntry(oid, len(route), completed[(oid, route[-1])])
                    core.banked[oid] = BankRecord(end, route[-1], reason, source, mts)
                continue
            bstation, entry = found
            core.backlog[bstation].remove(entry)
            core.banked[oid] = BankRecord(entry, bstation, reason, source, mts)
        for oid in sorted(stock_marks):
            _sts, flag, kind, _st = max(stock_marks[oid])
            if flag != 0:
                continue
            found = core._find_entry(oid)
            if found is not None:
                core.stock_kind[oid] = (found[0], kind)

        hold_actions: list[Action] = []
        hold_ts = now or max_ts or datetime.min.replace(tzinfo=UTC)
        for oid in sorted(hold_marks):
            hts, hrank, hkind, hreason, hstation, hstate = max(hold_marks[oid])
            hst = core.orders.get(oid)
            if hst is None or "failed" in hst.emitted or hstation not in route:
                continue
            if oid in core.banked or oid in core.holds:
                continue
            pending = hrank == 0
            # An applied bank whose LotBanked never got written is re-applied.
            lost = (
                hstate == "applied"
                and hkind == "bank"
                and not any(m[0] >= hts for m in bank_marks.get(oid, ()))
            )
            if not (pending or lost):
                continue
            hrec = HoldRecord(hkind, hreason, hstation, 0, hts)  # type: ignore[arg-type]
            hidx = route.index(hstation)
            out = core.outstanding[hstation]
            if (oid, hstation) in completed and hst.next_index > hidx:
                # The op completed but the bank / scrap never got written: re-apply.
                if hidx + 1 < len(route):
                    found = core._find_entry(oid)
                    if found is None:
                        continue
                    nstation, nentry = found
                    core.backlog[nstation].remove(nentry)
                else:
                    nstation = route[-1]
                    nentry = BacklogEntry(oid, len(route), completed[(oid, hstation)])
                hold_actions.extend(core._apply_hold(hrec, nentry, nstation, hold_ts))
            elif oid in core.cooling:
                cooled = core._cooling_entry(oid)
                assert cooled is not None
                hold_actions.extend(core._apply_hold(hrec, cooled[1], cooled[0], hold_ts))
                # the rework hold message must not be republished for an applied hold
                extra = [e for e in extra if e[0] != oid]
            elif pending and out is not None and out.order_id == oid:
                core.holds[oid] = HoldRecord(hkind, hreason, hstation, out.visit, hts)  # type: ignore[arg-type]

        for station, ev in status_best.items():
            h = core.health[station]
            h.reported = ev.status
            h.reported_ts = ev.ts
            repair_in = getattr(ev, "repair_in_s", None)
            if ev.status == "down" and isinstance(ev, StationStatus) and repair_in is not None:
                h.repair_until = ev.ts + timedelta(seconds=repair_in)
            if not core.dispatchable(station):
                h.unavailable_since = ev.ts

        reconcile: list[Action] = []
        for oid in sorted(core.orders):
            st = core.orders[oid]
            if "in_process" not in st.emitted and (
                any((oid, route[0], v) in started for v in visits.get((oid, route[0]), ()))
                or (oid, route[0]) in completed
            ):
                ts = min(
                    t
                    for t in (
                        [s.ts for s in seen_started if s.order_id == oid and s.step == route[0]]
                        + ([completed[(oid, route[0])]] if (oid, route[0]) in completed else [])
                    )
                )
                st.emitted.add("in_process")
                reconcile.append(EmitLifecycle("in_process", oid, ts))
            if (
                "completed" not in st.emitted
                and "failed" not in st.emitted
                and st.next_index >= len(route)
                and oid not in core.banked
            ):
                st.emitted.add("completed")
                reconcile.append(EmitLifecycle("completed", oid, completed[(oid, route[-1])]))
        for _oid, acts in sorted(extra, key=lambda e: e[0]):
            reconcile.extend(acts)
        reconcile.extend(hold_actions)
        core.paused = newest_pause is not None and not newest_pause[1]
        # Restore the active rule from the newest RuleChanged fact (D-04)
        if newest_rule is not None:
            core.rule = newest_rule[1]
        now_ts = now or max_ts or datetime.min.replace(tzinfo=UTC)
        for station in route:
            if core.dispatchable(station):
                reconcile.extend(core._release_auto(station, "auto_down", now_ts))
        reconcile.extend(core.reclassify(now_ts))
        return core, reconcile


def newest_run_id(facts: Iterable[Message]) -> str | None:
    """Run id of the lifecycle fact with the greatest ts (research A2)."""
    best: OrderLifecycle | None = None
    for f in facts:
        if isinstance(f, OrderLifecycle) and (best is None or f.ts > best.ts):
            best = f
    return best.run_id if best is not None else None
