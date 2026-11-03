import json
import random
from datetime import UTC, datetime, timedelta

from pizza.common.constants import STATIONS
from pizza.common.envelope import (
    LotBanked,
    LotReleased,
    LotStocked,
    LotUnstocked,
    Message,
    OpCompleted,
    OpDispatched,
    OpStarted,
    OrderLifecycle,
    PriorityChanged,
    RuleChanged,
    StationStatus,
)
from pizza.common.envelope import parse_message
from pizza.common.ids import job_event_id
from pizza.scheduler.core import (
    CoreConfig,
    Dispatch,
    EmitLifecycle,
    EmitLot,
    OrderRecord,
    SchedulerCore,
    newest_run_id,
)

RUN = "r1"
T0 = datetime(2026, 1, 1, tzinfo=UTC)


def ts(n: int) -> datetime:
    return T0 + timedelta(milliseconds=n)


def rec(oid: str) -> OrderRecord:
    return OrderRecord(oid, "margherita", 2, T0 + timedelta(hours=1), T0)


class Harness:
    """Drives a live core and records the facts the shell would produce."""

    def __init__(self) -> None:
        self.core = SchedulerCore()
        self.core.start_run(RUN)
        self.facts: list[Message] = []
        self.clock = 0
        self.inflight: list[Dispatch] = []
        # Every station reports idle first: a never-reported station is not dispatchable.
        for station in self.core.route:
            status = StationStatus.make(run_id=RUN, machine_id=station, ts=ts(0), status="idle")
            self.facts.append(status)
            self.core.on_station_status(status)

    def tick(self) -> datetime:
        self.clock += 1
        return ts(self.clock)

    def _apply(self, actions: list) -> None:
        for a in actions:
            if isinstance(a, EmitLifecycle):
                rec_ = self.core.orders[a.order_id].record
                self.facts.append(
                    OrderLifecycle.make(
                        run_id=RUN,
                        machine_id="scheduler",
                        ts=a.ts,
                        state=a.state,
                        order_id=a.order_id,
                        product=rec_.product,
                        qty=rec_.qty,
                        due_ts=rec_.due_ts,
                        created_ts=rec_.created_ts,
                    )
                )
            elif isinstance(a, Dispatch):
                self.inflight.append(a)
                self.facts.append(
                    OpDispatched.make(
                        run_id=RUN,
                        machine_id="scheduler",
                        ts=self.tick(),
                        order_id=a.order_id,
                        step=a.step,
                        visit=1,
                        qty=a.qty,
                        product=a.product,
                        command_event_id=a.command_event_id,
                        ready_ts=a.ready_ts,
                    )
                )

    def order(self, oid: str) -> None:
        # created fact precedes release
        r = rec(oid)
        self.facts.append(
            OrderLifecycle.make(
                run_id=RUN,
                machine_id="generator",
                ts=self.tick(),
                state="created",
                order_id=oid,
                product=r.product,
                qty=r.qty,
                due_ts=r.due_ts,
                created_ts=r.created_ts,
            )
        )
        self._apply(self.core.on_order(r, self.tick()))

    def start(self, d: Dispatch) -> None:
        ev = OpStarted.make(
            run_id=RUN,
            machine_id=d.step,
            ts=self.tick(),
            order_id=d.order_id,
            step=d.step,
            visit=1,
            attempt=1,
            qty=d.qty,
            command_event_id=d.command_event_id,
            redelivered=False,
        )
        self.facts.append(ev)
        self._apply(self.core.on_op_started(ev))

    def finish(self, d: Dispatch) -> None:
        ev = OpCompleted.make(
            run_id=RUN,
            machine_id=d.step,
            ts=self.tick(),
            order_id=d.order_id,
            step=d.step,
            visit=1,
            attempt=1,
            qty=d.qty,
            command_event_id=d.command_event_id,
            duration_s=0.0,
        )
        self.facts.append(ev)
        self._apply(self.core.on_op_completed(ev))


def scenario() -> Harness:
    h = Harness()
    for i in range(8):
        h.order(f"o{i}")
    rng = random.Random(3)
    for _ in range(40):
        if not h.inflight:
            break
        d = h.inflight.pop(rng.randrange(len(h.inflight)))
        h.start(d)
        h.finish(d)
    # leave one started-but-unfinished job
    if h.inflight:
        h.start(h.inflight[0])
    return h


def test_shuffled_replay_equals_live() -> None:
    h = scenario()
    live = h.core.snapshot()
    assert any(v is not None for v in live["outstanding"].values())
    rng = random.Random(11)
    for _ in range(25):
        facts = list(h.facts)
        rng.shuffle(facts)
        core, reconcile = SchedulerCore.rebuild(RUN, facts)
        assert core.snapshot() == live
        assert reconcile == []


def test_outstanding_not_redispatched() -> None:
    h = Harness()
    h.order("A")
    h.order("B")
    core, _ = SchedulerCore.rebuild(RUN, h.facts)
    assert core.snapshot()["outstanding"]["spin"] == "A"
    assert core.snapshot()["backlog"]["spin"] == ["B"]
    assert core.dispatch_ready() == []


def test_backlog_entered_ts_and_order() -> None:
    h = Harness()
    h.order("A")
    h.order("B")
    h.finish(h.inflight.pop(0))
    core, _ = SchedulerCore.rebuild(RUN, h.facts)
    # A completed spin; sauce already dispatched to A
    assert core.snapshot()["outstanding"]["sauce"] == "A"
    assert core.snapshot()["outstanding"]["spin"] == "B"


def test_released_never_dispatched_goes_to_spin_backlog() -> None:
    h = Harness()
    h.order("A")
    facts = [f for f in h.facts if not isinstance(f, OpDispatched)]
    core, _ = SchedulerCore.rebuild(RUN, facts)
    assert core.snapshot()["backlog"]["spin"] == ["A"]
    assert core.backlog["spin"][0].entered_ts == next(
        f.ts for f in facts if isinstance(f, OrderLifecycle) and f.state == "released"
    )
    actions = core.dispatch_ready()
    assert [a.step for a in actions] == ["spin"]


def test_created_only_not_rebuilt() -> None:
    h = Harness()
    h.order("A")
    facts = [f for f in h.facts if not (isinstance(f, OrderLifecycle) and f.state != "created")]
    facts = [f for f in facts if not isinstance(f, OpDispatched)]
    core, _ = SchedulerCore.rebuild(RUN, facts)
    assert core.orders == {}


def test_completed_without_dispatched_tolerated() -> None:
    h = Harness()
    h.order("A")
    h.finish(h.inflight[0])
    facts = [f for f in h.facts if not (isinstance(f, OpDispatched) and f.step == "spin")]
    core, _ = SchedulerCore.rebuild(RUN, facts)
    assert core.orders["A"].next_index == 1


def test_duplicate_facts_change_nothing() -> None:
    h = scenario()
    live = h.core.snapshot()
    core, _ = SchedulerCore.rebuild(RUN, h.facts + h.facts)
    assert core.snapshot() == live


def test_other_run_ignored() -> None:
    h = Harness()
    h.order("A")
    core, _ = SchedulerCore.rebuild("other", h.facts)
    assert core.orders == {}
    assert core.current_run_id == "other"


def test_reconcile_in_process_and_completed() -> None:
    h = Harness()
    h.order("A")
    d = h.inflight.pop(0)
    h.start(d)
    for _ in STATIONS:
        h.finish(d)
        if not h.inflight:
            break
        d = h.inflight.pop(0)
    assert h.core.snapshot()["orders"]["A"][0] == len(STATIONS)
    stripped = [
        f
        for f in h.facts
        if not (isinstance(f, OrderLifecycle) and f.state in ("in_process", "completed"))
    ]
    core, reconcile = SchedulerCore.rebuild(RUN, stripped)
    assert sorted(a.state for a in reconcile) == ["completed", "in_process"]
    assert core.snapshot() == h.core.snapshot()
    core2, again = SchedulerCore.rebuild(RUN, h.facts)
    assert again == []


def test_newest_run_id() -> None:
    assert newest_run_id([]) is None
    a = OrderLifecycle.make(
        run_id="old",
        machine_id="s",
        ts=ts(1),
        state="released",
        order_id="x",
        product="margherita",
        qty=1,
        due_ts=ts(5),
        created_ts=ts(0),
    )
    b = a.model_copy(update={"run_id": "new", "ts": ts(9)})
    assert newest_run_id([b, a]) == "new"


def _pf(cls, n: int, run: str = RUN):
    return cls.make(run_id=run, machine_id="scheduler", ts=ts(n))


def test_rebuild_restores_paused_from_newest_fact() -> None:
    from pizza.common.envelope import RunPaused, RunResumed

    h = Harness()
    h.order("A")
    base = list(h.facts)
    paused = [_pf(RunPaused, 100), _pf(RunResumed, 50), _pf(RunPaused, 10, "other")]
    for seed in range(5):
        facts = base + paused
        random.Random(seed).shuffle(facts)
        core, _ = SchedulerCore.rebuild(RUN, facts)
        assert core.paused is True
        assert core.dispatch_ready() == []
    resumed = [_pf(RunPaused, 100), _pf(RunResumed, 200)]
    core, _ = SchedulerCore.rebuild(RUN, base + resumed)
    assert core.paused is False
    core, _ = SchedulerCore.rebuild(RUN, base)
    assert core.paused is False
    tie = [_pf(RunPaused, 7), _pf(RunResumed, 7)]
    core, _ = SchedulerCore.rebuild(RUN, base + tie)
    assert core.paused is False


# -- Phase 6: visit-aware rebuild -------------------------------------------
def _rework_facts(cause: str | None = None) -> tuple[Harness, list[Message]]:
    from pizza.common.envelope import OpFailed
    from pizza.common.ids import job_event_id

    h = Harness()
    h.order("A")
    d = h.inflight.pop(0)
    h.start(d)
    fails = {}
    for v in (1, 2, 3):
        fails[v] = OpFailed.make(
            run_id=RUN,
            machine_id="spin",
            ts=ts(100 + v),
            order_id="A",
            step="spin",
            visit=v,
            attempt=1,
            qty=d.qty,
            product=d.product,
            command_event_id=job_event_id(RUN, "A", "spin", v),
            reason="boom",
            cause=cause,
        )
    h.fails = fails  # type: ignore[attr-defined]
    return h, list(h.facts)


def _disp(h: Harness, visit: int, n: int) -> OpDispatched:
    from pizza.common.ids import job_event_id

    return OpDispatched.make(
        run_id=RUN,
        machine_id="scheduler",
        ts=ts(n),
        order_id="A",
        step="spin",
        visit=visit,
        qty=2,
        product="margherita",
        command_event_id=job_event_id(RUN, "A", "spin", visit),
        ready_ts=ts(n),
    )


def test_rebuild_rework_visit_outstanding_and_shuffle_stable() -> None:
    h, base = _rework_facts()
    facts = [*base, h.fails[1], _disp(h, 2, 110)]
    core, reconcile = SchedulerCore.rebuild(RUN, facts)
    assert core.outstanding["spin"].visit == 2
    assert reconcile == []
    snap = core.snapshot()
    rng = random.Random(5)
    for _ in range(50):
        shuffled = list(facts)
        rng.shuffle(shuffled)
        c2, r2 = SchedulerCore.rebuild(RUN, shuffled)
        assert c2.snapshot() == snap
        assert r2 == []


def test_rebuild_reissues_lost_rework_once() -> None:
    from pizza.scheduler.core import PublishRework

    h, base = _rework_facts()
    core, reconcile = SchedulerCore.rebuild(RUN, [*base, h.fails[1]])
    assert core.outstanding["spin"] is None
    assert "A" in core.cooling
    assert core.cooling["A"].visit == 2
    assert core.cooling["A"].failed_ts == h.fails[1].ts
    assert [type(a) for a in reconcile] == [PublishRework]
    assert reconcile[0].visit == 2
    assert reconcile[0].failed_visit == 1
    assert reconcile[0].failed_ts == h.fails[1].ts
    assert reconcile[0].entered_ts == core.cooling["A"].entered_ts
    _core2, again = SchedulerCore.rebuild(RUN, [*base, h.fails[1], _disp(h, 2, 110)])
    assert again == []


def test_rebuild_cooling_shuffle_stable() -> None:
    h, base = _rework_facts()
    facts = [*base, h.fails[1]]
    core, _ = SchedulerCore.rebuild(RUN, facts)
    snap = core.snapshot()
    assert snap["cooling"] == {"A": ("spin", 2)}
    rng = random.Random(5)
    for _ in range(50):
        shuffled = list(facts)
        rng.shuffle(shuffled)
        c2, r2 = SchedulerCore.rebuild(RUN, shuffled)
        assert c2.snapshot() == snap
        assert len(r2) == 1


def test_rebuild_cooling_and_other_lot_outstanding_coexist() -> None:
    h, base = _rework_facts()
    h.order("B")
    from pizza.common.ids import job_event_id

    disp_b = OpDispatched.make(
        run_id=RUN,
        machine_id="scheduler",
        ts=ts(120),
        order_id="B",
        step="spin",
        visit=1,
        qty=2,
        product="margherita",
        command_event_id=job_event_id(RUN, "B", "spin", 1),
        ready_ts=ts(120),
    )
    facts = [*h.facts, h.fails[1], disp_b]
    core, _ = SchedulerCore.rebuild(RUN, facts)
    assert "A" in core.cooling
    assert core.outstanding["spin"] is not None
    assert core.outstanding["spin"].order_id == "B"
    assert all(e.order_id != "A" for e in core.backlog["spin"])


def test_rebuild_cooling_then_hold_return_dispatches_once() -> None:
    h, base = _rework_facts()
    core, _ = SchedulerCore.rebuild(RUN, [*base, h.fails[1]])
    now = ts(500)
    first = core.on_rework_ready("A", "spin", 2, RUN, now)
    second = core.on_rework_ready("A", "spin", 2, RUN, now)
    dispatches = [a for a in first + second if isinstance(a, Dispatch)]
    assert len(dispatches) == 1
    assert dispatches[0].visit == 2
    assert dispatches[0].ready_ts == h.fails[1].ts
    assert core.cooling == {}


def test_rebuild_returned_rework_lot_keeps_manual_bank() -> None:
    """CR-01: a lot banked after its hold came back is restored to BANK, not cooling."""
    from pizza.scheduler.core import PublishRework

    h, base = _rework_facts()
    fail_ts = h.fails[1].ts
    facts = [*base, h.fails[1], _banked("A", 200)]
    core, reconcile = SchedulerCore.rebuild(RUN, facts)
    assert "A" in core.banked
    assert "A" not in core.cooling
    assert core.banked["A"].source == "manual"
    assert core.banked["A"].entry.visit == 2
    assert core.banked["A"].entry.ready_ts == fail_ts
    assert not any(isinstance(a, PublishRework) for a in reconcile)
    # A sweep long after the failure must not dispatch the banked lot.
    assert [a for a in core.sweep(ts(60_000)) if isinstance(a, Dispatch)] == []
    # A later manual release puts it back and it dispatches as visit 2.
    acts = core.release("A", ts(60_001))
    dispatches = [a for a in acts if isinstance(a, Dispatch)]
    assert [(d.order_id, d.visit, d.ready_ts) for d in dispatches] == [("A", 2, fail_ts)]


def test_rebuild_returned_rework_lot_keeps_stocker_mark() -> None:
    """CR-01: a stocked mark after the hold came back is restored (healed by reclassify)."""
    from pizza.scheduler.core import PublishRework

    h, base = _rework_facts()
    stocked = _lot(LotStocked, "A", 200, kind="overflow")
    core, reconcile = SchedulerCore.rebuild(
        RUN, [*base, h.fails[1], stocked], config=CoreConfig(queue_limit=1)
    )
    assert "A" not in core.cooling
    assert not any(isinstance(a, PublishRework) for a in reconcile)
    # The lot is first in line, so the stale overflow mark is healed explicitly.
    assert [(a.fact, a.order_id) for a in reconcile if isinstance(a, EmitLot)] == [
        ("unstocked", "A")
    ]
    # A mark from before the failure does not count as "returned".
    early = _lot(LotStocked, "A", 50, kind="overflow")
    core2, reconcile2 = SchedulerCore.rebuild(RUN, [*base, h.fails[1], early])
    assert "A" in core2.cooling
    assert [type(a) for a in reconcile2 if not isinstance(a, EmitLot)] == [PublishRework]


def test_rebuild_dead_letter_reconcile() -> None:
    from pizza.scheduler.core import PublishDeadLetter

    h, base = _rework_facts()
    facts = [*base, h.fails[1], _disp(h, 2, 110), h.fails[2], _disp(h, 3, 120), h.fails[3]]
    core, reconcile = SchedulerCore.rebuild(RUN, facts)
    assert core.outstanding["spin"] is None
    assert "failed" in core.orders["A"].emitted
    assert [type(a) for a in reconcile] == [PublishDeadLetter, EmitLifecycle]
    assert reconcile[1].state == "failed"
    assert all(e.order_id != "A" for e in core.backlog["spin"])

    rec_ = core.orders["A"].record
    failed_fact = OrderLifecycle.make(
        run_id=RUN,
        machine_id="scheduler",
        ts=ts(200),
        state="failed",
        order_id="A",
        product=rec_.product,
        qty=rec_.qty,
        due_ts=rec_.due_ts,
        created_ts=rec_.created_ts,
    )
    core2, reconcile2 = SchedulerCore.rebuild(RUN, [*facts, failed_fact])
    assert reconcile2 == []
    assert core2.outstanding["spin"] is None
    assert "failed" in core2.orders["A"].emitted


def test_rebuild_with_breakdown_cause_matches_without_cause() -> None:
    h0, base0 = _rework_facts()
    h1, base1 = _rework_facts(cause="breakdown")
    log0 = [*base0, h0.fails[1], _disp(h0, 2, 110), h0.fails[2], _disp(h0, 3, 120), h0.fails[3]]
    log1 = [*base1, h1.fails[1], _disp(h1, 2, 110), h1.fails[2], _disp(h1, 3, 120), h1.fails[3]]
    c0, r0 = SchedulerCore.rebuild(RUN, log0)
    c1, r1 = SchedulerCore.rebuild(RUN, log1)
    assert c1.snapshot() == c0.snapshot()
    assert [type(a) for a in r1] == [type(a) for a in r0]
    assert c1.outstanding["spin"] is None
    assert "failed" in c1.orders["A"].emitted
    # lost rework re-issue is also identical
    c2, r2 = SchedulerCore.rebuild(RUN, [*base1, h1.fails[1]])
    assert c2.outstanding["spin"] is None
    assert c2.cooling["A"].visit == 2
    assert [type(a) for a in r2] == [
        type(a) for a in SchedulerCore.rebuild(RUN, [*base0, h0.fails[1]])[1]
    ]


def test_rebuild_rework_completion_advances() -> None:
    h, base = _rework_facts()
    from pizza.common.ids import job_event_id

    done = OpCompleted.make(
        run_id=RUN,
        machine_id="spin",
        ts=ts(150),
        order_id="A",
        step="spin",
        visit=2,
        attempt=1,
        qty=2,
        command_event_id=job_event_id(RUN, "A", "spin", 2),
        duration_s=0.0,
    )
    core, _ = SchedulerCore.rebuild(RUN, [*base, h.fails[1], _disp(h, 2, 110), done])
    assert core.orders["A"].next_index == 1
    assert core.outstanding["spin"] is None
    assert core.outstanding["sauce"] is None or core.outstanding["sauce"].visit == 1


def test_rebuild_restores_health() -> None:
    from pizza.common.envelope import StationHeartbeat

    h = Harness()
    h.order("A")
    base = [f for f in h.facts if not isinstance(f, OpDispatched)]
    extra = [
        StationStatus.make(run_id=RUN, machine_id="spin", ts=ts(500), status="down"),
        StationHeartbeat.make(run_id=RUN, machine_id="spin", ts=ts(400), status="idle"),
        StationStatus.make(run_id="other", machine_id="sauce", ts=ts(500), status="down"),
        StationHeartbeat.make(run_id="unknown", machine_id="cheese", ts=ts(500), status="down"),
    ]
    rng = random.Random(2)
    for _ in range(10):
        facts = [*base, *extra]
        rng.shuffle(facts)
        core, _ = SchedulerCore.rebuild(RUN, facts)
        assert core.health["spin"].reported == "down"
        assert core.health["spin"].offline is False
        assert core.health["sauce"].reported == "idle"
        assert core.health["cheese"].reported == "down"
        assert core.dispatch_ready() == []


def test_rebuild_keeps_fail_flags() -> None:
    r = rec("A")
    released = OrderLifecycle.make(
        run_id=RUN,
        machine_id="scheduler",
        ts=ts(1),
        state="released",
        order_id="A",
        product=r.product,
        qty=r.qty,
        due_ts=r.due_ts,
        created_ts=r.created_ts,
        fail_step="bake",
        fail_times=2,
        poison=True,
    )
    core, _ = SchedulerCore.rebuild(RUN, [released])
    assert core.fail_flags("A", "bake") == (2, True)
    assert core.fail_flags("A", "spin") == (None, False)


# -- stocker / BANK facts (plan 06-09) -----------------------------------------
def _released(oid: str, n: int) -> OrderLifecycle:
    r = rec(oid)
    return OrderLifecycle.make(
        run_id=RUN,
        machine_id="scheduler",
        ts=ts(n),
        state="released",
        order_id=oid,
        product=r.product,
        qty=r.qty,
        due_ts=r.due_ts,
        created_ts=r.created_ts,
    )


def _lot(cls, oid: str, n: int, station: str = "spin", **kw):
    return cls.make(
        run_id=RUN, machine_id="scheduler", ts=ts(n), order_id=oid, station=station, **kw
    )


def _banked(oid: str, n: int, source: str = "manual"):
    return _lot(LotBanked, oid, n, reason="customer", source=source)


def test_rebuild_bank_marks_resolve_by_ts_and_tie_releases() -> None:
    base = [_released("o-1", 1), _released("o-2", 2)]
    core, _ = SchedulerCore.rebuild(RUN, [*base, _banked("o-2", 5)])
    rec2 = core.banked["o-2"]
    assert (rec2.reason, rec2.source, rec2.since) == ("customer", "manual", ts(5))
    assert [e.order_id for e in core.backlog["spin"]] == ["o-1"]
    assert core.outstanding["spin"] is None

    core, _ = SchedulerCore.rebuild(
        RUN, [*base, _banked("o-2", 5), _lot(LotReleased, "o-2", 9, by="manual")]
    )
    assert core.banked == {}
    assert [e.order_id for e in core.backlog["spin"]] == ["o-1", "o-2"]

    core, _ = SchedulerCore.rebuild(
        RUN, [*base, _lot(LotReleased, "o-2", 3, by="manual"), _banked("o-2", 5)]
    )
    assert "o-2" in core.banked

    core, _ = SchedulerCore.rebuild(
        RUN, [*base, _banked("o-2", 5), _lot(LotReleased, "o-2", 5, by="manual")]
    )
    assert core.banked == {}

    facts = [*base, _banked("o-2", 5), _lot(LotReleased, "o-1", 2, by="manual")]
    snaps = set()
    for seed in range(8):
        shuffled = list(facts)
        random.Random(seed).shuffle(shuffled)
        c, _ = SchedulerCore.rebuild(RUN, shuffled)
        snaps.add(repr(c.snapshot()))
    assert len(snaps) == 1


def test_rebuild_stock_marks_and_reconcile() -> None:
    base = [_released("o-3", 1), _released("o-4", 2)]
    cfg = CoreConfig(queue_limit=1)
    stocked_o3 = _lot(LotStocked, "o-3", 4, kind="overflow")
    stocked_o4 = _lot(LotStocked, "o-4", 4, kind="overflow")

    core, acts = SchedulerCore.rebuild(RUN, [*base, stocked_o4], config=cfg)
    assert core.stock_kind == {"o-4": ("spin", "overflow")}
    assert [a for a in acts if isinstance(a, EmitLot)] == []

    # stale mark: o-3 is first in FIFO, so inside the limit -> healed deterministically
    core, acts = SchedulerCore.rebuild(RUN, [*base, stocked_o3], config=cfg)
    assert [(a.fact, a.order_id) for a in acts if isinstance(a, EmitLot)] == [
        ("unstocked", "o-3"),
        ("stocked", "o-4"),
    ]
    assert core.stock_kind == {"o-4": ("spin", "overflow")}

    # a later unstock clears the mark; a mark for an unknown order is dropped
    core, _ = SchedulerCore.rebuild(RUN, [*base, stocked_o4, _lot(LotUnstocked, "o-4", 6)])
    assert core.stock_kind == {}
    core, _ = SchedulerCore.rebuild(RUN, [*base, _lot(LotStocked, "ghost", 4, kind="overflow")])
    assert core.stock_kind == {}


def _idle_reports() -> list[Message]:
    """Every station reports idle: a never-reported station is not dispatchable."""
    return [StationStatus.make(run_id=RUN, machine_id=s, ts=ts(0), status="idle") for s in STATIONS]


def test_rebuild_releases_auto_down_when_station_up() -> None:
    base = [*_idle_reports(), _released("o-1", 1), _released("o-2", 2)]
    facts = [*base, _banked("o-1", 5, "auto_down"), _banked("o-2", 5, "auto_down")]
    core, acts = SchedulerCore.rebuild(RUN, facts)
    assert core.banked == {}
    released = [a for a in acts if isinstance(a, EmitLot) and a.fact == "released"]
    assert [(a.order_id, a.by) for a in released] == [("o-1", "auto"), ("o-2", "auto")]

    down = StationStatus.make(run_id=RUN, machine_id="spin", ts=ts(4), status="down")
    core, acts = SchedulerCore.rebuild(RUN, [*facts, down])
    assert set(core.banked) == {"o-1", "o-2"}
    assert [a for a in acts if isinstance(a, EmitLot) and a.fact == "released"] == []


def test_rebuild_now_defaults_to_max_fact_ts_and_takes_config() -> None:
    base = [
        *_idle_reports(),
        _released("o-1", 1),
        _released("o-2", 2),
        _banked("o-2", 5, "auto_down"),
    ]
    _, acts = SchedulerCore.rebuild(RUN, base)
    assert [a.ts for a in acts if isinstance(a, EmitLot)] == [ts(5)]
    cfg = CoreConfig(rework_limits={"bake": 1})
    _, acts = SchedulerCore.rebuild(RUN, base, config=cfg, now=ts(77))
    assert [a.ts for a in acts if isinstance(a, EmitLot)] == [ts(77)]


def test_rebuild_restores_rule_from_newest_rule_changed() -> None:
    """Rebuild restores the active rule from the newest RuleChanged fact (D-04)."""
    base = [*_idle_reports(), _released("o-1", 1)]

    # No RuleChanged fact -> default rule "edd"
    core, _ = SchedulerCore.rebuild(RUN, base)
    assert core.rule == "edd"

    # RuleChanged(fifo) at ts 2
    fifo_change = RuleChanged.make(run_id=RUN, machine_id="scheduler", ts=ts(2), rule="fifo")
    core, _ = SchedulerCore.rebuild(RUN, [*base, fifo_change])
    assert core.rule == "fifo"

    # Add RuleChanged(edd) at ts 5, then RuleChanged(spt) at ts 9
    # Newest is spt at ts 9
    edd_change = RuleChanged.make(run_id=RUN, machine_id="scheduler", ts=ts(5), rule="edd")
    spt_change = RuleChanged.make(run_id=RUN, machine_id="scheduler", ts=ts(9), rule="spt")
    core, _ = SchedulerCore.rebuild(RUN, [*base, edd_change, spt_change])
    assert core.rule == "spt"

    # Shuffle facts: order should not matter (idempotent)
    import random
    shuffled = [*base, spt_change, edd_change]
    random.shuffle(shuffled)
    core2, _ = SchedulerCore.rebuild(RUN, shuffled)
    assert core2.rule == "spt"

    # RuleChanged of a different run_id should be ignored
    other_run = RuleChanged.make(run_id="other_run", machine_id="scheduler", ts=ts(3), rule="fifo")
    core3, _ = SchedulerCore.rebuild(RUN, [*base, other_run, edd_change])
    assert core3.rule == "edd"

    # Duplicate RuleChanged should be idempotent
    core4, _ = SchedulerCore.rebuild(RUN, [*base, edd_change, edd_change, spt_change])
    assert core4.rule == "spt"


def _life(oid: str, n: int, state: str = "released", **extra) -> OrderLifecycle:
    return OrderLifecycle.make(
        run_id=RUN, machine_id="scheduler", ts=ts(n), state=state,
        order_id=oid, product="margherita", qty=1, due_ts=T0 + timedelta(hours=1),
        created_ts=T0, **extra,
    )


def _chg(oid: str, n: int, new: int, run: str = RUN) -> PriorityChanged:
    return PriorityChanged.make(
        run_id=run, machine_id="scheduler", ts=ts(n), order_id=oid,
        previous_priority=50, new_priority=new, source="manual",
    )


def test_rebuild_restores_priority_from_legacy_lifecycle_facts() -> None:
    """Legacy rush payloads parse to 10/50 and rebuild restores them."""
    legacy = []
    for oid, rush in (("o-1", True), ("o-2", False)):
        payload = json.loads(_life(oid, 2).model_dump_json())
        payload.pop("priority", None)
        payload["rush"] = rush
        legacy.append(parse_message(json.dumps(payload).encode()))
    core, _ = SchedulerCore.rebuild(RUN, [*_idle_reports(), *legacy])
    assert core.orders["o-1"].record.priority == 10
    assert core.orders["o-2"].record.priority == 50


def test_rebuild_priority_newest_ts_wins() -> None:
    facts = [*_idle_reports(), _life("o-1", 2, priority=50), _chg("o-1", 5, 7), _chg("o-1", 3, 20)]
    core, _ = SchedulerCore.rebuild(RUN, facts)
    assert core.orders["o-1"].record.priority == 7
    # a later lifecycle fact carrying the new priority agrees
    facts = [*_idle_reports(), _life("o-1", 2, priority=50), _chg("o-1", 5, 7),
             _life("o-1", 9, state="in_process", priority=7)]
    core, _ = SchedulerCore.rebuild(RUN, facts)
    assert core.orders["o-1"].record.priority == 7


def test_rebuild_priority_change_fact_wins_ts_tie() -> None:
    facts = [*_idle_reports(), _life("o-1", 2, priority=50), _chg("o-1", 2, 9)]
    core, _ = SchedulerCore.rebuild(RUN, facts)
    assert core.orders["o-1"].record.priority == 9


def test_rebuild_ignores_foreign_run_and_unknown_order_priority_change() -> None:
    facts = [*_idle_reports(), _life("o-1", 2, priority=50),
             _chg("o-1", 5, 3, run="other"), _chg("ghost", 5, 3)]
    core, _ = SchedulerCore.rebuild(RUN, facts)
    assert core.orders["o-1"].record.priority == 50
    assert "ghost" not in core.orders


def test_rebuild_priority_is_permutation_independent() -> None:
    import itertools

    facts = [
        _life("a", 2, priority=50), _life("b", 3, priority=50), _life("c", 4, priority=50),
        _chg("b", 6, 5), _chg("c", 6, 5), _chg("a", 7, 80),
    ]
    base = [*_idle_reports()]
    seen = set()
    for perm in itertools.permutations(facts):
        core, _ = SchedulerCore.rebuild(RUN, [*base, *perm])
        seen.add(tuple(core.snapshot()["backlog"]["spin"]))
    assert seen == {("b", "c", "a")}


# -- Phase 07.1: eta / started / repair restoration -------------------------


def _eta_facts() -> tuple[list[Message], datetime, dict[str, datetime]]:
    eta = T0 + timedelta(minutes=5)
    st_eta = {"spin": T0 + timedelta(seconds=10), "sauce": T0 + timedelta(seconds=20)}
    cmd = job_event_id(RUN, "o1", "spin", 1)
    facts: list[Message] = [
        OrderLifecycle.make(
            run_id=RUN,
            machine_id="generator",
            ts=ts(1),
            state="released",
            order_id="o1",
            product="margherita",
            qty=2,
            due_ts=T0 + timedelta(hours=1),
            created_ts=T0,
            eta_ts=eta,
            station_eta=st_eta,
        ),
        OpDispatched.make(
            run_id=RUN,
            machine_id="scheduler",
            ts=ts(2),
            order_id="o1",
            step="spin",
            visit=1,
            qty=2,
            product="margherita",
            command_event_id=cmd,
            ready_ts=ts(2),
        ),
        OpStarted.make(
            run_id=RUN,
            machine_id="spin",
            ts=ts(900),
            order_id="o1",
            step="spin",
            visit=1,
            attempt=1,
            qty=2,
            command_event_id=cmd,
            redelivered=False,
        ),
        OpStarted.make(
            run_id=RUN,
            machine_id="spin",
            ts=ts(500),
            order_id="o1",
            step="spin",
            visit=1,
            attempt=1,
            qty=2,
            command_event_id=cmd,
            redelivered=True,
        ),
        StationStatus.make(run_id=RUN, machine_id="bake", ts=ts(100), status="idle"),
        StationStatus.make(
            run_id=RUN, machine_id="bake", ts=ts(200), status="down", repair_in_s=30.0
        ),
        StationStatus.make(run_id=RUN, machine_id="slice", ts=ts(200), status="down"),
        StationStatus.make(run_id=RUN, machine_id="spin", ts=ts(50), status="idle"),
    ]
    return facts, eta, st_eta


def _eta_view(core: SchedulerCore):
    r = core.orders["o1"].record
    return (
        r.eta_ts,
        r.station_eta,
        core.outstanding_started["spin"],
        core.health["bake"].repair_until,
        core.health["slice"].repair_until,
    )


def test_rebuild_restores_eta_started_repair_order_independent():
    facts, eta, st_eta = _eta_facts()
    core, _ = SchedulerCore.rebuild(RUN, facts)
    view = _eta_view(core)
    assert view == (eta, st_eta, ts(500), ts(200) + timedelta(seconds=30), None)
    rng = random.Random(7)
    for _ in range(5):
        shuffled = [*facts, *facts[:4]]
        rng.shuffle(shuffled)
        again, _ = SchedulerCore.rebuild(RUN, shuffled)
        assert _eta_view(again) == view


def test_rebuild_degrades_without_started_fact():
    facts, _eta, _st = _eta_facts()
    facts = [f for f in facts if not isinstance(f, OpStarted)]
    core, _ = SchedulerCore.rebuild(RUN, facts)
    assert core.outstanding["spin"] is not None
    assert core.outstanding_started["spin"] is None
