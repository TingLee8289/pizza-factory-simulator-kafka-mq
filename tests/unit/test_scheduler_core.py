import random
from datetime import UTC, datetime, timedelta

import pytest

from pizza.common.constants import STATIONS
from pizza.common.envelope import OpCompleted, OpStarted, StationStatus
from pizza.common.ids import job_event_id
from pizza.scheduler import core as core_mod
from pizza.scheduler.core import (
    BacklogEntry,
    Dispatch,
    EmitLifecycle,
    EmitStationStatus,
    OrderRecord,
    SchedulerCore,
    select_next,
)

RUN = "r1"
T0 = datetime(2026, 1, 1, tzinfo=UTC)


def ts(n: int) -> datetime:
    return T0 + timedelta(milliseconds=n)


def rec(oid: str) -> OrderRecord:
    return OrderRecord(oid, "margherita", 1, T0 + timedelta(hours=1), T0)


def started(d: Dispatch, t: datetime, run: str = RUN) -> OpStarted:
    return OpStarted.make(
        run_id=run,
        machine_id=d.step,
        ts=t,
        order_id=d.order_id,
        step=d.step,
        visit=1,
        attempt=1,
        qty=d.qty,
        command_event_id=d.command_event_id,
        redelivered=False,
    )


def completed(d: Dispatch, t: datetime, run: str = RUN) -> OpCompleted:
    return OpCompleted.make(
        run_id=run,
        machine_id=d.step,
        ts=t,
        order_id=d.order_id,
        step=d.step,
        visit=1,
        attempt=1,
        qty=d.qty,
        command_event_id=d.command_event_id,
        duration_s=0.0,
    )


def ready(c: SchedulerCore, n: int = -1) -> None:
    """Every station reports idle first: a never-reported station is not dispatchable."""
    for station in c.route:
        c.on_station_status(
            StationStatus.make(run_id=RUN, machine_id=station, ts=ts(n), status="idle")
        )


def fresh() -> SchedulerCore:
    c = SchedulerCore()
    c.start_run(RUN)
    ready(c)
    return c


def test_first_order_released_and_dispatched_second_queues() -> None:
    c = fresh()
    a1 = c.on_order(rec("A"), ts(0))
    assert isinstance(a1[0], EmitLifecycle) and a1[0].state == "released"
    assert isinstance(a1[1], Dispatch) and a1[1].step == "spin"
    assert a1[1].command_event_id == job_event_id(RUN, "A", "spin", 1)
    a2 = c.on_order(rec("B"), ts(1))
    assert len(a2) == 1 and isinstance(a2[0], EmitLifecycle)
    assert c.backlog_lengths()["spin"] == 1


def test_completion_returns_credit_and_routes_next() -> None:
    c = fresh()
    da = c.on_order(rec("A"), ts(0))[1]
    c.on_order(rec("B"), ts(1))
    actions = c.on_op_completed(completed(da, ts(10)))
    dispatches = [a for a in actions if isinstance(a, Dispatch)]
    assert {(d.order_id, d.step) for d in dispatches} == {("A", "sauce"), ("B", "spin")}
    sauce = next(d for d in dispatches if d.step == "sauce")
    assert sauce.ready_ts == ts(10)


def test_fifo_and_tie_break() -> None:
    c = fresh()
    c.on_order(rec("A"), ts(0))
    c.on_order(rec("C"), ts(5))
    c.on_order(rec("B"), ts(5))
    assert [
        e.order_id for e in sorted(c.backlog["spin"], key=lambda e: (e.entered_ts, e.order_id))
    ] == [
        "B",
        "C",
    ]
    assert c.snapshot()["backlog"]["spin"] == ["B", "C"]


def test_unbounded_backlog() -> None:
    c = fresh()
    for i in range(50):
        c.on_order(rec(f"o{i:02d}"), ts(i))
    assert c.backlog_lengths()["spin"] == 49
    assert c.snapshot()["outstanding"]["spin"] == "o00"


def test_started_emits_in_process_once_first_station_only() -> None:
    c = fresh()
    d = c.on_order(rec("A"), ts(0))[1]
    s = started(d, ts(1))
    assert c.on_op_started(s) == [EmitLifecycle("in_process", "A", ts(1))]
    assert c.on_op_started(s) == []
    d2 = c.on_op_completed(completed(d, ts(2)))[-1]
    assert c.on_op_started(started(d2, ts(3))) == []


def test_package_completion_emits_completed_once() -> None:
    c = fresh()
    d = c.on_order(rec("A"), ts(0))[1]
    n = 1
    emitted = []
    while True:
        acts = c.on_op_completed(completed(d, ts(n)))
        n += 1
        emitted += [a for a in acts if isinstance(a, EmitLifecycle)]
        nxt = [a for a in acts if isinstance(a, Dispatch)]
        if not nxt:
            break
        d = nxt[0]
    assert [e.state for e in emitted] == ["completed"]


def test_duplicate_completion_no_double_credit() -> None:
    c = fresh()
    da = c.on_order(rec("A"), ts(0))[1]
    c.on_order(rec("B"), ts(1))
    ev = completed(da, ts(5))
    assert c.on_op_completed(ev)
    assert c.on_op_completed(ev) == []


def test_ignored_events() -> None:
    c = fresh()
    d = c.on_order(rec("A"), ts(0))[1]
    assert c.on_op_completed(completed(d, ts(1), run="other")) == []
    ghost = Dispatch("Z", "spin", 1, "margherita", 1, ts(0), job_event_id(RUN, "Z", "spin", 1))
    assert c.on_op_completed(completed(ghost, ts(1))) == []
    odd = Dispatch("A", "oven", 1, "margherita", 1, ts(0), job_event_id(RUN, "A", "oven", 1))
    assert c.on_op_completed(completed(odd, ts(1))) == []
    # station with no outstanding job
    sauce = Dispatch("A", "sauce", 1, "margherita", 1, ts(0), job_event_id(RUN, "A", "sauce", 1))
    assert c.on_op_completed(completed(sauce, ts(1))) == []
    assert c.snapshot()["outstanding"]["spin"] == "A"


def test_known_order_redelivery_noop() -> None:
    c = fresh()
    c.on_order(rec("A"), ts(0))
    assert c.on_order(rec("A"), ts(1)) == []


def test_start_run_resets() -> None:
    c = fresh()
    c.on_order(rec("A"), ts(0))
    c.start_run("r2")
    assert c.current_run_id == "r2"
    assert c.backlog_lengths()["spin"] == 0
    assert c.snapshot()["orders"] == {}


def test_rollback_and_release_dispatch() -> None:
    c = fresh()
    c.on_order(rec("A"), ts(0))
    c.on_order(rec("B"), ts(1))
    c.release_dispatch("spin")
    assert c.snapshot()["outstanding"]["spin"] is None
    assert c.snapshot()["backlog"]["spin"] == ["A", "B"]
    c.rollback_order("A")
    assert c.snapshot()["backlog"]["spin"] == ["B"]
    assert "A" not in c.orders


def test_select_next_patchable(monkeypatch: pytest.MonkeyPatch) -> None:
    c = fresh()
    c.on_order(rec("A"), ts(0))
    c.on_order(rec("B"), ts(1))
    c.on_order(rec("C"), ts(2))
    monkeypatch.setattr(
        core_mod, "select_next", lambda b, key=None: max(b, key=lambda e: e.entered_ts)
    )
    d = c.on_op_completed(
        completed(
            Dispatch("A", "spin", 1, "margherita", 1, ts(0), job_event_id(RUN, "A", "spin", 1)),
            ts(10),
        )
    )
    assert [x.order_id for x in d if isinstance(x, Dispatch) and x.step == "spin"] == ["C"]


def test_select_next_default() -> None:
    entries = [
        BacklogEntry("b", 0, ts(2)),
        BacklogEntry("a", 0, ts(2)),
        BacklogEntry("c", 0, ts(1)),
    ]
    assert select_next(entries).order_id == "c"


def test_one_outstanding_invariant_simulation() -> None:
    rng = random.Random(7)
    c = fresh()
    clock = 0
    pending: list[Dispatch] = []
    done: dict[str, list[str]] = {}
    for i in range(30):
        clock += 1
        pending += [a for a in c.on_order(rec(f"o{i:02d}"), ts(clock)) if isinstance(a, Dispatch)]
    while pending:
        d = pending.pop(rng.randrange(len(pending)))
        done.setdefault(d.order_id, []).append(d.step)
        clock += 1
        for a in c.on_op_completed(completed(d, ts(clock))):
            if isinstance(a, Dispatch):
                pending.append(a)
        outs = [o for o in c.snapshot()["outstanding"].values() if o is not None]
        assert len(outs) <= len(STATIONS)
        assert len(pending) <= len(STATIONS)
    assert len(done) == 30
    assert all(steps == list(STATIONS) for steps in done.values())


def test_pause_defaults_and_start_run_clears() -> None:
    c = SchedulerCore()
    assert c.paused is False and c.rule == "edd"
    c.paused = True
    c.start_run(RUN)
    assert c.paused is False


def test_paused_holds_dispatch_until_resume() -> None:
    c = SchedulerCore()
    c.start_run(RUN)
    ready(c)
    c.paused = True
    actions = c.on_order(rec("A"), ts(1))
    assert [type(a) for a in actions] == [EmitLifecycle]
    assert c.dispatch_ready() == []
    assert c.backlog_lengths()[STATIONS[0]] == 1
    assert c.outstanding[STATIONS[0]] is None
    c.paused = False
    held = c.dispatch_ready()
    assert [d.order_id for d in held] == ["A"]
    assert held[0].step == STATIONS[0]


def test_unreported_station_not_dispatched_until_first_report() -> None:
    c = SchedulerCore()
    c.start_run(RUN)
    spin = STATIONS[0]
    assert c.health[spin].reported is None
    assert c.dispatchable(spin) is False
    assert c.snapshot()["health"][spin] == (None, False)

    actions = c.on_order(rec("A"), ts(1))
    assert [a for a in actions if isinstance(a, Dispatch)] == []
    assert c.outstanding[spin] is None
    assert [e.order_id for e in c.backlog[spin]] == ["A"]

    # The first idle report releases the queued work immediately.
    actions = c.on_station_status(
        StationStatus.make(run_id=RUN, machine_id=spin, ts=ts(2), status="idle")
    )
    dispatched = [a for a in actions if isinstance(a, Dispatch)]
    assert [(d.order_id, d.step) for d in dispatched] == [("A", spin)]
    assert c.outstanding[spin] is not None and c.outstanding[spin].order_id == "A"
    assert c.backlog_lengths()[spin] == 0


def test_unreported_station_offline_recovery_does_not_echo_idle() -> None:
    c = SchedulerCore()
    c.start_run(RUN)
    spin = STATIONS[0]
    c.set_offline(spin, True, ts(1))
    out = c.set_offline(spin, False, ts(2))
    # No idle is emitted on behalf of a station that never reported, so it stays unreachable.
    assert not any(isinstance(a, EmitStationStatus) and a.status == "idle" for a in out)
    assert c.dispatchable(spin) is False


def _prio_core() -> SchedulerCore:
    c = SchedulerCore()
    c.start_run(RUN)
    for station in c.route:
        c.on_station_status(
            StationStatus.make(run_id=RUN, machine_id=station, ts=ts(0), status="idle")
        )
    return c


def test_set_priority_reorders_backlog_but_not_outstanding() -> None:
    c = _prio_core()
    for i, oid in enumerate(["A", "B", "C"]):
        c.on_order(rec(oid), ts(i + 1))
    assert c.snapshot()["outstanding"]["spin"] == "A"
    assert c.snapshot()["backlog"]["spin"] == ["B", "C"]
    assert c.set_priority("C", 5) is True
    assert c.snapshot()["backlog"]["spin"] == ["C", "B"]
    # outstanding lot: record updated, but dispatch state untouched
    assert c.set_priority("A", 99) is True
    assert c.snapshot()["outstanding"]["spin"] == "A"
    assert c.snapshot()["backlog"]["spin"] == ["C", "B"]
    assert c.orders["A"].record.priority == 99


def test_set_priority_unknown_or_terminal_returns_false() -> None:
    c = _prio_core()
    assert c.set_priority("ghost", 5) is False
    c.on_order(rec("A"), ts(1))
    c.orders["A"].emitted.add("completed")
    assert c.is_terminal("A") is True
    assert c.is_terminal("ghost") is False
    assert c.set_priority("A", 5) is False
    assert c.orders["A"].record.priority == 50
