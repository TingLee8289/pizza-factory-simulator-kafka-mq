"""Deferred manual bank / scrap (quick 261023-pfp): pure scheduler core tests."""

from datetime import UTC, datetime, timedelta

from pizza.common.envelope import OpCompleted, OpFailed, StationStatus
from pizza.common.ids import job_event_id
from pizza.scheduler.core import (
    CoreConfig,
    EmitHold,
    EmitLifecycle,
    EmitLot,
    OrderRecord,
    PublishDeadLetter,
    PublishRework,
    SchedulerCore,
)

RUN = "r1"
T0 = datetime(2026, 1, 1, tzinfo=UTC)


def ts(n: int) -> datetime:
    return T0 + timedelta(milliseconds=n)


def rec(oid: str) -> OrderRecord:
    return OrderRecord(oid, "margherita", 2, T0 + timedelta(hours=1), T0)


def new_core(**kw) -> SchedulerCore:
    core = SchedulerCore(config=CoreConfig(**kw))
    core.start_run(RUN)
    for station in core.route:
        core.on_station_status(
            StationStatus.make(run_id=RUN, machine_id=station, ts=ts(-1), status="idle")
        )
    return core


def completed(order: str, step: str, n: int, visit: int = 1) -> OpCompleted:
    return OpCompleted.make(
        run_id=RUN,
        machine_id=step,
        ts=ts(n),
        order_id=order,
        step=step,
        visit=visit,
        attempt=1,
        qty=2,
        command_event_id=job_event_id(RUN, order, step, visit),
        duration_s=0.0,
    )


def failed(order: str, step: str, n: int, visit: int = 1) -> OpFailed:
    return OpFailed.make(
        run_id=RUN,
        machine_id=step,
        ts=ts(n),
        order_id=order,
        step=step,
        visit=visit,
        attempt=1,
        qty=2,
        command_event_id=job_event_id(RUN, order, step, visit),
        reason="boom",
    )


def holds(actions, state: str | None = None) -> list[EmitHold]:
    return [a for a in actions if isinstance(a, EmitHold) and (state is None or a.state == state)]


def lot_facts(actions, fact: str) -> list[EmitLot]:
    return [a for a in actions if isinstance(a, EmitLot) and a.fact == fact]


def finish_until(core: SchedulerCore, oid: str, upto: int, n0: int = 100) -> int:
    """Complete ``oid`` at stations route[0..upto-1]; returns the next ts counter."""
    n = n0
    for step in core.route[:upto]:
        core.on_op_completed(completed(oid, step, n))
        n += 1
    return n


def test_bank_outstanding_records_hold_and_is_idempotent() -> None:
    core = new_core()
    core.on_order(rec("A"), ts(1))
    acts = core.bank("A", "customer", ts(2))
    h = holds(acts)
    assert [(a.state, a.kind, a.station, a.reason) for a in h] == [
        ("requested", "bank", "spin", "customer")
    ]
    assert "A" not in core.banked
    assert core.snapshot()["holds"] == {"A": ("bank", "spin")}
    assert core.bank("A", "customer", ts(3)) == []  # same kind: no-op
    assert core.scrap("A", "operator", ts(3)) == []  # other kind while pending: rejected
    assert core.holds["A"].kind == "bank"


def test_unknown_terminal_and_prerelease_blocked() -> None:
    core = new_core()
    assert core.bank("nope", "customer", ts(1)) == []
    assert core.scrap("nope", "operator", ts(1)) == []
    core.on_order(rec("A"), ts(2))
    core.orders["A"].emitted.add("failed")
    assert core.bank("A", "customer", ts(3)) == []
    assert core.scrap("A", "operator", ts(3)) == []
    assert core.cancel_hold("A", ts(4)) == []


def test_cancel_hold() -> None:
    core = new_core()
    core.on_order(rec("A"), ts(1))
    core.bank("A", "schedule", ts(2))
    acts = core.cancel_hold("A", ts(3))
    assert [(a.state, a.kind) for a in holds(acts)] == [("cancelled", "bank")]
    assert core.holds == {}
    assert core.cancel_hold("A", ts(4)) == []
    # the op completes normally afterwards: the lot goes to the next backlog
    core.on_op_completed(completed("A", "spin", 10))
    assert core.outstanding["sauce"].order_id == "A"
    assert core.banked == {}


def test_deferred_bank_applied_on_completion_mid_route() -> None:
    core = new_core()
    core.on_order(rec("A"), ts(1))
    core.bank("A", "customer", ts(2))
    acts = core.on_op_completed(completed("A", "spin", 10))
    assert [a.state for a in holds(acts)] == ["applied"]
    banked = lot_facts(acts, "banked")
    assert len(banked) == 1
    assert (banked[0].station, banked[0].source, banked[0].reason) == (
        "sauce",
        "manual",
        "customer",
    )
    assert acts.index(holds(acts)[0]) < acts.index(banked[0])
    assert core.orders["A"].next_index == 1
    assert core.banked["A"].entry.step_index == 1
    assert core.backlog["sauce"] == [] and core.outstanding["sauce"] is None
    assert core.holds == {}
    # releasing continues into the sauce backlog / dispatch
    rel = core.release("A", ts(20))
    assert lot_facts(rel, "released")
    assert core.outstanding["sauce"].order_id == "A"


def test_deferred_scrap_applied_on_completion_mid_route() -> None:
    core = new_core()
    core.on_order(rec("A"), ts(1))
    core.scrap("A", "quality", ts(2))
    acts = core.on_op_completed(completed("A", "spin", 10))
    assert [a.state for a in holds(acts)] == ["applied"]
    dl = [a for a in acts if isinstance(a, PublishDeadLetter)]
    fl = [a for a in acts if isinstance(a, EmitLifecycle) and a.state == "failed"]
    assert len(dl) == 1 and dl[0].reason == "manual_scrap:quality"
    assert len(fl) == 1 and fl[0].step == "sauce"
    assert core.is_terminal("A") and core.outstanding["sauce"] is None
    assert core.backlog["sauce"] == []


def test_manual_hold_beats_auto_wait_bank() -> None:
    core = new_core(bank_wait_threshold=1)
    core.on_station_status(
        StationStatus.make(run_id=RUN, machine_id="sauce", ts=ts(0), status="down")
    )
    core.on_order(rec("A"), ts(1))
    core.on_order(rec("B"), ts(2))
    core.on_op_completed(completed("A", "spin", 10))  # sauce backlog now [A] (at threshold)
    core.bank("B", "customer", ts(11))
    acts = core.on_op_completed(completed("B", "spin", 12))
    banked = lot_facts(acts, "banked")
    assert [(b.order_id, b.source) for b in banked] == [("B", "manual")]


def test_last_station_deferred_bank_parks_then_release_ships() -> None:
    core = new_core()
    core.on_order(rec("A"), ts(1))
    n = finish_until(core, "A", len(core.route) - 1)
    last = core.route[-1]
    assert core.outstanding[last].order_id == "A"
    core.bank("A", "schedule", ts(n))
    acts = core.on_op_completed(completed("A", last, n + 1))
    assert not [a for a in acts if isinstance(a, EmitLifecycle) and a.state == "completed"]
    banked = lot_facts(acts, "banked")
    assert [(b.station, b.source) for b in banked] == [(last, "manual")]
    rec_ = core.banked["A"]
    assert rec_.entry.step_index == len(core.route) and rec_.station == last
    assert "completed" not in core.orders["A"].emitted
    rel = core.release("A", ts(n + 5))
    assert [a.fact for a in rel if isinstance(a, EmitLot)] == ["released"]
    comp = [a for a in rel if isinstance(a, EmitLifecycle) and a.state == "completed"]
    assert len(comp) == 1
    assert "A" not in core.banked
    assert all(core.backlog[s] == [] for s in core.route)
    assert core.is_terminal("A")
    assert core.release("A", ts(n + 6)) == []


def test_last_station_deferred_scrap_fails_not_completes() -> None:
    core = new_core()
    core.on_order(rec("A"), ts(1))
    n = finish_until(core, "A", len(core.route) - 1)
    last = core.route[-1]
    core.scrap("A", "material", ts(n))
    acts = core.on_op_completed(completed("A", last, n + 1))
    assert not [a for a in acts if isinstance(a, EmitLifecycle) and a.state == "completed"]
    fl = [a for a in acts if isinstance(a, EmitLifecycle) and a.state == "failed"]
    assert len(fl) == 1 and fl[0].step == last
    assert "completed" not in core.orders["A"].emitted and "failed" in core.orders["A"].emitted


def test_op_failed_rework_with_hold_applies_instead_of_cooling() -> None:
    core = new_core()
    core.on_order(rec("A"), ts(1))
    core.bank("A", "customer", ts(2))
    acts = core.on_op_failed(failed("A", "spin", 10), ts(11))
    assert not [a for a in acts if isinstance(a, PublishRework)]
    assert "A" not in core.cooling
    assert [a.state for a in holds(acts)] == ["applied"]
    banked = lot_facts(acts, "banked")
    assert [(b.station, b.source) for b in banked] == [("spin", "manual")]
    assert core.banked["A"].entry.visit == 2
    assert core.outstanding["spin"] is None
    # a later rework-ready message is a no-op
    assert core.on_rework_ready("A", "spin", 2, RUN, ts(30)) == []


def test_op_failed_rework_with_scrap_hold() -> None:
    core = new_core()
    core.on_order(rec("A"), ts(1))
    core.scrap("A", "other", ts(2))
    acts = core.on_op_failed(failed("A", "spin", 10), ts(11))
    assert not [a for a in acts if isinstance(a, PublishRework)]
    assert [a for a in acts if isinstance(a, PublishDeadLetter)][0].reason == "manual_scrap:other"
    assert "A" not in core.cooling and core.is_terminal("A")


def test_op_failed_exhausted_clears_hold() -> None:
    core = new_core()
    core.on_order(rec("A"), ts(1))
    core.on_op_failed(failed("A", "spin", 5), ts(5))
    core.on_rework_ready("A", "spin", 2, RUN, ts(6))
    core.on_op_failed(failed("A", "spin", 7, 2), ts(7))
    core.on_rework_ready("A", "spin", 3, RUN, ts(8))
    assert core.outstanding["spin"].visit == 3
    core.bank("A", "customer", ts(9))
    assert "A" in core.holds
    acts = core.on_op_failed(failed("A", "spin", 10, 3), ts(10))
    assert [a.state for a in holds(acts)] == ["cancelled"]
    assert core.holds == {}
    assert [a for a in acts if isinstance(a, PublishDeadLetter)]
    assert core.is_terminal("A")


def test_cooling_lot_bank_and_scrap_are_immediate() -> None:
    core = new_core()
    core.on_order(rec("A"), ts(1))
    core.on_order(rec("B"), ts(2))
    core.on_op_failed(failed("A", "spin", 5), ts(5))
    assert "A" in core.cooling
    acts = core.bank("A", "customer", ts(7))
    assert [(b.station, b.source) for b in lot_facts(acts, "banked")] == [("spin", "manual")]
    assert "A" not in core.cooling and "A" in core.banked
    assert core.on_rework_ready("A", "spin", 2, RUN, ts(8)) == []
    assert holds(acts) == [] and core.holds == {}
    # scrap on a cooling lot
    core2 = new_core()
    core2.on_order(rec("A"), ts(1))
    core2.on_op_failed(failed("A", "spin", 5), ts(5))
    acts2 = core2.scrap("A", "quality", ts(7))
    assert [a for a in acts2 if isinstance(a, PublishDeadLetter)][0].visit == 2
    assert core2.is_terminal("A") and "A" not in core2.cooling
    assert core2.on_rework_ready("A", "spin", 2, RUN, ts(8)) == []


def test_banked_lot_can_still_be_scrapped_and_rollback_clears_hold() -> None:
    core = new_core()
    core.on_order(rec("A"), ts(1))
    core.on_order(rec("B"), ts(2))
    core.bank("B", "customer", ts(3))
    core.scrap("B", "operator", ts(4))
    assert core.is_terminal("B")
    core.bank("A", "customer", ts(5))
    core.rollback_order("A")
    assert core.holds == {}
