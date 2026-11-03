"""Rebuild-from-Kafka behaviour of deferred manual bank / scrap (quick 261023-pfp)."""

import random
from datetime import UTC, datetime, timedelta

from pizza.common.constants import STATIONS
from pizza.common.envelope import (
    LotBanked,
    LotHold,
    LotReleased,
    Message,
    OpCompleted,
    OpDispatched,
    OpFailed,
    OrderLifecycle,
    StationStatus,
)
from pizza.common.ids import job_event_id
from pizza.scheduler.core import (
    EmitHold,
    EmitLifecycle,
    EmitLot,
    PublishDeadLetter,
    PublishRework,
    SchedulerCore,
)

RUN = "r1"
T0 = datetime(2026, 1, 1, tzinfo=UTC)
LAST = STATIONS[-1]


def ts(n: int) -> datetime:
    return T0 + timedelta(milliseconds=n)


def lifecycle(state: str, n: int, oid: str = "A") -> OrderLifecycle:
    return OrderLifecycle.make(
        run_id=RUN,
        machine_id="scheduler",
        ts=ts(n),
        state=state,
        order_id=oid,
        product="margherita",
        qty=2,
        due_ts=T0 + timedelta(hours=1),
        created_ts=T0,
    )


def dispatched(step: str, n: int, oid: str = "A") -> OpDispatched:
    return OpDispatched.make(
        run_id=RUN,
        machine_id="scheduler",
        ts=ts(n),
        order_id=oid,
        step=step,
        visit=1,
        qty=2,
        product="margherita",
        command_event_id=job_event_id(RUN, oid, step, 1),
        ready_ts=ts(n),
    )


def completed(step: str, n: int, oid: str = "A") -> OpCompleted:
    return OpCompleted.make(
        run_id=RUN,
        machine_id=step,
        ts=ts(n),
        order_id=oid,
        step=step,
        visit=1,
        attempt=1,
        qty=2,
        command_event_id=job_event_id(RUN, oid, step, 1),
        duration_s=0.0,
    )


def failed(step: str, n: int, oid: str = "A") -> OpFailed:
    return OpFailed.make(
        run_id=RUN,
        machine_id=step,
        ts=ts(n),
        order_id=oid,
        step=step,
        visit=1,
        attempt=1,
        qty=2,
        command_event_id=job_event_id(RUN, oid, step, 1),
        reason="boom",
    )


def hold(state: str, step: str, n: int, kind: str = "bank", oid: str = "A") -> LotHold:
    return LotHold.make(
        run_id=RUN,
        machine_id="scheduler",
        ts=ts(n),
        order_id=oid,
        station=step,
        state=state,
        kind=kind,
        reason="customer" if kind == "bank" else "quality",
    )


def banked(step: str, n: int, oid: str = "A") -> LotBanked:
    return LotBanked.make(
        run_id=RUN,
        machine_id="scheduler",
        ts=ts(n),
        order_id=oid,
        station=step,
        reason="customer",
        source="manual",
    )


def idle() -> list[Message]:
    return [StationStatus.make(run_id=RUN, machine_id=s, ts=ts(0), status="idle") for s in STATIONS]


def base(upto: int = 0) -> list[Message]:
    """Order A released, ops completed at STATIONS[:upto], dispatched at STATIONS[upto]."""
    facts: list[Message] = [*idle(), lifecycle("released", 1)]
    n = 2
    for step in STATIONS[:upto]:
        facts += [dispatched(step, n), completed(step, n + 1)]
        n += 2
    if upto < len(STATIONS):
        facts.append(dispatched(STATIONS[upto], n))
    return facts


def rebuild(facts: list[Message]):
    return SchedulerCore.rebuild(RUN, facts)


def test_pending_hold_restored_and_outstanding_preserved() -> None:
    core, rec = rebuild([*base(), hold("requested", "spin", 10)])
    assert core.snapshot()["holds"] == {"A": ("bank", "spin")}
    assert core.outstanding["spin"].order_id == "A"
    assert not [a for a in rec if isinstance(a, EmitHold)]


def test_cancelled_hold_not_restored() -> None:
    facts = [*base(), hold("requested", "spin", 10), hold("cancelled", "spin", 11)]
    core, _ = rebuild(facts)
    assert core.holds == {}
    assert core.outstanding["spin"].order_id == "A"


def test_tie_between_requested_and_cancelled_is_cleared() -> None:
    facts = [*base(), hold("requested", "spin", 10), hold("cancelled", "spin", 10)]
    core, _ = rebuild(facts)
    assert core.holds == {}
    facts = [*base(), hold("cancelled", "spin", 10), hold("requested", "spin", 10)]
    core, _ = rebuild(facts)
    assert core.holds == {}


def test_shuffled_hold_facts_same_result() -> None:
    facts = [*base(), hold("requested", "spin", 10)]
    rng = random.Random(5)
    for _ in range(10):
        rng.shuffle(facts)
        core, _ = rebuild(list(facts))
        assert core.snapshot()["holds"] == {"A": ("bank", "spin")}


def test_hold_op_completed_without_applied_reapplies_bank() -> None:
    facts = [*base(), hold("requested", "spin", 10), completed("spin", 11)]
    core, rec = rebuild(facts)
    assert [a.state for a in rec if isinstance(a, EmitHold)] == ["applied"]
    banks = [a for a in rec if isinstance(a, EmitLot) and a.fact == "banked"]
    assert [(b.station, b.source) for b in banks] == [("sauce", "manual")]
    assert core.banked["A"].station == "sauce"
    assert core.backlog["sauce"] == [] and core.outstanding["sauce"] is None
    assert core.holds == {}


def test_hold_op_completed_without_applied_reapplies_scrap() -> None:
    facts = [*base(), hold("requested", "spin", 10, "scrap"), completed("spin", 11)]
    core, rec = rebuild(facts)
    assert [a for a in rec if isinstance(a, PublishDeadLetter)][0].reason == "manual_scrap:quality"
    assert [a.state for a in rec if isinstance(a, EmitLifecycle) and a.state != "in_process"] == [
        "failed"
    ]
    assert core.is_terminal("A")


def test_applied_and_banked_midroute_not_reapplied() -> None:
    facts = [
        *base(),
        hold("requested", "spin", 10),
        completed("spin", 11),
        hold("applied", "spin", 11),
        banked("sauce", 11),
    ]
    core, rec = rebuild(facts)
    assert core.banked["A"].station == "sauce"
    assert not [a for a in rec if isinstance(a, EmitHold)]
    assert not [a for a in rec if isinstance(a, EmitLot) and a.fact == "banked"]


def test_applied_but_bank_fact_lost_is_reapplied() -> None:
    facts = [
        *base(),
        hold("requested", "spin", 10),
        completed("spin", 11),
        hold("applied", "spin", 11),
    ]
    core, rec = rebuild(facts)
    assert core.banked["A"].station == "sauce"
    assert [a.fact for a in rec if isinstance(a, EmitLot)][:1] == ["banked"]


def test_last_station_end_bank_restored_without_completed() -> None:
    n = len(STATIONS)
    facts = [
        *base(n - 1),
        hold("requested", LAST, 100),
        completed(LAST, 101),
        hold("applied", LAST, 101),
        banked(LAST, 101),
    ]
    core, rec = rebuild(facts)
    assert core.banked["A"].entry.step_index == n
    assert core.banked["A"].station == LAST
    assert "completed" not in core.orders["A"].emitted
    assert not [a for a in rec if isinstance(a, EmitLifecycle) and a.state == "completed"]
    assert all(core.backlog[s] == [] for s in STATIONS)


def test_last_station_completed_without_applied_reapplies_end_bank() -> None:
    n = len(STATIONS)
    facts = [*base(n - 1), hold("requested", LAST, 100), completed(LAST, 101)]
    core, rec = rebuild(facts)
    assert core.banked["A"].entry.step_index == n
    assert [
        (a.station, a.source) for a in rec if isinstance(a, EmitLot) and a.fact == "banked"
    ] == [(LAST, "manual")]
    assert not [a for a in rec if isinstance(a, EmitLifecycle) and a.state == "completed"]


def test_released_end_bank_rebuilds_as_completed() -> None:
    n = len(STATIONS)
    facts = [
        *base(n - 1),
        hold("requested", LAST, 100),
        completed(LAST, 101),
        hold("applied", LAST, 101),
        banked(LAST, 101),
        LotReleased.make(
            run_id=RUN, machine_id="scheduler", ts=ts(110), order_id="A", station=LAST, by="manual"
        ),
        lifecycle("completed", 110),
    ]
    core, rec = rebuild(facts)
    assert core.banked == {}
    assert core.is_terminal("A")
    assert not [a for a in rec if isinstance(a, EmitLifecycle) and a.state != "in_process"]


def test_hold_applied_on_op_failed_rebuilds_banked_at_failed_station() -> None:
    facts = [
        *base(),
        hold("requested", "spin", 10),
        failed("spin", 11),
        hold("applied", "spin", 11),
        banked("spin", 11),
    ]
    core, rec = rebuild(facts)
    assert core.banked["A"].station == "spin" and core.banked["A"].entry.visit == 2
    assert core.cooling == {}
    assert not [a for a in rec if isinstance(a, PublishRework)]


def test_hold_with_op_failed_but_nothing_applied_banks_instead_of_cooling() -> None:
    facts = [*base(), hold("requested", "spin", 10), failed("spin", 11)]
    core, rec = rebuild(facts)
    assert core.cooling == {}
    assert core.banked["A"].station == "spin"
    assert not [a for a in rec if isinstance(a, PublishRework)]
    assert [a.state for a in rec if isinstance(a, EmitHold)] == ["applied"]
