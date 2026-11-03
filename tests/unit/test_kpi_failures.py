"""Phase 6 KPI metrics: failed/BANK WIP, cycle time, Little basis, availability."""

import uuid

import pytest

from pizza.common.envelope import (
    LotBanked,
    LotReleased,
    Message,
    OpCompleted,
    OpDispatched,
    OpFailed,
    OpStarted,
    OrderLifecycle,
    StationStatus,
)
from pizza.common.recipes import load_all_recipes
from pizza.kpi.fold import fold, new_state
from pizza.kpi.metrics import KpiConfig, _classify, advance
from tests.unit.kpi_fixtures import RUN, at, cooling_rework_log, shuffled


def build(msgs: list[Message]):
    state = new_state(RUN)
    for m in msgs:
        state = fold(state, m)
    return state


def snap_at(msgs: list[Message], when: float):
    return advance(build(msgs), at(when), load_all_recipes(), KpiConfig(warmup_orders=0))


def life(state: str, when: float, oid: str, created: float = 0.0, **extra):
    return OrderLifecycle.make(
        run_id=RUN,
        machine_id="scheduler",
        ts=at(when),
        state=state,
        order_id=oid,
        product="margherita",
        qty=1,
        due_ts=at(3600),
        created_ts=at(created),
        **extra,
    )


def banked(when: float, oid: str, reason: str = "capacity"):
    return LotBanked.make(
        run_id=RUN,
        machine_id="stocker",
        ts=at(when),
        order_id=oid,
        station="bake",
        reason=reason,
        source="manual",
    )


def released(when: float, oid: str):
    return LotReleased.make(
        run_id=RUN, machine_id="stocker", ts=at(when), order_id=oid, station="bake", by="manual"
    )


def status(station: str, when: float, st: str):
    return StationStatus.make(run_id=RUN, machine_id=station, ts=at(when), status=st)


def wip_scenario(bank_reason: str = "capacity") -> list[Message]:
    return [
        life("created", 0, "A"),
        life("released", 0, "A"),
        life("created", 0, "B"),
        life("released", 0, "B"),
        banked(5, "B", bank_reason),
        life("created", 0, "C"),
        life("failed", 4, "C", step="spin", visit=1),
        life("created", 0, "D"),
        life("completed", 10, "D"),
    ]


def test_wip_excludes_failed_and_open_bank() -> None:
    s = snap_at(wip_scenario("capacity"), 20)
    assert (s.wip_now, s.wip_bank, s.wip_failed, s.wip_bank_capacity) == (1, 1, 1, 1)
    s = snap_at(wip_scenario("customer"), 20)
    assert (s.wip_now, s.wip_bank, s.wip_bank_capacity) == (1, 1, 0)


def test_released_bank_returns_to_wip() -> None:
    s = snap_at([*wip_scenario(), released(8, "B")], 20)
    assert s.wip_now == 2
    assert s.wip_bank == 0


def test_failed_op_never_run_and_rework_wait_counts() -> None:
    cmd1, cmd2 = uuid.uuid4(), uuid.uuid4()
    op = {"order_id": "o1", "step": "spin", "attempt": 1, "qty": 1}
    msgs = [
        life("created", 0, "o1"),
        life("released", 0, "o1"),
        OpDispatched.make(
            run_id=RUN, machine_id="scheduler", ts=at(1), visit=1, command_event_id=cmd1,
            ready_ts=at(0), **op,
        ),
        OpStarted.make(
            run_id=RUN, machine_id="spin", ts=at(2), visit=1, redelivered=False,
            command_event_id=cmd1, **op,
        ),
        OpFailed.make(
            run_id=RUN, machine_id="spin", ts=at(5), visit=1, reason="boom",
            command_event_id=cmd1, **op,
        ),
    ]  # fmt: skip
    state = build(msgs)
    order = state.orders["o1"]
    steps = {"spin": [(v, r) for (o, st, v), r in state.ops.items() if st == "spin"]}
    assert _classify(order, steps) == ("spin", "wait")
    msgs.append(
        OpDispatched.make(
            run_id=RUN, machine_id="scheduler", ts=at(7), visit=2, command_event_id=cmd2,
            ready_ts=at(5), **op,
        )
    )  # fmt: skip
    s = snap_at(msgs, 8)
    assert s.stations["spin"].wip_run == 0
    assert s.stations["spin"].wait_mean_s == pytest.approx((1.0 + 2.0) / 2)


def _single(bank: list[Message]) -> list[Message]:
    return [life("created", 0, "x"), life("completed", 100, "x"), *bank]


def test_cycle_time_excludes_customer_bank_keeps_capacity() -> None:
    cust = snap_at(_single([banked(20, "x", "customer"), released(50, "x")]), 100)
    assert cust.cycle_time.mean_s == pytest.approx(70.0)
    cap = snap_at(_single([banked(20, "x", "capacity"), released(50, "x")]), 100)
    assert cap.cycle_time.mean_s == pytest.approx(100.0)


def test_little_basis_matches_cycle_time_basis() -> None:
    cust = snap_at(_single([banked(20, "x", "schedule"), released(50, "x")]), 100)
    assert cust.little.l == pytest.approx(0.7)
    assert cust.little.status == "pass"
    cap = snap_at(_single([banked(20, "x", "capacity"), released(50, "x")]), 100)
    assert cap.little.l == pytest.approx(1.0)
    assert cap.little.status == "pass"


def test_failed_order_leaves_little_area_at_failed_ts() -> None:
    msgs = [*_single([]), life("created", 0, "f"), life("failed", 40, "f", step="spin", visit=1)]
    s = snap_at(msgs, 100)
    assert s.little.n_in_system == 0
    assert s.wip_avg == pytest.approx((100 + 40) / 100)


def test_availability_and_utilization_exclude_down() -> None:
    msgs = [
        life("created", 0, "x"),
        status("bake", 0, "busy"),
        status("bake", 60, "down"),
        status("bake", 90, "busy"),
        status("bake", 120, "idle"),
    ]
    s = snap_at(msgs, 120)
    bake = s.stations["bake"]
    assert bake.utilization == pytest.approx(90 / 120)
    assert bake.availability == pytest.approx(90 / 120)
    assert bake.down_s == pytest.approx(30.0)
    assert s.stations["spin"].availability is None


def test_availability_one_without_down_and_open_down_counts() -> None:
    msgs = [life("created", 0, "x"), status("bake", 0, "busy"), status("bake", 50, "idle")]
    assert snap_at(msgs, 100).stations["bake"].availability == 1.0
    msgs = [life("created", 0, "x"), status("bake", 0, "busy"), status("bake", 50, "down")]
    bake = snap_at(msgs, 100).stations["bake"]
    assert bake.down_s == pytest.approx(50.0)
    assert bake.availability == pytest.approx(0.5)


def test_snapshot_is_order_insensitive() -> None:
    msgs = [
        *wip_scenario("customer"),
        released(8, "B"),
        banked(12, "B", "capacity"),
        status("bake", 0, "busy"),
        status("bake", 6, "down"),
        status("bake", 9, "busy"),
    ]
    ref = snap_at(msgs, 20).model_dump(exclude={"event_id"})
    for seed in range(8):
        got = snap_at(shuffled(msgs, seed), 20).model_dump(exclude={"event_id"})
        assert got == ref


# -- Phase 6.1: breakdown-caused op_failed (cause is ignored by the fold) ----
def _op(oid: str = "o1", visit: int = 1, cmd: uuid.UUID | None = None) -> dict:
    return {
        "order_id": oid,
        "step": "spin",
        "visit": visit,
        "attempt": 1,
        "qty": 1,
        "command_event_id": cmd or uuid.uuid4(),
    }


def _breakdown_rework_log(oid: str = "o1", complete_at: float = 40.0) -> list[Message]:
    """started v1 -> station down -> op_failed(breakdown) -> TTL gap -> v2 completes."""
    op1, op2 = _op(oid, 1), _op(oid, 2)
    return [
        life("created", 0, oid),
        life("released", 0, oid),
        OpStarted.make(
            run_id=RUN, machine_id="spin", ts=at(2), redelivered=False, product="margherita", **op1
        ),
        status("spin", 2, "busy"),
        status("spin", 10, "down"),
        OpFailed.make(
            run_id=RUN, machine_id="spin", ts=at(10), reason="breakdown", cause="breakdown", **op1
        ),
        OpDispatched.make(run_id=RUN, machine_id="scheduler", ts=at(25), ready_ts=at(10), **op2),
        status("spin", 25, "idle"),
        OpStarted.make(
            run_id=RUN, machine_id="spin", ts=at(26), redelivered=False, product="margherita", **op2
        ),
        OpCompleted.make(
            run_id=RUN, machine_id="spin", ts=at(30), duration_s=4.0, product="margherita", **op2
        ),
        life("completed", complete_at, oid),
    ]


def test_breakdown_failed_order_excluded_from_wip_and_throughput() -> None:
    op = _op("f")
    msgs = [
        *_single([]),
        life("created", 0, "f"),
        life("released", 0, "f"),
        OpStarted.make(run_id=RUN, machine_id="spin", ts=at(2), redelivered=False, **op),
        OpFailed.make(
            run_id=RUN, machine_id="spin", ts=at(10), reason="boom", cause="breakdown", **op
        ),
        life("failed", 12, "f", step="spin", visit=1),
    ]
    s = snap_at(msgs, 100)
    assert s.orders_completed == 1  # only "x"; the scrapped order is not throughput
    assert s.wip_now == 0
    assert s.wip_failed == 1
    assert s.little.n_in_system == 0
    same = snap_at(
        [m.model_copy(update={"cause": None}) if isinstance(m, OpFailed) else m for m in msgs],
        100,
    )
    assert same.model_dump(exclude={"event_id"}) == s.model_dump(exclude={"event_id"})


def test_reworked_after_breakdown_cycle_time_includes_outage_and_ttl() -> None:
    s = snap_at(_breakdown_rework_log(complete_at=40.0), 100)
    assert s.orders_completed == 1
    assert s.cycle_time.mean_s == pytest.approx(40.0)  # release 0 -> done 40, outage + TTL incl.
    assert s.wip_now == 0


def test_busy_down_idle_sequence_counts_only_busy_interval() -> None:
    msgs = [
        life("created", 0, "x"),
        status("spin", 0, "busy"),
        status("spin", 40, "down"),
        status("spin", 70, "idle"),
    ]
    spin = snap_at(msgs, 100).stations["spin"]
    assert spin.down_s == pytest.approx(30.0)
    assert spin.utilization == pytest.approx(40 / 100)  # busy only before down, none after repair
    assert spin.availability == pytest.approx(40 / 70)  # busy / (busy + down)


def test_littles_law_with_breakdown_rework() -> None:
    s = snap_at(_breakdown_rework_log(complete_at=100.0), 100)
    assert s.little.status == "pass"
    assert s.little.l == pytest.approx(1.0)
    assert s.little.w_s == pytest.approx(100.0)
    assert s.little.lambda_w == pytest.approx(s.little.l, rel=0.05)


# -- Phase 6.2 D-10: cooling rework visit (TTL lives in the scheduler hold queue) --
def test_cooling_rework_wait_includes_ttl_track_in_excludes_it() -> None:
    state = build(cooling_rework_log())
    rec2 = state.ops[("o1", "spin", 2)]
    assert (rec2.dispatched_ts - rec2.ready_ts).total_seconds() == pytest.approx(5.4)
    assert (rec2.started_ts - rec2.dispatched_ts).total_seconds() == pytest.approx(0.2)
    s = snap_at(cooling_rework_log(), 11)
    # v1 wait 1.0 / track-in 1.0; v2 wait 5.4 (TTL + queue) / track-in 0.2
    assert s.stations["spin"].wait_mean_s == pytest.approx((1.0 + 5.4) / 2)
    assert s.stations["spin"].track_in_mean_s == pytest.approx((1.0 + 0.2) / 2)


def test_cooling_rework_classify_wait_until_dispatch_then_track_in() -> None:
    def classify(msgs: list[Message]):
        state = build(msgs)
        steps = {"spin": [(v, r) for (o, st, v), r in state.ops.items() if st == "spin"]}
        return _classify(state.orders["o1"], steps)

    full = cooling_rework_log(with_v2_start=False)
    assert classify(full[:5]) == ("spin", "wait")  # failed, TTL still cooling
    assert classify(full) == ("spin", "track_in")  # dispatched, not yet started
    assert classify(cooling_rework_log()) == ("spin", "run")


def test_cooling_rework_cycle_time_includes_ttl() -> None:
    msgs = [*cooling_rework_log(), life("completed", 30, "o1")]
    s = snap_at(msgs, 30)
    assert s.little.w_s == pytest.approx(30.0)
