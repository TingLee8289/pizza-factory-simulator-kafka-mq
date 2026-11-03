"""Phase 6 fold tests: failed orders, op failures, BANK intervals, heartbeats."""

import uuid
from typing import Any

from pizza.common.envelope import (
    LotBanked,
    LotReleased,
    Message,
    OpFailed,
    OpStarted,
    OrderLifecycle,
    StationHeartbeat,
    StationStatus,
)
from pizza.kpi.fold import KpiState, fold, new_state
from tests.unit.kpi_fixtures import RUN, at, shuffled


def fold_all(msgs: list[Message]) -> KpiState:
    state = new_state(RUN)
    for m in msgs:
        state = fold(state, m)
    return state


def life(state_name: str, when: float, oid: str = "o1", **extra: Any) -> Message:
    return OrderLifecycle.make(
        run_id=RUN,
        machine_id="scheduler",
        ts=at(when),
        state=state_name,
        order_id=oid,
        product="margherita",
        qty=1,
        due_ts=at(3600),
        created_ts=at(0),
        **extra,
    )


def banked(when: float, reason: str = "capacity", oid: str = "o1") -> Message:
    return LotBanked.make(
        run_id=RUN,
        machine_id="stocker",
        ts=at(when),
        order_id=oid,
        station="bake",
        reason=reason,
        source="manual",
    )


def released(when: float, oid: str = "o1") -> Message:
    return LotReleased.make(
        run_id=RUN, machine_id="stocker", ts=at(when), order_id=oid, station="bake", by="manual"
    )


def test_failed_lifecycle_sets_min_failed_ts_and_idempotent() -> None:
    f = life("failed", 30.0, step="spin", visit=1)
    f2 = life("failed", 20.0, step="spin", visit=1)
    state = fold_all([life("created", 0.0), f, f, f2])
    assert state.orders["o1"].failed_ts == at(20.0)


def test_op_failed_sets_failed_ts_only() -> None:
    op = {
        "order_id": "o1",
        "step": "spin",
        "visit": 1,
        "attempt": 1,
        "qty": 1,
        "command_event_id": uuid.uuid4(),
    }
    msgs = [
        OpStarted.make(run_id=RUN, machine_id="spin", ts=at(1), redelivered=False, **op),
        OpFailed.make(run_id=RUN, machine_id="spin", ts=at(5), reason="boom", **op),
    ]
    rec = fold_all(msgs).ops[("o1", "spin", 1)]
    assert rec.started_ts == at(1)
    assert rec.failed_ts == at(5)
    assert rec.completed_ts is None


def test_bank_intervals_open_close_reopen_and_order_insensitive() -> None:
    msgs = [life("created", 0.0), banked(10, "customer"), released(20), banked(30, "capacity")]
    ordered = fold_all(msgs).orders["o1"].bank_intervals
    assert [(i.start, i.end, i.reason) for i in ordered] == [
        (at(10), at(20), "customer"),
        (at(30), None, "capacity"),
    ]
    for seed in range(10):
        assert fold_all(shuffled(msgs, seed)).orders["o1"].bank_intervals == ordered


def test_heartbeat_ignored_and_down_status_logged() -> None:
    hb = StationHeartbeat.make(run_id=RUN, machine_id="bake", ts=at(1), status="idle")
    assert fold_all([hb]).status_logs == {}
    down = StationStatus.make(run_id=RUN, machine_id="bake", ts=at(2), status="down")
    assert fold_all([down]).status_logs["bake"][0][1] == "down"


def test_lot_facts_for_unknown_order_create_no_order_rec() -> None:
    assert fold_all([banked(1), released(2)]).orders == {}
