from datetime import UTC, datetime, timedelta

from pizza.common.envelope import OpCompleted, OpFailed, StationStatus
from pizza.common.ids import job_event_id
from pizza.scheduler.core import (
    CoreConfig,
    Dispatch,
    EmitHold,
    EmitLifecycle,
    EmitLot,
    OrderRecord,
    PublishDeadLetter,
    SchedulerCore,
)

RUN = "r1"
T0 = datetime(2026, 1, 1, tzinfo=UTC)


def ts(n: int) -> datetime:
    return T0 + timedelta(milliseconds=n)


def sec(n: float) -> datetime:
    return T0 + timedelta(seconds=n)


def rec(oid: str) -> OrderRecord:
    return OrderRecord(oid, "margherita", 2, T0 + timedelta(hours=1), T0)


def new_core(**kw) -> SchedulerCore:
    core = SchedulerCore(config=CoreConfig(**kw))
    core.start_run(RUN)
    # Every station reports idle first: a never-reported station is not dispatchable.
    for station in core.route:
        core.on_station_status(status(station, "idle", ts(-1)))
    return core


def status(station: str, st: str, t: datetime) -> StationStatus:
    return StationStatus.make(run_id=RUN, machine_id=station, ts=t, status=st)


def completed(order: str, step: str, n: int) -> OpCompleted:
    return OpCompleted.make(
        run_id=RUN,
        machine_id=step,
        ts=ts(n),
        order_id=order,
        step=step,
        visit=1,
        attempt=1,
        qty=2,
        command_event_id=job_event_id(RUN, order, step, 1),
        duration_s=0.0,
    )


def lots(actions, fact: str) -> list[EmitLot]:
    return [a for a in actions if isinstance(a, EmitLot) and a.fact == fact]


def dispatched(actions, step: str = "spin") -> list[str]:
    return [a.order_id for a in actions if isinstance(a, Dispatch) and a.step == step]


def test_overflow_stocked_beyond_queue_limit() -> None:
    core = new_core(queue_limit=2)
    acts = []
    for i, oid in enumerate(["o-1", "o-2", "o-3", "o-4", "o-5", "o-6"]):
        acts += core.on_order(rec(oid), ts(i + 1))
    # o-1 dispatched; backlog o-2..o-6, first two inside the limit.
    stocked = lots(acts, "stocked")
    assert [a.order_id for a in stocked] == ["o-4", "o-5", "o-6"]
    assert all(a.kind == "overflow" for a in stocked)
    assert set(core.stock_kind) == {"o-4", "o-5", "o-6"}
    # credit frees: o-2 dispatched, o-4 falls back inside the limit.
    acts = core.on_op_completed(completed("o-1", "spin", 50))
    assert dispatched(acts) == ["o-2"]
    assert [a.order_id for a in lots(acts, "unstocked")] == ["o-4"]
    assert set(core.stock_kind) == {"o-5", "o-6"}


def test_down_blocked_then_offline_then_recovery_order() -> None:
    core = new_core()
    core.on_station_status(status("bake", "down", ts(1)))
    for i, oid in enumerate(["A", "B", "C"]):
        core.on_order(rec(oid), ts(10 + i))
        for j, step in enumerate(("spin", "sauce", "cheese", "toppings")):
            core.on_op_completed(completed(oid, step, 100 * (i + 1) + j))
    assert core.snapshot()["backlog"]["bake"] == ["A", "B", "C"]
    assert core.snapshot()["stock"] == {
        "A": "station_down",
        "B": "station_down",
        "C": "station_down",
    }
    acts = core.set_offline("bake", True, ts(5000))
    assert [a.kind for a in lots(acts, "stocked")] == ["station_offline"] * 3
    core.set_offline("bake", False, ts(5001))
    acts = core.on_station_status(status("bake", "idle", ts(6000)))
    assert [a.order_id for a in lots(acts, "unstocked")] == ["A", "B", "C"]
    assert dispatched(acts, "bake") == ["A"]
    assert core.snapshot()["stock"] == {}


def test_manual_bank() -> None:
    core = new_core()
    for i, oid in enumerate(["o-1", "o-2", "o-3"]):
        core.on_order(rec(oid), ts(i + 1))
    acts = core.bank("o-2", "customer", ts(10))
    banked = lots(acts, "banked")
    assert len(banked) == 1 and banked[0].reason == "customer" and banked[0].source == "manual"
    assert [e.order_id for e in core.backlog["spin"]] == ["o-3"]
    assert core.snapshot()["banked"] == {"o-2": "customer"}
    # outstanding lot: a deferred hold is recorded (no bank yet), repeat is a no-op
    held = core.bank("o-1", "customer", ts(11))
    assert [(a.state, a.kind) for a in held if isinstance(a, EmitHold)] == [("requested", "bank")]
    assert "o-1" in core.holds and "o-1" not in core.banked
    assert core.bank("o-1", "customer", ts(11)) == []
    assert core.bank("o-2", "customer", ts(11)) == []  # already banked
    assert core.bank("nope", "customer", ts(11)) == []
    core.on_op_completed(completed("o-1", "spin", 20))
    assert dispatched(core.settle(ts(21))) == []
    assert core.outstanding["spin"].order_id == "o-3"
    # terminal orders cannot be banked
    st = core.orders["o-3"]
    st.emitted.add("failed")
    assert core.bank("o-3", "customer", ts(30)) == []




def test_manual_scrap_backlog_lot() -> None:
    core = new_core()
    for i, oid in enumerate(["o-1", "o-2", "o-3"]):
        core.on_order(rec(oid), ts(i + 1))
    acts = core.scrap("o-2", "quality", ts(10))
    dl = [a for a in acts if isinstance(a, PublishDeadLetter)]
    fl = [a for a in acts if isinstance(a, EmitLifecycle) and a.state == "failed"]
    assert len(dl) == 1 and dl[0].order_id == "o-2" and dl[0].reason == "manual_scrap:quality"
    assert len(fl) == 1 and fl[0].step == "spin" and fl[0].visit == 1
    assert [e.order_id for e in core.backlog["spin"]] == ["o-3"]
    assert "failed" in core.orders["o-2"].emitted and core.is_terminal("o-2")
    # idempotent / not scrappable: terminal, unknown; outstanding (dispatched) = deferred hold
    assert core.scrap("o-2", "quality", ts(11)) == []
    held = core.scrap("o-1", "quality", ts(11))
    assert [(a.state, a.kind) for a in held if isinstance(a, EmitHold)] == [("requested", "scrap")]
    assert "failed" not in core.orders["o-1"].emitted
    assert core.scrap("o-1", "quality", ts(11)) == []
    assert [a.state for a in core.cancel_hold("o-1", ts(12))] == ["cancelled"]
    assert core.scrap("nope", "quality", ts(11)) == []
    assert core.outstanding["spin"].order_id == "o-1"


def test_manual_scrap_banked_lot() -> None:
    core = new_core()
    for i, oid in enumerate(["o-1", "o-2"]):
        core.on_order(rec(oid), ts(i + 1))
    core.bank("o-2", "customer", ts(5))
    assert "o-2" in core.banked
    acts = core.scrap("o-2", "other", ts(6))
    assert any(isinstance(a, PublishDeadLetter) for a in acts)
    assert any(isinstance(a, EmitLifecycle) and a.state == "failed" for a in acts)
    assert core.banked == {}
    # a scrapped lot can no longer be released or banked
    assert core.release("o-2", ts(7)) == []
    assert core.bank("o-2", "customer", ts(7)) == []


def test_manual_scrap_stocked_lot_emits_unstocked() -> None:
    core = new_core(queue_limit=1)
    for i, oid in enumerate(["o-1", "o-2", "o-3", "o-4"]):
        core.on_order(rec(oid), ts(i + 1))
    assert "o-4" in core.stock_kind
    acts = core.scrap("o-4", "material", ts(10))
    assert [a.order_id for a in lots(acts, "unstocked")] == ["o-4"]
    assert "o-4" not in core.stock_kind


def test_threshold_off_by_default() -> None:
    core = new_core()
    for i in range(20):
        core.on_order(rec(f"o-{i}"), ts(i + 1))
    assert core.banked == {}
    assert len(core.backlog["spin"]) == 19


def test_rollback_and_backlog_lengths_exclude_banked() -> None:
    core = new_core(queue_limit=1)
    for i, oid in enumerate(["o-1", "o-2", "o-3", "o-4"]):
        core.on_order(rec(oid), ts(i + 1))
    core.bank("o-2", "schedule", ts(10))
    assert core.backlog_lengths()["spin"] == 2
    core.rollback_order("o-2")
    assert "o-2" not in core.banked and "o-2" not in core.orders
    core.rollback_order("o-4")
    assert "o-4" not in core.stock_kind


def test_stocker_rule_aware_classification() -> None:
    """Stocker classification follows the active dispatch rule (Pitfall 4)."""
    from pizza.common.constants import STATIONS

    recipes = {s: {"margherita": 1.0} for s in STATIONS}
    core = SchedulerCore(config=CoreConfig(queue_limit=2, recipes=recipes))
    core.start_run(RUN)
    # Every station reports idle first
    for station in core.route:
        core.on_station_status(status(station, "idle", ts(-1)))

    # Create 4 orders with different due_ts
    # A: due at T0 + 8h, B: due at T0 + 2h, C: due at T0 + 5h, D: due at T0 + 3h
    due_a = T0 + timedelta(hours=8)
    due_b = T0 + timedelta(hours=2)
    due_c = T0 + timedelta(hours=5)
    due_d = T0 + timedelta(hours=3)

    # Switch to EDD
    core.set_rule("edd")

    core.on_order(OrderRecord("A", "margherita", 1, due_a, T0), ts(1))
    core.on_order(OrderRecord("B", "margherita", 1, due_b, T0), ts(2))
    core.on_order(OrderRecord("C", "margherita", 1, due_c, T0), ts(3))
    core.on_order(OrderRecord("D", "margherita", 1, due_d, T0), ts(4))

    # Under EDD with queue_limit=2:
    # - A is outstanding
    # - In backlog by EDD: B (due 2h), D (due 3h) should be in queue (first 2 in EDD order)
    # - C (due 5h) should be overflow
    backlog = core.snapshot()["backlog"]["spin"]
    # Backlog should be sorted by EDD: B (due 2h), D (due 3h), C (due 5h)
    assert backlog == ["B", "D", "C"]

    # Check stock classification: only overflow items are tracked in stock_kind
    stock = core.snapshot()["stock"]
    assert stock.get("C") == "overflow"
    assert "B" not in stock  # B is in queue, not stocked
    assert "D" not in stock  # D is in queue, not stocked


def test_set_rule_changes_dispatch_order() -> None:
    """set_rule changes the active dispatch rule (D-04)."""
    core = new_core()

    # Verify set_rule works
    assert core.rule == "edd"
    core.set_rule("fifo")
    assert core.rule == "fifo"
    core.set_rule("edd")
    assert core.rule == "edd"
    core.set_rule("spt")
    assert core.rule == "spt"

    # Invalid rules raise ValueError
    try:
        core.set_rule("invalid")
        raise AssertionError("should raise ValueError")
    except ValueError:
        pass


def _fail(order: str, n: int) -> OpFailed:
    return OpFailed.make(
        run_id=RUN,
        machine_id="spin",
        ts=ts(n),
        order_id=order,
        step="spin",
        visit=1,
        attempt=1,
        qty=2,
        command_event_id=job_event_id(RUN, order, "spin", 1),
        reason="boom",
    )


def test_cooling_lot_not_in_backlog_or_queue_limit() -> None:
    core = new_core(queue_limit=2)
    for i, oid in enumerate(["o-1", "o-2", "o-3", "o-4"]):
        core.on_order(rec(oid), ts(i + 1))
    # o-1 outstanding, o-2/o-3 inside the limit, o-4 overflow
    assert set(core.stock_kind) == {"o-4"}
    acts = core.on_op_failed(_fail("o-1", 10))
    assert "o-1" in core.cooling
    assert all(e.order_id != "o-1" for e in core.backlog["spin"])
    assert "o-1" not in core.stock_kind
    assert core.snapshot()["cooling"] == {"o-1": ("spin", 2)}
    # cooling lots are held by the scheduler: manual bank acts immediately
    assert [a.fact for a in core.bank("o-1", "customer", ts(11)) if isinstance(a, EmitLot)][
        :1
    ] == ["banked"]
    assert "o-1" in core.banked and "o-1" not in core.cooling
    assert dispatched(acts) == ["o-2"]
    assert core.backlog_lengths()["spin"] == 2  # cooling lot not counted
    assert set(core.stock_kind) == set()  # o-3, o-4 now within the limit of 2


def test_rework_return_uses_enter_backlog() -> None:
    core = new_core(bank_wait_threshold=1)
    core.on_order(rec("o-1"), ts(1))
    core.on_order(rec("o-2"), ts(2))
    core.on_op_failed(_fail("o-1", 10))  # o-2 dispatched
    core.on_order(rec("o-3"), ts(11))  # backlog now [o-3]
    # returning o-1 goes through _enter_backlog: backlog already at threshold -> auto BANK
    acts = core.on_rework_ready("o-1", "spin", 2, RUN, ts(20))
    banked = lots(acts, "banked")
    assert [a.order_id for a in banked] == ["o-1"]
    assert banked[0].source == "auto_wait"
    assert "o-1" in core.banked
    assert "o-1" not in core.cooling


def test_set_priority_changes_overflow_set_on_settle() -> None:
    core = new_core(queue_limit=2)
    for i, oid in enumerate(["o-1", "o-2", "o-3", "o-4", "o-5"]):
        core.on_order(rec(oid), ts(i + 1))
    # o-1 dispatched; backlog o-2,o-3 inside the limit, o-4,o-5 stocked.
    assert set(core.stock_kind) == {"o-4", "o-5"}
    assert core.set_priority("o-5", 1) is True
    acts = core.settle(ts(100))
    assert [a.order_id for a in lots(acts, "unstocked")] == ["o-5"]
    assert [a.order_id for a in lots(acts, "stocked")] == ["o-3"]
    assert set(core.stock_kind) == {"o-3", "o-4"}
