import uuid
from datetime import UTC, datetime, timedelta

from pizza.common.envelope import (
    OpCompleted,
    OpFailed,
    StationHeartbeat,
    StationStatus,
)
from pizza.common.ids import job_event_id
from pizza.scheduler.core import (
    Dispatch,
    EmitLifecycle,
    EmitLot,
    EmitStationStatus,
    OrderRecord,
    PublishDeadLetter,
    PublishRework,
    SchedulerCore,
)

RUN = "r1"
T0 = datetime(2026, 1, 1, tzinfo=UTC)


def ts(n: int) -> datetime:
    return T0 + timedelta(milliseconds=n)


def rec(oid: str, **kw) -> OrderRecord:
    return OrderRecord(oid, "margherita", 2, T0 + timedelta(hours=1), T0, **kw)


def ready(core: SchedulerCore, n: int = -1) -> None:
    """Every station reports idle first: a never-reported station is not dispatchable."""
    for station in core.route:
        core.on_station_status(
            StationStatus.make(run_id=RUN, machine_id=station, ts=ts(n), status="idle")
        )


def new_core() -> SchedulerCore:
    core = SchedulerCore()
    core.start_run(RUN)
    ready(core)
    return core


def status(station: str, st: str, n: int, run: str = RUN) -> StationStatus:
    return StationStatus.make(run_id=run, machine_id=station, ts=ts(n), status=st)


def hb(station: str, st: str, n: int, run: str = RUN) -> StationHeartbeat:
    return StationHeartbeat.make(run_id=run, machine_id=station, ts=ts(n), status=st)


def completed(order: str, step: str, visit: int, n: int, cmd=None) -> OpCompleted:
    return OpCompleted.make(
        run_id=RUN,
        machine_id=step,
        ts=ts(n),
        order_id=order,
        step=step,
        visit=visit,
        attempt=1,
        qty=2,
        command_event_id=cmd or job_event_id(RUN, order, step, visit),
        duration_s=0.0,
    )


def failed(
    order: str, step: str, visit: int, n: int, run: str = RUN, cause: str | None = None
) -> OpFailed:
    return OpFailed.make(
        run_id=run,
        machine_id=step,
        ts=ts(n),
        order_id=order,
        step=step,
        visit=visit,
        attempt=1,
        qty=2,
        command_event_id=job_event_id(RUN, order, step, visit),
        reason="boom",
        cause=cause,
    )


def cycle_rework(core: SchedulerCore, oid: str, step: str, failed_visit: int, n: int) -> list:
    """Simulate the hold queue returning a reworked lot after a failure at ``failed_visit``."""
    return core.on_rework_ready(oid, step, failed_visit + 1, RUN, ts(n + 5000))


def to_bake(core: SchedulerCore, oid: str) -> None:
    """Advance an order until it waits in the bake backlog / is dispatched there."""
    core.on_order(rec(oid), ts(1))
    n = 10
    for step in ("spin", "sauce", "cheese", "toppings"):
        core.on_op_completed(completed(oid, step, 1, n))
        n += 1


# -- task 1 -----------------------------------------------------------------
def test_down_station_not_dispatched_until_idle() -> None:
    core = new_core()
    core.on_station_status(status("bake", "down", 5))
    to_bake(core, "A")
    assert core.outstanding["bake"] is None
    assert [e.order_id for e in core.backlog["bake"]] == ["A"]
    actions = core.on_station_status(status("bake", "idle", 50))
    assert [a.step for a in actions if isinstance(a, Dispatch)] == ["bake"]
    assert core.outstanding["bake"].order_id == "A"


def test_other_stations_still_dispatch_when_one_down() -> None:
    core = new_core()
    core.on_station_status(status("bake", "down", 5))
    actions = core.on_order(rec("A"), ts(6))
    assert any(isinstance(a, Dispatch) and a.step == "spin" for a in actions)


def test_down_keeps_credit_and_late_completion_releases() -> None:
    core = new_core()
    core.on_order(rec("A"), ts(1))
    core.on_order(rec("B"), ts(2))
    core.on_station_status(status("spin", "down", 5))
    assert core.outstanding["spin"].order_id == "A"
    actions = core.on_op_completed(completed("A", "spin", 1, 6))
    assert core.outstanding["spin"] is None
    assert not any(isinstance(a, Dispatch) and a.step == "spin" for a in actions)
    assert [e.order_id for e in core.backlog["spin"]] == ["B"]


def test_set_offline_blocks_and_needs_both_to_recover() -> None:
    core = new_core()
    core.set_offline("spin", True, ts(1))
    acts = core.on_order(rec("A"), ts(2))
    assert [a for a in acts if not isinstance(a, EmitLot)] == [
        EmitLifecycle("released", "A", ts(2))
    ]
    # reported offline from the station fact
    core.on_station_status(status("spin", "offline", 3))
    core.set_offline("spin", False, ts(4))
    assert core.outstanding["spin"] is None  # reported still offline
    actions = core.on_station_status(status("spin", "idle", 5))
    assert [a.step for a in actions if isinstance(a, Dispatch)] == ["spin"]


def test_reported_offline_alone_blocks() -> None:
    core = new_core()
    core.on_station_status(status("spin", "offline", 1))
    core.on_order(rec("A"), ts(2))
    assert core.outstanding["spin"] is None


def test_status_ts_greatest_wins_and_filters() -> None:
    core = new_core()
    core.on_station_status(status("bake", "down", 10))
    core.on_station_status(status("bake", "idle", 5))
    assert core.health["bake"].reported == "down"
    core.on_station_status(status("bake", "idle", 20, run="other"))
    assert core.health["bake"].reported == "down"
    core.on_station_status(status("bake", "idle", 30, run="unknown"))
    assert core.health["bake"].reported == "idle"
    core.on_station_status(hb("bake", "down", 40))
    assert not core.dispatchable("bake")
    assert core.on_station_status(status("nope", "down", 50)) == []


def test_visit_is_per_entry_not_constant() -> None:
    import pizza.scheduler.core as c

    assert not hasattr(c, "VISIT")


def test_late_visit_completion_ignored() -> None:
    core = new_core()
    core.on_order(rec("A"), ts(1))
    core.on_op_failed(failed("A", "spin", 1, 5))
    cycle_rework(core, "A", "spin", 1, 5)
    assert core.outstanding["spin"].visit == 2
    assert core.on_op_completed(completed("A", "spin", 1, 6)) == []
    assert core.outstanding["spin"].visit == 2
    core.on_op_completed(completed("A", "spin", 2, 7))
    assert core.outstanding["spin"] is None


def test_fail_flags() -> None:
    core = new_core()
    core.on_order(rec("A", fail_step="bake", fail_times=2, poison=True), ts(1))
    assert core.fail_flags("A", "bake") == (2, True)
    assert core.fail_flags("A", "spin") == (None, False)
    assert core.fail_flags("zzz", "bake") == (None, False)


# -- task 2 -----------------------------------------------------------------
def test_rework_releases_credit() -> None:
    core = new_core()
    core.on_order(rec("A"), ts(1))
    core.on_order(rec("B"), ts(2))
    actions = core.on_op_failed(failed("A", "spin", 1, 5))
    assert actions[0] == PublishRework(
        order_id="A",
        step="spin",
        visit=2,
        product="margherita",
        qty=2,
        entered_ts=ts(1),
        command_event_id=job_event_id(RUN, "A", "spin", 2),
        failed_visit=1,
        failed_ts=ts(5),
    )
    assert any(isinstance(a, Dispatch) and a.order_id == "B" for a in actions[1:])
    assert core.outstanding["spin"].order_id == "B"
    assert core.backlog["spin"] == []
    assert "A" in core.cooling
    # a cooling lot is held by the scheduler: manual bank acts immediately
    acts = core.bank("A", "customer", ts(6))
    assert [a.fact for a in acts if isinstance(a, EmitLot)] == ["banked"]
    assert "A" not in core.cooling and "A" in core.banked


def test_exhaust_dead_letters_and_releases_credit() -> None:
    core = new_core()
    core.on_order(rec("A"), ts(1))
    core.on_op_failed(failed("A", "spin", 1, 5))
    cycle_rework(core, "A", "spin", 1, 5)
    core.on_op_failed(failed("A", "spin", 2, 6))
    cycle_rework(core, "A", "spin", 2, 6)
    assert core.outstanding["spin"].visit == 3
    core.on_order(rec("B"), ts(7))
    actions = core.on_op_failed(failed("A", "spin", 3, 8))
    assert isinstance(actions[0], PublishDeadLetter)
    assert actions[0].visit == 3
    assert actions[0].reason == "rework_exhausted"
    assert actions[1] == EmitLifecycle("failed", "A", ts(8), step="spin", visit=3)
    assert any(isinstance(a, Dispatch) and a.order_id == "B" for a in actions[2:])
    assert core.outstanding["spin"].order_id == "B"
    assert "failed" in core.orders["A"].emitted


def test_zero_limit_station_dead_letters_immediately() -> None:
    core = new_core()
    to_bake(core, "A")
    assert core.outstanding["bake"].order_id == "A"
    actions = core.on_op_failed(failed("A", "bake", 1, 99))
    assert isinstance(actions[0], PublishDeadLetter)
    assert core.outstanding["bake"] is None


def test_failed_guards() -> None:
    core = new_core()
    core.on_order(rec("A"), ts(1))
    ev = failed("A", "spin", 1, 5)
    assert len(core.on_op_failed(ev)) == 1
    assert core.on_op_failed(ev) == []
    assert core.on_op_failed(failed("A", "spin", 1, 6)) == []  # stale visit
    cycle_rework(core, "A", "spin", 1, 6)
    assert core.on_op_failed(failed("B", "spin", 2, 6)) == []  # other order
    assert core.on_op_failed(failed("A", "spin", 2, 6, run="other")) == []
    assert core.on_op_failed(failed("A", "nope", 2, 6)) == []
    core.on_op_failed(failed("A", "spin", 2, 7))
    cycle_rework(core, "A", "spin", 2, 7)
    core.on_op_failed(failed("A", "spin", 3, 8))
    assert core.on_op_failed(failed("A", "spin", 3, 9)) == []  # already failed


def test_set_offline_actions() -> None:
    core = new_core()
    assert core.set_offline("bake", True, ts(1)) == [EmitStationStatus("bake", "offline", ts(1))]
    assert core.set_offline("bake", True, ts(2)) == []
    core.on_station_status(hb("bake", "busy", 3))
    out = core.set_offline("bake", False, ts(4))
    assert out[0] == EmitStationStatus("bake", "busy", ts(4))

    core2 = new_core()
    core2.on_station_status(status("bake", "offline", 1))
    core2.set_offline("bake", True, ts(2))
    out2 = core2.set_offline("bake", False, ts(3))
    assert out2[0] == EmitStationStatus("bake", "idle", ts(3))


def test_set_offline_recovery_dispatches() -> None:
    core = new_core()
    core.set_offline("bake", True, ts(1))
    to_bake(core, "A")
    assert core.outstanding["bake"] is None
    out = core.set_offline("bake", False, ts(100))
    assert any(isinstance(a, Dispatch) and a.step == "bake" for a in out)


def test_rework_completion_advances_with_visit_1() -> None:
    core = new_core()
    core.on_order(rec("A"), ts(1))
    core.on_order(rec("B"), ts(2))
    core.on_op_failed(failed("A", "spin", 1, 5))
    assert core.outstanding["spin"].order_id == "B"
    core.on_op_completed(completed("B", "spin", 1, 6))
    assert core.outstanding["spin"] is None  # A is still cooling
    actions = cycle_rework(core, "A", "spin", 1, 6)
    assert any(isinstance(a, Dispatch) and a.order_id == "A" and a.visit == 2 for a in actions)
    core.on_op_completed(completed("A", "spin", 2, 7))
    assert core.orders["A"].next_index == 1
    queued = [e for e in core.backlog["sauce"] if e.order_id == "A"]
    assert queued and queued[0].visit == 1


def test_uuid_type_sanity() -> None:
    assert isinstance(job_event_id(RUN, "A", "spin", 1), uuid.UUID)


# -- Phase 6.1: breakdown-caused op_failed shares the one rework table -------
def test_breakdown_cause_reworks_within_limit() -> None:
    core = new_core()
    core.on_order(rec("A"), ts(1))
    for v in (1, 2):
        actions = core.on_op_failed(failed("A", "spin", v, 5 + v, cause="breakdown"))
        assert [type(a) for a in actions] == [PublishRework]
        assert actions[0].visit == v + 1
        assert actions[0].failed_visit == v
        assert core.outstanding["spin"] is None
        assert "A" in core.cooling
        back = cycle_rework(core, "A", "spin", v, 5 + v)
        assert [type(a) for a in back] == [Dispatch]
        assert core.outstanding["spin"].order_id == "A"
        assert core.outstanding["spin"].visit == v + 1


def test_breakdown_cause_exhausted_dead_letters_and_releases_credit() -> None:
    core = new_core()
    core.on_order(rec("A"), ts(1))
    core.on_op_failed(failed("A", "spin", 1, 5, cause="breakdown"))
    cycle_rework(core, "A", "spin", 1, 5)
    core.on_op_failed(failed("A", "spin", 2, 6, cause="breakdown"))
    cycle_rework(core, "A", "spin", 2, 6)
    core.on_order(rec("B"), ts(6))
    actions = core.on_op_failed(failed("A", "spin", 3, 7, cause="breakdown"))
    assert isinstance(actions[0], PublishDeadLetter)
    assert actions[0].visit == 3
    assert actions[0].reason == "rework_exhausted"
    assert actions[1] == EmitLifecycle("failed", "A", ts(7), step="spin", visit=3)
    assert any(isinstance(a, Dispatch) and a.order_id == "B" for a in actions[2:])
    assert core.outstanding["spin"].order_id == "B"
    assert "failed" in core.orders["A"].emitted


def test_breakdown_cause_zero_limit_dead_letters_immediately() -> None:
    core = new_core()
    to_bake(core, "A")
    assert core.outstanding["bake"].order_id == "A"
    actions = core.on_op_failed(failed("A", "bake", 1, 99, cause="breakdown"))
    assert isinstance(actions[0], PublishDeadLetter)
    assert core.outstanding["bake"] is None


def test_duplicate_breakdown_failed_ignored() -> None:
    core = new_core()
    core.on_order(rec("A"), ts(1))
    ev = failed("A", "spin", 1, 5, cause="breakdown")
    assert len(core.on_op_failed(ev)) == 1
    assert core.on_op_failed(ev) == []
    assert core.on_op_failed(failed("A", "spin", 1, 6, cause="breakdown")) == []
    cycle_rework(core, "A", "spin", 1, 6)
    assert core.outstanding["spin"].visit == 2


def test_mixed_quality_then_breakdown_share_one_table() -> None:
    core = new_core()
    core.on_order(rec("A"), ts(1))
    a1 = core.on_op_failed(failed("A", "spin", 1, 5, cause="quality"))
    cycle_rework(core, "A", "spin", 1, 5)
    a2 = core.on_op_failed(failed("A", "spin", 2, 6, cause="breakdown"))
    cycle_rework(core, "A", "spin", 2, 6)
    assert [type(a) for a in a1] == [PublishRework]
    assert [type(a) for a in a2] == [PublishRework]
    assert a2[0].visit == 3
    # visit 3 exhausts the limit regardless of cause
    a3 = core.on_op_failed(failed("A", "spin", 3, 7, cause="quality"))
    assert isinstance(a3[0], PublishDeadLetter)


# -- Phase 6.2: credit release + cooling ------------------------------------
def test_on_rework_ready_idempotent() -> None:
    core = new_core()
    core.on_order(rec("A"), ts(1))
    core.on_op_failed(failed("A", "spin", 1, 5))
    assert core.on_rework_ready("A", "spin", 2, "other", ts(9)) == []  # wrong run
    assert core.on_rework_ready("A", "spin", 3, RUN, ts(9)) == []  # wrong visit
    assert core.on_rework_ready("A", "sauce", 2, RUN, ts(9)) == []  # wrong station
    assert core.on_rework_ready("Z", "spin", 2, RUN, ts(9)) == []  # unknown order
    first = core.on_rework_ready("A", "spin", 2, RUN, ts(9))
    assert any(isinstance(a, Dispatch) for a in first)
    assert core.on_rework_ready("A", "spin", 2, RUN, ts(9)) == []  # duplicate
    # terminal orders: cooling lot whose order already failed/completed is ignored
    core.on_order(rec("C"), ts(10))
    # C queued behind A (outstanding visit 2): the outstanding guard rejects it
    assert core.on_op_failed(failed("C", "spin", 1, 11)) == []
    assert "C" not in core.cooling
    core2 = new_core()
    core2.on_order(rec("D"), ts(1))
    core2.on_op_failed(failed("D", "spin", 1, 5))
    core2.orders["D"].emitted.add("failed")
    assert core2.on_rework_ready("D", "spin", 2, RUN, ts(9)) == []
    core2.orders["D"].emitted.discard("failed")
    core2.orders["D"].emitted.add("completed")
    assert core2.on_rework_ready("D", "spin", 2, RUN, ts(9)) == []


def test_rework_ready_fifo_original_entered_ts() -> None:
    core = new_core()
    core.on_order(rec("A"), ts(1))
    core.on_order(rec("B"), ts(2))
    core.on_order(rec("C"), ts(3))
    core.on_station_status(status("spin", "down", 4))  # no dispatch while down
    core.on_op_failed(failed("A", "spin", 1, 5))
    assert core.outstanding["spin"] is None
    core.on_rework_ready("A", "spin", 2, RUN, ts(10))
    entry = next(e for e in core.backlog["spin"] if e.order_id == "A")
    assert entry.entered_ts == ts(1)
    assert entry.ready_ts == ts(5)
    assert core.snapshot()["backlog"]["spin"] == ["A", "B", "C"]


def test_rework_dispatch_ready_ts_is_failed_ts() -> None:
    core = new_core()
    core.on_order(rec("A"), ts(1))
    core.on_op_failed(failed("A", "spin", 1, 5))
    out = cycle_rework(core, "A", "spin", 1, 5)
    d = [a for a in out if isinstance(a, Dispatch)]
    assert len(d) == 1
    assert d[0].visit == 2
    assert d[0].ready_ts == ts(5)
    assert core.outstanding["spin"].entered_ts == ts(1)


def test_sweep_returns_cooling_after_grace_and_late_hold_dropped() -> None:
    core = new_core()
    core.on_order(rec("A"), ts(1))
    core.on_op_failed(failed("A", "spin", 1, 5))
    assert core.sweep(ts(5) + timedelta(seconds=14)) == []
    assert "A" in core.cooling
    out = core.sweep(ts(5) + timedelta(seconds=16))
    assert any(isinstance(a, Dispatch) and a.order_id == "A" and a.visit == 2 for a in out)
    assert "A" not in core.cooling
    assert core.on_rework_ready("A", "spin", 2, RUN, ts(20000)) == []


def test_rework_hold_grace_follows_retry_ttl() -> None:
    """WR-01: the default grace is derived from RETRY_TTL_MS, not a literal."""
    from pizza.common.topology import RETRY_TTL_MS
    from pizza.scheduler.core import CoreConfig

    assert CoreConfig().rework_hold_grace_s == 3 * RETRY_TTL_MS / 1000


def test_sweep_grace_counts_from_cooling_start_when_scheduler_lags() -> None:
    """WR-01: a late-handled op_failed still gets the full grace before the sweep."""
    core = new_core()
    core.on_order(rec("A"), ts(1))
    grace = core.config.rework_hold_grace_s
    handled = ts(5) + timedelta(seconds=grace + 10)  # scheduler lagged past the grace
    core.on_op_failed(failed("A", "spin", 1, 5), handled)
    assert core.sweep(handled + timedelta(seconds=1)) == []
    assert "A" in core.cooling
    out = core.sweep(handled + timedelta(seconds=grace + 1))
    assert any(isinstance(a, Dispatch) and a.order_id == "A" for a in out)
    # The ready_ts for KPI WAIT still comes from the station's op_failed ts (D-10).
    assert [a.ready_ts for a in out if isinstance(a, Dispatch)] == [ts(5)]
