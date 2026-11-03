"""Core state inputs (started_ts, repair_until, eta fields) and the ETA estimate adapter."""

import math
import time
from datetime import UTC, datetime, timedelta

import pytest

from pizza.common.envelope import (
    OpCompleted,
    OpFailed,
    OpStarted,
    StationHeartbeat,
    StationStatus,
)
from pizza.common.ids import job_event_id
from pizza.scheduler import estimate as est
from pizza.scheduler import eta
from pizza.scheduler.core import (
    CoreConfig,
    Dispatch,
    OrderRecord,
    SchedulerCore,
    ordering_key,
)

RUN = "r1"
T0 = datetime(2026, 1, 1, tzinfo=UTC)
UNIT = {
    "spin": 2.0,
    "sauce": 1.0,
    "cheese": 1.0,
    "toppings": 3.0,
    "bake": 4.0,
    "slice": 1.0,
    "package": 1.0,
}
ROUTE = tuple(UNIT)
RECIPES = {s: {"margherita": u, "pepperoni": 2 * u} for s, u in UNIT.items()}
CFG = CoreConfig(recipes=RECIPES)
TOTAL = sum(UNIT.values())  # 13


def at(seconds: float) -> datetime:
    return T0 + timedelta(seconds=seconds)


def make_core(config: CoreConfig | None = None) -> SchedulerCore:
    core = SchedulerCore(config=config)
    core.set_rule("fifo")  # these estimates are FIFO-based; the core default is EDD
    core.start_run(RUN)
    for s in core.route:
        core.on_station_status(
            StationStatus.make(run_id=RUN, machine_id=s, ts=at(0), status="idle")
        )
    return core


def order(
    oid: str, product: str = "margherita", qty: int = 1, due: float = 3600, **kw
) -> OrderRecord:
    return OrderRecord(oid, product, qty, at(due), at(0), **kw)


def status(station: str, t: float, st: str, repair: float | None = None) -> StationStatus:
    extra = {"repair_in_s": repair} if repair is not None else {}
    return StationStatus.make(run_id=RUN, machine_id=station, ts=at(t), status=st, **extra)


def started(d: Dispatch, t: float, run: str = RUN, visit: int | None = None) -> OpStarted:
    return OpStarted.make(
        run_id=run,
        machine_id=d.step,
        ts=at(t),
        order_id=d.order_id,
        step=d.step,
        visit=visit or d.visit,
        attempt=1,
        qty=d.qty,
        command_event_id=d.command_event_id,
        redelivered=False,
    )


def completed(d: Dispatch, t: float) -> OpCompleted:
    return OpCompleted.make(
        run_id=RUN,
        machine_id=d.step,
        ts=at(t),
        order_id=d.order_id,
        step=d.step,
        visit=d.visit,
        attempt=1,
        qty=d.qty,
        command_event_id=d.command_event_id,
        duration_s=0.0,
    )


def failed(d: Dispatch, t: float) -> OpFailed:
    return OpFailed.make(
        run_id=RUN,
        machine_id=d.step,
        ts=at(t),
        order_id=d.order_id,
        step=d.step,
        visit=d.visit,
        attempt=1,
        qty=d.qty,
        command_event_id=job_event_id(RUN, d.order_id, d.step, d.visit),
        reason="boom",
    )


def dispatch_first(core: SchedulerCore, oid: str, t: float = 1) -> Dispatch:
    actions = core.on_order(order(oid), at(t))
    return next(a for a in actions if isinstance(a, Dispatch))


# -- Task 1: core state ----------------------------------------------------


def test_outstanding_started_set_and_cleared_on_complete():
    core = make_core()
    d = dispatch_first(core, "o1")
    assert core.outstanding_started["spin"] is None
    core.on_op_started(started(d, 5))
    assert core.outstanding_started["spin"] == at(5)
    core.on_op_started(started(d, 9))  # duplicate (same command) does not change it
    assert core.outstanding_started["spin"] == at(5)
    core.on_op_completed(completed(d, 10))
    assert core.outstanding_started["spin"] is None


def test_outstanding_started_ignores_stale_run_and_other_order():
    core = make_core()
    d = dispatch_first(core, "o1")
    core.on_op_started(started(d, 5, run="other"))
    assert core.outstanding_started["spin"] is None
    core.on_order(order("o2"), at(2))
    other = Dispatch("o2", "spin", 1, "margherita", 1, at(2), job_event_id(RUN, "o2", "spin", 1))
    core.on_op_started(started(other, 6))
    assert core.outstanding_started["spin"] is None


def test_outstanding_started_cleared_on_dead_letter_and_rework():
    core = make_core()
    d = dispatch_first(core, "o1")
    core.on_op_started(started(d, 5))
    core.on_op_failed(failed(d, 6), at(6))  # spin allows rework
    assert core.outstanding["spin"] is None
    assert core.outstanding_started["spin"] is None

    core2 = make_core()
    core2.on_order(order("o1"), at(1))
    d1 = Dispatch("o1", "spin", 1, "margherita", 1, at(1), job_event_id(RUN, "o1", "spin", 1))
    core2.on_op_started(started(d1, 2))
    core2.on_op_completed(completed(d1, 3))
    d2 = Dispatch("o1", "sauce", 1, "margherita", 1, at(3), job_event_id(RUN, "o1", "sauce", 1))
    core2.on_op_started(started(d2, 4))
    assert core2.outstanding_started["sauce"] == at(4)
    core2.on_op_failed(failed(d2, 5), at(5))  # sauce: dead-letter
    assert core2.outstanding_started["sauce"] is None


def test_outstanding_started_cleared_on_release_dispatch_rollback_reset():
    core = make_core()
    d = dispatch_first(core, "o1")
    core.on_op_started(started(d, 5))
    core.release_dispatch("spin")
    assert core.outstanding_started["spin"] is None
    d = next(iter(core.dispatch_ready()))
    core.on_op_started(started(d, 6))
    core.rollback_order("o1")
    assert core.outstanding_started["spin"] is None
    d = dispatch_first(core, "o2")
    core.on_op_started(started(d, 7))
    core.start_run("r2")
    assert all(v is None for v in core.outstanding_started.values())


def test_repair_until_set_and_cleared():
    core = make_core()
    core.on_station_status(status("bake", 10, "down", repair=30))
    assert core.health["bake"].repair_until == at(40)
    core.on_station_status(
        StationHeartbeat.make(run_id=RUN, machine_id="bake", ts=at(12), status="down")
    )
    assert core.health["bake"].repair_until == at(40)
    core.on_station_status(status("bake", 5, "idle"))  # stale: ignored
    assert core.health["bake"].repair_until == at(40)
    core.on_station_status(status("bake", 20, "idle"))
    assert core.health["bake"].repair_until is None


def test_repair_until_none_without_repair_in_s():
    core = make_core()
    core.on_station_status(status("bake", 10, "down"))
    assert core.health["bake"].repair_until is None


def test_order_record_eta_fields_default_none():
    r = OrderRecord("o", "margherita", 1, at(1), at(0))
    assert r.eta_ts is None and r.station_eta is None
    r2 = OrderRecord("o", "margherita", 1, at(1), at(0), eta_ts=at(9), station_eta={"spin": at(2)})
    assert r2.eta_ts == at(9)


# -- Task 2: estimate_new --------------------------------------------------


def new(core, *, product="margherita", qty=1, priority=50, due=None, k=1.5, now=2.0, scale=1.0):
    return est.estimate_new(
        core,
        product=product,
        qty=qty,
        priority=priority,
        due_ts=at(due) if due is not None else None,
        buffer_k=k,
        now=at(now),
        scale=scale,
    )


def cum() -> list[float]:
    out, acc = [], 0.0
    for u in UNIT.values():
        acc += u
        out.append(acc)
    return out


def test_new_empty_line_scale_1():
    core = make_core(CFG)
    r = new(core)
    assert r.lead_wall_s == TOTAL
    assert list(r.station_lead_wall_s.values()) == cum()
    assert r.degraded is False


def test_new_scale_divides():
    core = make_core(CFG)
    r = new(core, scale=8.0)
    assert r.lead_wall_s == TOTAL / 8
    assert list(r.station_lead_wall_s.values()) == [c / 8 for c in cum()]


def test_new_fifo_queue_ahead():
    core = make_core(CFG)
    core.on_order(order("o1"), at(1))  # running at spin
    core.on_order(order("o2"), at(1.5))  # queued at spin
    r = new(core)
    free = [4.0] + [2 * u for u in list(UNIT.values())[1:]]
    expect = eta.finish_offsets(free, list(UNIT.values()))
    assert list(r.station_lead_wall_s.values()) == expect
    assert r.station_lead_wall_s["spin"] == 6.0


@pytest.mark.parametrize("rule", ["fifo", "edd", "spt"])
@pytest.mark.parametrize("with_rush", [False, True])
def test_new_ahead_matches_dispatch_order(rule, with_rush):
    core = make_core(CFG)
    core.set_rule(rule)
    core.on_order(order("r0"), at(1))  # running at spin
    specs = [
        ("a", "margherita", 5, 5.0, 50),
        ("b", "pepperoni", 1, 3600.0, 50),
        ("c", "margherita", 2, 10.0, 10 if with_rush else 50),
        ("d", "margherita", 1, 3600.0, 50),
        ("e", "pepperoni", 3, 7.0, 50),
    ]
    for i, (oid, prod, qty, due, prio) in enumerate(specs):
        core.on_order(order(oid, prod, qty, due, priority=prio), at(1.1 + i / 10))
    view = est._view(core, at(2), 1.0)
    provisional = at(2 + TOTAL * 1.5)
    entry = est.BacklogEntry(est._NEW_ID, 0, at(2))
    ctx = est._new_lot_context(core, "spin", "margherita", 1, provisional, 50)
    key = ordering_key(rule, entry, ctx)
    ahead = {lot.order_id for lot in est._ahead(view, 0, key, None)}
    expected = {
        e.order_id
        for e in core.backlog["spin"]
        if ordering_key(rule, e, core._ctx("spin", e)) < key
    }
    assert ahead == expected
    order_ids = [e.order_id for e in sorted(core.backlog["spin"], key=core._rule_key("spin"))]
    assert set(order_ids[: len(ahead)]) == ahead
    if with_rush:
        assert order_ids[0] == "c"  # priority 10 sorts ahead of older non-rush lots
        assert "c" in ahead


def test_new_edd_provisional_due_and_explicit():
    core = make_core(CFG)
    core.set_rule("edd")
    core.on_order(order("r0"), at(1))
    core.on_order(order("x", due=5), at(1.1))
    core.on_order(order("y", due=3600), at(1.2))
    a, b = new(core), new(core)
    assert a == b
    # provisional due = 2 + 13*1.5 = 21.5: x (due 5) is ahead, y (3600) is not
    assert a.station_lead_wall_s["spin"] == 2.0 + 2.0 + 2.0
    explicit = new(core, due=0.5)
    assert explicit.station_lead_wall_s["spin"] == 2.0 + 2.0


def test_new_running_remaining():
    core = make_core(CFG)
    d = dispatch_first(core, "o1")  # margherita qty 1 -> spin proc 2
    core.on_order(order("o1b", qty=1), at(1))
    core2 = make_core(CFG)
    core2.on_order(order("o1", qty=3), at(1))  # proc 6 at spin
    d2 = Dispatch("o1", "spin", 1, "margherita", 3, at(1), job_event_id(RUN, "o1", "spin", 1))
    r_full = new(core2, now=5)
    assert r_full.station_lead_wall_s["spin"] == 6.0 + 2.0  # started unknown: full proc
    core2.on_op_started(started(d2, 1))
    r = new(core2, now=5)
    assert r.station_lead_wall_s["spin"] == 2.0 + 2.0  # 6 - 4 elapsed
    r_floor = new(core2, now=50)
    assert r_floor.station_lead_wall_s["spin"] == 2.0
    assert d.order_id == "o1"


def test_new_running_remaining_uses_scale():
    core = make_core(CFG)
    core.on_order(order("o1", qty=3), at(1))
    d = Dispatch("o1", "spin", 1, "margherita", 3, at(1), job_event_id(RUN, "o1", "spin", 1))
    core.on_op_started(started(d, 1))
    r = new(core, now=3, scale=2.0)  # 2 wall s * 2 = 4 sim elapsed -> 2 remaining + 2 own
    assert r.station_lead_wall_s["spin"] == (2.0 + 2.0) / 2


def test_new_repair_and_degraded():
    core = make_core(CFG)
    core.on_station_status(status("bake", 1, "down", repair=21))  # until 22
    r = new(core, now=2)
    assert r.lead_wall_s == 20 + 4 + 1 + 1  # repair 20 remaining + bake + slice + package
    assert r.degraded is False
    r2 = new(core, now=2, scale=2.0)
    assert r2.lead_wall_s == (40 + 6) / 2
    core.on_station_status(status("bake", 3, "idle"))
    assert new(core).lead_wall_s == TOTAL

    core.on_station_status(status("bake", 4, "down"))  # manual: unknown repair
    r3 = new(core)
    assert r3.lead_wall_s == TOTAL and r3.degraded is True

    core.on_station_status(status("bake", 5, "idle"))
    core.set_offline("slice", True, at(6))
    assert new(core).degraded is True
    fresh = SchedulerCore(config=CFG)
    fresh.start_run(RUN)
    assert new(fresh).degraded is True


def test_new_exclusions_and_cooling():
    core = make_core(CFG)
    core.on_order(order("r0"), at(1))
    for oid in ("banked", "failed", "done"):
        core.on_order(order(oid), at(1.1))
    core.bank("banked", "manual", at(1.2))
    core.orders["failed"].emitted.add("failed")
    core.orders["done"].emitted.add("completed")
    assert new(core).station_lead_wall_s["spin"] == 2.0 + 2.0  # only r0 running

    c = make_core(CFG)
    d = dispatch_first(c, "o1")
    c.on_op_started(started(d, 1))
    c.on_op_failed(failed(d, 2), at(2))  # spin allows rework -> cooling
    assert "o1" in c.cooling
    assert new(c).station_lead_wall_s["spin"] == 2.0 + 2.0  # cooling lot at full proc


def test_new_upstream_lot_counts_only_if_ahead():
    spt = make_core(CFG)
    spt.set_rule("spt")
    spt.on_order(order("r0"), at(1))
    spt.on_order(order("big", qty=10), at(1.1))
    fifo = make_core(CFG)
    fifo.on_order(order("r0"), at(1))
    fifo.on_order(order("big", qty=10), at(1.1))
    assert new(spt).station_lead_wall_s["spin"] == 2.0 + 2.0
    assert new(fifo).station_lead_wall_s["spin"] == 2.0 + 20.0 + 2.0


def test_new_invalid_product_and_recipe():
    core = make_core(CFG)
    with pytest.raises(ValueError, match="unicorn"):
        new(core, product="unicorn")
    bad = {s: {"margherita": math.inf} for s in UNIT}
    core2 = make_core(CoreConfig(recipes=bad))
    with pytest.raises(ValueError, match="margherita"):
        new(core2)
    with pytest.raises(ValueError):
        new(core, scale=0)


# -- Task 3: estimate_wip --------------------------------------------------


def hold_spin(core):
    core.on_station_status(status("spin", 0.5, "down"))


def test_wip_queued_lot_and_scale():
    core = make_core(CFG)
    hold_spin(core)
    core.on_order(order("o1"), at(1))
    assert est.estimate_wip(core, now=at(2), scale=1.0) == {"o1": at(2 + TOTAL)}
    assert est.estimate_wip(core, now=at(2), scale=4.0) == {"o1": at(2 + TOTAL / 4)}


def test_wip_running_lot_elapsed():
    core = make_core(CFG)
    core.on_order(order("o1", qty=3), at(1))
    d = Dispatch("o1", "spin", 1, "margherita", 3, at(1), job_event_id(RUN, "o1", "spin", 1))
    core.on_op_started(started(d, 1))
    got = est.estimate_wip(core, now=at(4), scale=1.0)
    assert got == {"o1": at(4 + 3 + (TOTAL - 2) * 3)}


def test_wip_excludes_self_and_counts_ahead():
    core = make_core(CFG)
    hold_spin(core)
    core.on_order(order("o1"), at(1))
    alone = est.estimate_wip(core, now=at(2), scale=1.0)["o1"]
    core.on_order(order("o2"), at(1.5))
    both = est.estimate_wip(core, now=at(2), scale=1.0)
    assert both["o1"] == alone
    assert both["o2"] == at(2 + 15)


def test_wip_excludes_terminal_includes_cooling_and_limit():
    core = make_core(CFG)
    core.on_order(order("r0"), at(1))
    d = Dispatch("r0", "spin", 1, "margherita", 1, at(1), job_event_id(RUN, "r0", "spin", 1))
    core.on_op_started(started(d, 1))
    core.on_op_failed(failed(d, 2), at(2))
    for oid in ("hold", "banked", "failed", "done", "live"):
        core.on_order(order(oid), at(3))
    core.bank("banked", "manual", at(3.1))
    core.orders["failed"].emitted.add("failed")
    core.orders["done"].emitted.add("completed")
    got = est.estimate_wip(core, now=at(4.0004), scale=1.0)
    assert set(got) == {"r0", "hold", "live"}
    assert all(v.tzinfo is not None and v.microsecond % 1000 == 0 for v in got.values())
    assert list(est.estimate_wip(core, now=at(4), scale=1.0, limit=1)) == ["r0"]


def test_wip_perf_200_lots():
    core = make_core(CFG)
    hold_spin(core)
    for i in range(200):
        core.on_order(order(f"o{i:03d}"), at(1 + i / 1000))
    t = time.perf_counter()
    got = est.estimate_wip(core, now=at(5), scale=1.0)
    assert len(got) == 200
    assert time.perf_counter() - t < 0.5


# -- estimate_lot ----------------------------------------------------------


def test_lot_queue_wait_pushes_in_after_previous_out():
    core = make_core(CFG)
    hold_spin(core)
    core.on_order(order("o1"), at(1))
    core.on_order(order("o2"), at(1.5))
    got = est.estimate_lot(core, "o2", now=at(2), scale=1.0)
    assert got is not None
    spin_in, spin_out = got["spin"]
    assert spin_in == at(2 + 2.0)  # waits behind o1 at spin
    assert spin_out == at(2 + 4.0)
    sauce_in, _ = got["sauce"]
    assert sauce_in is not None and sauce_in >= spin_out
    assert got["package"][1] == est.estimate_wip(core, now=at(2), scale=1.0)["o2"]


def test_lot_running_current_has_only_out():
    core = make_core(CFG)
    core.on_order(order("o1", qty=3), at(1))
    d = Dispatch("o1", "spin", 1, "margherita", 3, at(1), job_event_id(RUN, "o1", "spin", 1))
    core.on_op_started(started(d, 1))
    got = est.estimate_lot(core, "o1", now=at(4), scale=1.0)
    assert got is not None
    assert got["spin"] == (None, at(4 + 3))
    assert got["sauce"][0] is not None
    assert got["package"][1] == est.estimate_wip(core, now=at(4), scale=1.0)["o1"]


def test_lot_unknown_banked_terminal_none():
    core = make_core(CFG)
    hold_spin(core)
    assert est.estimate_lot(core, "ghost", now=at(2), scale=1.0) is None
    core.on_order(order("b"), at(1))
    core.on_order(order("t"), at(1))
    core.bank("b", "manual", at(1.1))
    core.orders["t"].emitted.add("completed")
    assert est.estimate_lot(core, "b", now=at(2), scale=1.0) is None
    assert est.estimate_lot(core, "t", now=at(2), scale=1.0) is None
    with pytest.raises(ValueError):
        est.estimate_lot(core, "b", now=at(2), scale=0.0)


def test_new_lower_priority_number_leads_no_longer():
    core = make_core(CFG)
    core.on_order(order("r0"), at(1))
    for i, oid in enumerate("abc"):
        core.on_order(order(oid, qty=3), at(1.1 + i / 10))
    urgent = new(core, priority=10).lead_wall_s
    normal = new(core, priority=50).lead_wall_s
    assert urgent < normal
