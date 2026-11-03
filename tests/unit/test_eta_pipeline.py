"""Broker-free ETA pipeline contract (Phase 7.1, plan 11).

Chain: estimate -> OrderSubmitted / OrderLifecycle facts -> scheduler rebuild
-> dashboard fold -> eta_error. In-process objects only: no broker, no clock.
"""

import random
from datetime import UTC, datetime, timedelta
from itertools import islice

import pytest

from pizza.common.envelope import (
    OrderLifecycle,
    OrderSubmitted,
    RunParams,
    StationStatus,
)
from pizza.common.recipes import load_all_recipes, theoretical_total_s
from pizza.dashboard import state as state_mod
from pizza.dashboard.state import fold, line_snapshot, new_line_state
from pizza.generator.plan import plan_orders
from pizza.kpi.eta import eta_error_s
from pizza.scheduler import estimate as est
from pizza.scheduler.core import CoreConfig, OrderRecord, SchedulerCore

RUN = "r-eta"
T0 = datetime(2026, 1, 1, tzinfo=UTC)
RECIPES = load_all_recipes()
ROUTE = tuple(RECIPES)


def at(seconds: float) -> datetime:
    return T0 + timedelta(seconds=seconds)


def _trunc(t: datetime) -> datetime:
    return t.replace(microsecond=t.microsecond // 1000 * 1000)


def make_core(rule: str = "fifo") -> SchedulerCore:
    core = SchedulerCore(config=CoreConfig(recipes=RECIPES))
    core.start_run(RUN)
    core.set_rule(rule)
    for s in core.route:
        core.on_station_status(
            StationStatus.make(run_id=RUN, machine_id=s, ts=at(0), status="idle")
        )
    return core


def queue(core: SchedulerCore, oid: str, product: str, qty: int, t: float = 1.0) -> None:
    core.on_order(OrderRecord(oid, product, qty, at(3600), at(0)), at(t))


def estimate(core: SchedulerCore, product: str, qty: int, *, k: float = 1.0, now: float = 2.0):
    return est.estimate_new(
        core,
        product=product,
        qty=qty,
        priority=50,
        due_ts=None,
        buffer_k=k,
        now=at(now),
        scale=1.0,
    )


def lifecycle_facts(oid, product, qty, created, due, eta_ts, station_eta, done_at):
    def make(state, offset):
        return OrderLifecycle.make(
            run_id=RUN,
            machine_id="generator",
            ts=created + timedelta(seconds=offset),
            state=state,
            order_id=oid,
            product=product,
            qty=qty,
            due_ts=due,
            created_ts=created,
            eta_ts=eta_ts,
            station_eta=station_eta,
        )

    return [make("created", 0.0), make("released", 0.5), make("completed", done_at)]


def dashboard_lot(facts, oid):
    st = new_line_state(RUN, adopt=True)
    for f in facts:
        st = fold(st, f)
    return next(lt for lt in line_snapshot(st)["lots"] if lt["order_id"] == oid)


def test_chain_estimate_to_facts_to_rebuild_to_dashboard_to_error():
    core = make_core()
    queue(core, "L-0001", "pepperoni", 2)
    queue(core, "L-0002", "veggie", 1)
    created = at(2.0)
    new = estimate(core, "margherita", 2, k=1.5, now=2.0)
    assert new.lead_wall_s > 0 and not new.degraded

    # Compose the frozen fields exactly as the generator does.
    eta_ts = _trunc(created + timedelta(seconds=new.lead_wall_s))
    station_eta = {
        s: _trunc(created + timedelta(seconds=v)) for s, v in new.station_lead_wall_s.items()
    }
    due = _trunc(created + timedelta(seconds=new.lead_wall_s * 1.5))
    assert list(station_eta) == list(ROUTE)
    ordered = list(station_eta.values())
    assert all(a < b for a, b in zip(ordered, ordered[1:], strict=False))
    assert ordered[-1] == eta_ts

    submitted = OrderSubmitted.make(
        run_id=RUN,
        machine_id="generator",
        order_id="L-0003",
        product="margherita",
        qty=2,
        due_ts=due,
        created_ts=created,
        eta_ts=eta_ts,
        station_eta=station_eta,
    )
    assert submitted.eta_ts == eta_ts and submitted.station_eta == station_eta

    done_at = new.lead_wall_s + 7.0
    facts = lifecycle_facts("L-0003", "margherita", 2, created, due, eta_ts, station_eta, done_at)

    # Scheduler side: rebuild recovers the frozen values, whatever the delivery order.
    ref_core, _ = SchedulerCore.rebuild(RUN, facts, config=CoreConfig(recipes=RECIPES))
    rec = ref_core.orders["L-0003"].record
    assert rec.eta_ts == eta_ts and dict(rec.station_eta) == station_eta
    rng = random.Random(7)
    for _ in range(5):
        mixed = [*facts, *facts[:2]]
        rng.shuffle(mixed)
        core2, _ = SchedulerCore.rebuild(RUN, mixed, config=CoreConfig(recipes=RECIPES))
        rec2 = core2.orders["L-0003"].record
        assert rec2.eta_ts == eta_ts and dict(rec2.station_eta) == station_eta

    # Dashboard side: same frozen values, same under shuffle/duplicates.
    lot = dashboard_lot(facts, "L-0003")
    assert lot["eta_ts"] == state_mod._iso(eta_ts)
    assert lot["station_eta"] == {k: state_mod._iso(v) for k, v in station_eta.items()}
    for seed in range(5):
        mixed = [*facts, *facts[1:]]
        random.Random(seed).shuffle(mixed)
        assert dashboard_lot(mixed, "L-0003") == lot

    # Derived error equals the pure KPI function over the same timestamps.
    completed_ts = created + timedelta(seconds=done_at)
    assert lot["eta_error_s"] == eta_error_s(completed_ts, eta_ts)
    assert lot["eta_error_s"] == pytest.approx(7.0, abs=1e-3)


def test_empty_line_lead_equals_theoretical_total_and_due_formula():
    core = make_core()
    for product, qty in (("margherita", 1), ("pepperoni", 3), ("veggie", 2)):
        new = estimate(core, product, qty, k=1.0)
        total = theoretical_total_s(RECIPES, product, qty)
        assert new.lead_wall_s == pytest.approx(total)
        created = at(2.0)
        due = created + timedelta(seconds=new.lead_wall_s * 1.0)
        assert (due - created).total_seconds() == pytest.approx(total)


# Golden snapshot of the seeded theory plan: (product, qty, due offset s, arrival offset s).
GOLDEN_THEORY_PLAN = [
    ("margherita", 1, 101.169, 35.169),
    ("veggie", 2, 197.731, 49.731),
    ("margherita", 3, 259.934, 61.935),
    ("veggie", 2, 279.081, 131.082),
    ("veggie", 2, 366.276, 218.276),
    ("pepperoni", 3, 467.64, 245.64),
    ("veggie", 1, 337.737, 263.737),
    ("margherita", 3, 462.48, 264.481),
    ("veggie", 2, 452.26, 304.26),
    ("margherita", 2, 495.392, 363.392),
]


def test_theory_plan_is_untouched_by_eta_phase():
    params = RunParams(seed=1234, due_mode="theory")
    got = [
        (o.product, o.qty, (o.due_ts - T0).total_seconds(), o.offset_s)
        for o in islice(plan_orders(params, T0, RECIPES), 10)
    ]
    assert len(got) == len(GOLDEN_THEORY_PLAN)
    for (p, q, d, a), (gp, gq, gd, ga) in zip(got, GOLDEN_THEORY_PLAN, strict=True):
        assert (p, q) == (gp, gq)
        assert d == pytest.approx(gd, abs=2e-3)
        assert a == pytest.approx(ga, abs=2e-3)
    # due_mode does not alter the seeded plan itself (the estimate is applied by the service).
    other = [
        (o.product, o.qty, o.due_ts, o.offset_s)
        for o in islice(plan_orders(RunParams(seed=1234, due_mode="eta"), T0, RECIPES), 10)
    ]
    same = [
        (o.product, o.qty, o.due_ts, o.offset_s)
        for o in islice(plan_orders(params, T0, RECIPES), 10)
    ]
    assert other == same


def test_rule_changes_estimate_when_processing_times_differ():
    """D-09: the same queue yields different ETAs under fifo and spt."""
    leads = {}
    for rule in ("fifo", "spt"):
        core = make_core(rule)
        queue(core, "L-0001", "pepperoni", 3, t=1.0)  # long job ahead in arrival order
        # New short job: spt lets it overtake the long queued job, fifo does not.
        leads[rule] = estimate(core, "margherita", 1, now=2.0).lead_wall_s
    assert leads["fifo"] != leads["spt"]
    assert leads["spt"] < leads["fifo"]
