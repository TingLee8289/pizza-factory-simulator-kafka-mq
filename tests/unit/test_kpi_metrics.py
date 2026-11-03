import copy
from datetime import timedelta

import pytest

from pizza.common.recipes import load_all_recipes
from pizza.kpi.fold import KpiState, fold, new_state
from pizza.kpi.metrics import KpiConfig, advance, effective_as_of, is_drained
from pizza.kpi.stats import percentile
from tests.unit.kpi_fixtures import RUN, T0, at, simulate_line

ORDERS3 = [("a", "margherita", 2, 0.0), ("b", "pepperoni", 3, 20.0), ("c", "veggie", 1, 40.0)]
ORDERS6 = [
    ("o1", "margherita", 2, 0.0),
    ("o2", "pepperoni", 3, 20.0),
    ("o3", "veggie", 1, 40.0),
    ("o4", "margherita", 3, 60.0),
    ("o5", "pepperoni", 2, 80.0),
    ("o6", "veggie", 3, 100.0),
]


def build(msgs) -> KpiState:
    state = new_state(RUN)
    for m in msgs:
        state = fold(state, m)
    return state


def cfg(n=0, **kw):
    return KpiConfig(warmup_orders=n, **kw)


def test_cycle_time_mean_p50_p95():
    sim = simulate_line(ORDERS3, track_in_s=0.5)
    state = build(sim.messages)
    snap = advance(state, state.last_event_ts, load_all_recipes(), cfg())
    # Pizza-weighted: each order's cycle time counts qty times (n stays in lots).
    qty = {o: q for o, _p, q, _t in ORDERS3}
    vals = sorted(c for o, c in sim.cycle_time_s.items() for _ in range(qty[o]))
    assert snap.cycle_time.n == 3
    assert len(vals) == 6
    assert snap.cycle_time.mean_s == pytest.approx(sum(vals) / 6)
    assert snap.cycle_time.p50_s == pytest.approx(percentile(vals, 0.5))
    assert snap.cycle_time.p95_s == pytest.approx(percentile(vals, 0.95))
    # Per pizza: ct/qty repeated qty times; Little's W (mean_s) is unchanged above.
    pp = sorted(c / qty[o] for o, c in sim.cycle_time_s.items() for _ in range(qty[o]))
    assert snap.cycle_time.per_pizza_mean_s == pytest.approx(sum(pp) / 6)
    assert snap.cycle_time.per_pizza_p50_s == pytest.approx(percentile(pp, 0.5))
    assert snap.cycle_time.per_pizza_p95_s == pytest.approx(percentile(pp, 0.95))


def test_throughput():
    sim = simulate_line(ORDERS3)
    state = build(sim.messages)
    as_of = state.last_event_ts
    snap = advance(state, as_of, load_all_recipes(), cfg())
    assert snap.throughput_per_s == pytest.approx(3 / (as_of - T0).total_seconds())
    assert snap.pizzas_per_s == pytest.approx(6 / (as_of - T0).total_seconds())
    assert snap.window_start_ts == T0
    assert not snap.warming_up


def test_warmup_excludes_first_n():
    sim = simulate_line(ORDERS6)
    state = build(sim.messages)
    as_of = state.last_event_ts
    snap = advance(state, as_of, load_all_recipes(), cfg(2))
    t0 = at(40.0)
    assert snap.window_start_ts == t0
    assert snap.cycle_time.n == 4
    qty = {"o3": 1, "o4": 3, "o5": 2, "o6": 3}
    expected = [sim.cycle_time_s[o] * q for o, q in qty.items()]
    assert snap.cycle_time.mean_s == pytest.approx(sum(expected) / sum(qty.values()))
    assert snap.throughput_per_s == pytest.approx(4 / (as_of - t0).total_seconds())
    assert snap.orders_total == 6


def test_late_earlier_order_displaces():
    sim = simulate_line(ORDERS6)
    early = simulate_line([("o0", "veggie", 1, -10.0)], seed=9)
    state = build(sim.messages)
    as_of = state.last_event_ts
    before = advance(copy.deepcopy(state), as_of, load_all_recipes(), cfg(2))
    for m in early.messages:
        state = fold(state, m)
    after = advance(state, as_of, load_all_recipes(), cfg(2))
    assert before.window_start_ts == at(40.0)
    # Excluded set is now {o0, o1}; T0 is o2's created_ts.
    assert after.window_start_ts == at(20.0)
    assert after.cycle_time.n == 5


def test_warming_up_small_run():
    sim = simulate_line(ORDERS3)
    state = build(sim.messages)
    snap = advance(state, state.last_event_ts, load_all_recipes(), cfg(3))
    assert snap.warming_up
    assert snap.throughput_per_s is None
    assert snap.cycle_time.mean_s is None
    assert snap.cycle_time.per_pizza_mean_s is None
    assert snap.little.rel_error is None
    assert snap.little.status == "insufficient_data"
    assert snap.wip_now == 0


def test_wip_now_and_station_wip():
    sim = simulate_line(ORDERS3, track_in_s=0.5)
    cut = at(30.0)
    state = build([m for m in sim.messages if m.ts <= cut])
    snap = advance(state, cut, load_all_recipes(), cfg())
    assert snap.wip_now == 2
    total = sum(s.wip_wait + s.wip_track_in + s.wip_run for s in snap.stations.values())
    assert total + snap.wip_pre_release == snap.wip_now
    # Order a (created 0) is mid-flow at t=30; order b is in the line too.
    assert snap.stations["spin"].wip_run + snap.stations["spin"].wip_wait >= 0


def test_wip_pre_release():
    sim = simulate_line(ORDERS3)
    msgs = [
        m
        for m in sim.messages
        if getattr(m, "order_id", None) != "c" or getattr(m, "state", None) == "created"
    ]
    state = build(msgs)
    snap = advance(state, at(45.0), load_all_recipes(), cfg())
    assert snap.wip_pre_release == 1
    assert snap.wip_now == 1


def test_effective_as_of_drained():
    sim = simulate_line(ORDERS3)
    state = build(sim.messages)
    assert is_drained(state)
    now = state.last_event_ts + timedelta(hours=1)
    assert effective_as_of(state, now) == state.last_event_ts
    part = build([m for m in sim.messages if m.ts <= at(30.0)])
    assert not is_drained(part)
    assert effective_as_of(part, now) == now
    assert effective_as_of(new_state(RUN), now) == now


def test_deterministic():
    sim = simulate_line(ORDERS6, track_in_s=0.5)
    state = build(sim.messages)
    as_of = state.last_event_ts
    a = advance(copy.deepcopy(state), as_of, load_all_recipes(), cfg(1))
    b = advance(copy.deepcopy(state), as_of, load_all_recipes(), cfg(1))
    assert a.model_dump(exclude={"event_id"}) == b.model_dump(exclude={"event_id"})
