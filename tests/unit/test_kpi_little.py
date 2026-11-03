import pytest

from pizza.common.recipes import load_all_recipes
from pizza.kpi.fold import fold, new_state
from pizza.kpi.metrics import KpiConfig, advance
from tests.unit.kpi_fixtures import RUN, at, simulate_line

ORDERS = [
    ("o1", "margherita", 2, 0.0),
    ("o2", "pepperoni", 3, 20.0),
    ("o3", "veggie", 1, 40.0),
    ("o4", "margherita", 3, 60.0),
]


def build(msgs):
    state = new_state(RUN)
    for m in msgs:
        state = fold(state, m)
    return state


def cfg(n=0, **kw):
    return KpiConfig(warmup_orders=n, **kw)


def test_drained_passes():
    sim = simulate_line(ORDERS, track_in_s=0.5)
    state = build(sim.messages)
    snap = advance(state, state.last_event_ts, load_all_recipes(), cfg())
    assert snap.little.status == "pass"
    assert snap.little.rel_error < 1e-9
    assert snap.little.l == pytest.approx(snap.little.lambda_w)
    assert snap.wip_avg_pizzas == pytest.approx(snap.little.l)
    # L is in pizzas: lambda = pizzas/s, W = pizza-weighted mean cycle time
    assert snap.little.lambda_per_s == pytest.approx(snap.pizzas_per_s)
    assert snap.little.w_s == pytest.approx(snap.cycle_time.mean_s)
    assert snap.little.l > snap.wip_avg  # qty > 1 lots inflate the pizza average


def test_inflight_fraction_formula():
    sim = simulate_line(ORDERS)
    cut = at(100.0)
    msgs = [m for m in sim.messages if m.ts <= cut]
    state = build(msgs)
    snap = advance(state, cut, load_all_recipes(), cfg())
    done = {o: r for o, r in state.orders.items() if r.completed_ts is not None}
    inflight = [r for r in state.orders.values() if r.completed_ts is None]
    a_done = sum((r.completed_ts - r.created_ts).total_seconds() * r.qty for r in done.values())
    a_in = sum((cut - r.created_ts).total_seconds() * r.qty for r in inflight)
    assert snap.little.rel_error == pytest.approx(a_in / (a_done + a_in))
    assert snap.little.n_completed == len(done)
    assert snap.little.n_in_system == len(inflight)
    expect = "pass" if a_in / (a_done + a_in) <= 0.10 else "fail"
    assert snap.little.status == expect
    # tolerance flips the status both ways
    loose = advance(state, cut, load_all_recipes(), cfg(little_tolerance=0.99))
    tight = advance(state, cut, load_all_recipes(), cfg(little_tolerance=0.0))
    assert loose.little.status == "pass"
    assert tight.little.status == "fail"
    assert tight.little.tolerance == 0.0


def test_dropped_completed_fails():
    sim = simulate_line(ORDERS)
    msgs = [
        m
        for m in sim.messages
        if not (m.type == "order.lifecycle" and m.state == "completed" and m.order_id == "o1")
    ]
    state = build(msgs)
    snap = advance(state, state.last_event_ts, load_all_recipes(), cfg())
    assert snap.little.status == "fail"
    assert snap.little.rel_error > 0.10


def test_insufficient_data():
    snap = advance(new_state(RUN), at(10.0), load_all_recipes(), cfg())
    assert snap.little.status == "insufficient_data"
    assert snap.little.l is None
    assert snap.little.tolerance == pytest.approx(0.10)
    sim = simulate_line(ORDERS)
    state = build([m for m in sim.messages if m.ts <= at(1.0)])
    snap = advance(state, at(1.0), load_all_recipes(), cfg())
    assert snap.little.status == "insufficient_data"
    assert snap.little.n_completed == 0


def test_warmup_excluded_from_little():
    sim = simulate_line(ORDERS)
    state = build(sim.messages)
    as_of = state.last_event_ts
    a = advance(state, as_of, load_all_recipes(), cfg(0))
    b = advance(state, as_of, load_all_recipes(), cfg(2))
    assert a.little.n_completed == 4
    assert b.little.n_completed == 2
    assert a.little.l != b.little.l
    # pizza-weighted: o3 has qty 1, o4 has qty 3
    assert b.little.w_s == pytest.approx(
        (sim.cycle_time_s["o3"] * 1 + sim.cycle_time_s["o4"] * 3) / 4
    )
    assert b.little.status == "pass"
