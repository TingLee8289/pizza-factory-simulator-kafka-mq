from pizza.common.envelope import RunStarted
from pizza.common.recipes import load_all_recipes
from pizza.kpi.fold import fold, new_state
from pizza.kpi.metrics import KpiConfig, advance
from tests.unit.kpi_fixtures import RUN, T0, at, simulate_line

ORDERS = [("o1", "margherita", 2, 0.0), ("o2", "pepperoni", 3, 20.0), ("o3", "veggie", 1, 40.0)]


def build(msgs, run_id=RUN):
    state = new_state(run_id)
    for m in msgs:
        state = fold(state, m)
    return state


def test_run_started_resets():
    state = build(simulate_line(ORDERS).messages)
    assert state.orders
    state = fold(state, RunStarted.make(run_id="r2", machine_id="scheduler", ts=at(500.0)))
    assert state.run_id == "r2"
    assert not state.orders and not state.ops and not state.status_logs
    snap = advance(state, at(501.0), load_all_recipes(), KpiConfig(warmup_orders=2))
    assert snap.warming_up
    assert snap.run_id == "r2"
    assert snap.orders_total == 0


def test_foreign_run_ignored_after_reset():
    state = build(simulate_line(ORDERS).messages)
    state = fold(state, RunStarted.make(run_id="r2", machine_id="scheduler", ts=at(500.0)))
    for m in simulate_line(ORDERS, run_id="r1").messages:
        state = fold(state, m)
    assert state.run_id == "r2"
    assert not state.orders


def test_warmup_zero_allowed():
    state = build(simulate_line(ORDERS).messages)
    snap = advance(state, state.last_event_ts, load_all_recipes(), KpiConfig(warmup_orders=0))
    assert not snap.warming_up
    assert snap.window_start_ts == T0
    assert snap.cycle_time.n == 3


def test_warmup_boundary_exactly_n_plus_one():
    state = build(simulate_line(ORDERS).messages)
    as_of = state.last_event_ts
    n2 = advance(state, as_of, load_all_recipes(), KpiConfig(warmup_orders=2))
    assert not n2.warming_up  # exactly N+1 orders
    assert n2.window_start_ts == at(40.0)
    assert n2.cycle_time.n == 1
    n3 = advance(state, as_of, load_all_recipes(), KpiConfig(warmup_orders=3))
    assert n3.warming_up
    assert n3.throughput_per_s is None
