import copy

import pytest

from pizza.common.constants import STATIONS
from pizza.common.recipes import load_all_recipes
from pizza.kpi.fold import fold, new_state
from pizza.kpi.metrics import KpiConfig, advance
from pizza.kpi.theory import compute_theory, expected_qty
from tests.unit.kpi_fixtures import RUN, T0, at, cooling_rework_log, simulate_line

ORDERS = [
    ("o1", "margherita", 2, 0.0),
    ("o2", "pepperoni", 3, 20.0),
    ("o3", "veggie", 1, 40.0),
    ("o4", "margherita", 3, 60.0),
]


def build(msgs, run_id=RUN):
    state = new_state(run_id)
    for m in msgs:
        state = fold(state, m)
    return state


def cfg(**kw):
    return KpiConfig(warmup_orders=0, **kw)


def test_utilization_matches_oracle():
    sim = simulate_line(ORDERS, track_in_s=0.5)
    state = build(sim.messages)
    as_of = state.last_event_ts
    snap = advance(state, as_of, load_all_recipes(), cfg())
    window = (as_of - T0).total_seconds()
    for s in STATIONS:
        u = snap.stations[s].utilization
        assert u == pytest.approx(sim.busy_s[s] / window)
        assert u <= 1.0001


def test_open_busy_interval():
    sim = simulate_line([("o1", "margherita", 2, 0.0)])
    # Keep only up to the first busy of 'spin' and drop its idle.
    msgs = [
        m
        for m in sim.messages
        if not (m.type == "station.status" and m.machine_id == "spin" and m.status == "idle")
        and not (m.type.startswith("op.") and getattr(m, "step", "") == "spin" and m.ts > at(0))
    ]
    state = build([m for m in msgs if m.ts <= at(5.0)])
    snap = advance(state, at(5.0), load_all_recipes(), cfg())
    assert snap.stations["spin"].utilization == pytest.approx(1.0)


def test_repeated_busy_is_noop_and_offline_closes():
    from pizza.common.envelope import StationStatus

    state = new_state(RUN)
    base = simulate_line([("o1", "margherita", 1, 0.0)])
    for m in base.messages:
        if m.type == "order.lifecycle":
            state = fold(state, m)
    for st, off in [("busy", 0), ("busy", 2), ("offline", 4), ("idle", 6)]:
        ev = StationStatus.make(run_id=RUN, machine_id="bake", ts=at(off), status=st)
        state = fold(state, ev)
    snap = advance(state, at(10.0), load_all_recipes(), cfg())
    assert snap.stations["bake"].utilization == pytest.approx(0.4)


def test_queue_wait_track_in():
    sim = simulate_line(ORDERS, track_in_s=0.5)
    state = build(sim.messages)
    snap = advance(state, state.last_event_ts, load_all_recipes(), cfg())
    for s in STATIONS:
        k = snap.stations[s]
        assert k.track_in_mean_s == pytest.approx(0.5)
        assert k.wait_mean_s is not None and k.wait_mean_s >= 0
        assert k.queue_mean_s == pytest.approx(k.wait_mean_s + 0.5)
        assert k.queue_samples == 4
    # bake is the bottleneck: some order must have waited there.
    assert snap.stations["bake"].wait_p95_s > 0


def test_station_throughput_counts_op_completions():
    sim = simulate_line(ORDERS, track_in_s=0.5)
    state = build(sim.messages)
    snap = advance(state, state.last_event_ts, load_all_recipes(), cfg())
    window = snap.window_s
    for s in STATIONS:
        done = sum(1 for (_o, step, _v), r in state.ops.items() if step == s and r.completed_ts)
        assert snap.stations[s].throughput_per_s == pytest.approx(done / window)


def test_station_pizzas_per_s_weights_by_qty():
    sim = simulate_line(ORDERS, track_in_s=0.5)
    state = build(sim.messages)
    snap = advance(state, state.last_event_ts, load_all_recipes(), cfg())
    window = snap.window_s
    qty = {o: q for (o, _p, q, _t) in ORDERS}
    for s in STATIONS:
        done = sum(
            qty[o] for (o, step, _v), r in state.ops.items() if step == s and r.completed_ts
        )
        assert snap.stations[s].pizzas_per_s == pytest.approx(done / window)
    assert snap.theory.capacity_pizzas_per_s == pytest.approx(
        snap.theory.capacity_per_s * expected_qty()
    )


def test_wip_pizzas_sums_qty_of_wip_orders():
    sim = simulate_line(ORDERS, track_in_s=0.5)
    mid = at(30)
    state = build([m for m in sim.messages if m.ts <= mid])
    snap = advance(state, mid, load_all_recipes(), cfg())
    expected = sum(
        o.qty
        for o in state.orders.values()
        if o.completed_ts is None
        and o.failed_ts is None
        and not (o.bank_intervals and o.bank_intervals[-1].end is None)
    )
    assert snap.wip_pizzas == expected
    assert snap.wip_now > 0 and snap.wip_pizzas >= snap.wip_now


def test_negative_durations_counted():
    sim = simulate_line(ORDERS[:2], track_in_s=0.5)
    state = build(sim.messages)
    rec = next(r for (o, s, _v), r in state.ops.items() if o == "o1" and s == "spin")
    rec.started_ts = rec.dispatched_ts.replace(year=2020)
    snap = advance(state, state.last_event_ts, load_all_recipes(), cfg())
    assert snap.negative_durations == 1
    assert snap.stations["spin"].queue_samples == 1


def test_cooling_rework_track_in_excludes_ttl():
    """Phase 6.2 D-10: v2 dispatched after TTL; Track-In is dispatch->start only."""
    state = build(cooling_rework_log())
    snap = advance(state, at(11), load_all_recipes(), cfg())
    k = snap.stations["spin"]
    assert k.track_in_mean_s == pytest.approx((1.0 + 0.2) / 2)
    assert k.wait_mean_s == pytest.approx((1.0 + 5.4) / 2)
    assert k.queue_mean_s == pytest.approx((2.0 + 5.6) / 2)


def test_theory_next_to_measured():
    sim = simulate_line(ORDERS, track_in_s=0.5)
    state = build(sim.messages)
    snap = advance(state, state.last_event_ts, load_all_recipes(), cfg())
    assert snap.theory.weights_source == "observed"
    w = {"margherita": 2.0, "pepperoni": 1.0, "veggie": 1.0}
    th = compute_theory(load_all_recipes(), w, expected_qty())
    assert snap.theory.capacity_per_s == pytest.approx(th.capacity_per_s)
    assert snap.theory.bottleneck_station == th.bottleneck_station
    assert snap.capacity_ratio == pytest.approx(snap.throughput_per_s / th.capacity_per_s)


def test_weights_observed_fallback():
    state = new_state(RUN)
    snap = advance(state, T0, load_all_recipes(), cfg())
    assert snap.theory.weights_source == "uniform"
    assert snap.capacity_ratio is None
    state = new_state(RUN, {"margherita": 1.0, "pepperoni": 0.0, "veggie": 0.0}, "run_started")
    snap = advance(state, T0, load_all_recipes(), cfg())
    assert snap.theory.weights_source == "run_started"
    bad = new_state(RUN, {"margherita": 0.0}, "run_started")
    snap = advance(bad, T0, load_all_recipes(), cfg())
    assert snap.theory.capacity_per_s is None
    assert snap.theory.expected_station_time_s == {}


def test_bottleneck_tick_driven():
    sim = simulate_line(ORDERS, track_in_s=0.0)
    state = build(sim.messages)
    as_of = state.last_event_ts
    c = cfg(bottleneck_confirm_n=3, bottleneck_min_util=0.0)
    counts = []
    for _ in range(3):
        snap = advance(state, as_of, load_all_recipes(), c)
        counts.append((snap.bottleneck, snap.bottleneck_candidate_count))
    assert counts[0] == (None, 1)
    assert counts[1] == (None, 2)
    assert counts[2][0] is not None
    assert state.bottleneck == counts[2][0]
    snap = advance(copy.deepcopy(new_state(RUN)), as_of, load_all_recipes(), c)
    assert snap.bottleneck is None
