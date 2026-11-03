from collections import Counter
from datetime import UTC, datetime, timedelta
from itertools import islice

import pytest

from pizza.common.constants import PRODUCTS
from pizza.common.envelope import RunParams
from pizza.common.ids import make_order_id
from pizza.common.recipes import load_all_recipes, theoretical_total_s
from pizza.generator.plan import plan_orders

T0 = datetime(2026, 1, 1, tzinfo=UTC)
RECIPES = load_all_recipes()


def take(params, n, t0=T0):
    return list(islice(plan_orders(params, t0, RECIPES), n))


def test_same_seed_reproducibility():
    p = RunParams(seed=7)
    assert take(p, 50) == take(RunParams(seed=7), 50)


def test_different_seed_difference():
    assert take(RunParams(seed=1), 20) != take(RunParams(seed=2), 20)


def test_same_seed_different_t0_shifts():
    a = take(RunParams(seed=3), 10)
    delta = timedelta(seconds=90)
    b = take(RunParams(seed=3), 10, T0 + delta)
    for x, y in zip(a, b, strict=True):
        assert (x.order_id, x.product, x.qty, x.offset_s) == (
            y.order_id,
            y.product,
            y.qty,
            y.offset_s,
        )
        assert y.created_ts - x.created_ts == delta
        assert y.due_ts - x.due_ts == delta


def test_stream_independence():
    base = take(RunParams(seed=5), 30)
    other_w = take(RunParams(seed=5, weights={PRODUCTS[0]: 1.0}), 30)
    assert [o.offset_s for o in base] == [o.offset_s for o in other_w]
    other_rate = take(RunParams(seed=5, arrival_rate=0.5), 30)
    assert [o.qty for o in base] == [o.qty for o in other_rate]


def test_qty_and_mix():
    orders = take(RunParams(seed=9, arrival_rate=100), 2000)
    assert {o.qty for o in orders} == {1, 2, 3}
    weights = dict.fromkeys(PRODUCTS, 0.0)
    weights[PRODUCTS[0]] = 3.0
    weights[PRODUCTS[1]] = 1.0
    orders = take(RunParams(seed=9, weights=weights), 2000)
    counts = Counter(o.product for o in orders)
    assert set(counts) == {PRODUCTS[0], PRODUCTS[1]}
    assert 2.4 < counts[PRODUCTS[0]] / counts[PRODUCTS[1]] < 3.7


def test_mean_interarrival():
    orders = take(RunParams(seed=11, arrival_rate=0.1), 5000)
    mean = orders[-1].offset_s / 5000
    assert abs(mean - 10.0) / 10.0 < 0.05


def test_due_ts_fixed_k():
    for o in take(RunParams(seed=1, k=2.5), 20):
        expect = o.created_ts + timedelta(
            seconds=theoretical_total_s(RECIPES, o.product, o.qty) * 2.5
        )
        assert abs(o.due_ts - expect) < timedelta(milliseconds=1)
        assert o.due_ts.microsecond % 1000 == 0
        assert o.created_ts.microsecond % 1000 == 0
        assert o.due_ts.utcoffset() == timedelta(0)


def test_due_ts_k_range():
    for o in take(RunParams(seed=1, k=1.5, k_max=3.0), 100):
        total = theoretical_total_s(RECIPES, o.product, o.qty)
        k_eff = (o.due_ts - o.created_ts).total_seconds() / total
        assert 1.5 - 1e-3 <= k_eff <= 3.0 + 1e-3


def test_max_orders_and_ids():
    orders = list(plan_orders(RunParams(seed=4, max_orders=5), T0, RECIPES))
    assert len(orders) == 5
    assert [o.order_id for o in orders] == [make_order_id(4, n) for n in range(1, 6)]
    assert orders[0].order_id == "L-0001"


def test_unbounded_is_iterator():
    assert len(take(RunParams(seed=4), 7)) == 7


def test_unknown_product_raises_eagerly():
    with pytest.raises(ValueError):
        plan_orders(RunParams(weights={"nonexistent": 1.0}), T0, RECIPES)


def _plan_offsets(params, n, rate_source=None):
    return [
        o.offset_s for o in islice(plan_orders(params, T0, RECIPES, rate_source=rate_source), n)
    ]


def test_rate_source_constant_is_identical():
    params = RunParams(seed=7, arrival_rate=0.5)
    base = _plan_offsets(params, 20)
    assert _plan_offsets(params, 20, rate_source=lambda: 0.5) == base


def test_rate_change_scales_later_gaps_only():
    params = RunParams(seed=7, arrival_rate=0.5)
    base = _plan_offsets(params, 10)
    state = {"rate": 0.5}
    orders = plan_orders(params, T0, RECIPES, rate_source=lambda: state["rate"])
    got = []
    for i, order in enumerate(orders):
        got.append(order.offset_s)
        if i == 3:
            state["rate"] = 1.0  # doubled rate -> gaps halve
        if i == 9:
            break
    assert got[:4] == base[:4]
    for k in range(4, 10):
        assert got[k] - got[k - 1] == pytest.approx((base[k] - base[k - 1]) * 0.5)


def test_non_positive_rate_source_falls_back():
    params = RunParams(seed=7, arrival_rate=0.5)
    assert _plan_offsets(params, 5, rate_source=lambda: 0.0) == _plan_offsets(params, 5)


def test_planned_order_eta_fields_default_none_and_plan_deterministic():
    params = RunParams(seed=11, k=1.5, k_max=3.0)
    orders = take(params, 20)
    assert all(o.eta_ts is None and o.station_eta is None for o in orders)
    again = take(RunParams(seed=11, k=1.5, k_max=3.0), 20)
    assert [o.due_ts for o in orders] == [o.due_ts for o in again]
    for o in orders:
        total = theoretical_total_s(RECIPES, o.product, o.qty)
        lead = (o.due_ts - o.created_ts).total_seconds()
        assert 1.5 * total - 0.002 <= lead <= 3.0 * total + 0.002


# -- Phase 07.2 numeric priority (D-08) -------------------------------------
_GOLDEN_SEED_42 = [
    ("2026-01-01T00:00:49.499000+00:00", "margherita", 2, "2026-01-01T00:03:01.499000+00:00"),
    ("2026-01-01T00:01:11.275000+00:00", "veggie", 2, "2026-01-01T00:03:39.275000+00:00"),
    ("2026-01-01T00:01:15.793000+00:00", "pepperoni", 1, "2026-01-01T00:02:29.793000+00:00"),
    ("2026-01-01T00:01:35.095000+00:00", "margherita", 2, "2026-01-01T00:03:47.095000+00:00"),
    ("2026-01-01T00:01:51.293000+00:00", "margherita", 2, "2026-01-01T00:04:03.293000+00:00"),
]


def test_existing_streams_byte_identical_golden_seed_42():
    got = [
        (o.created_ts.isoformat(), o.product, o.qty, o.due_ts.isoformat())
        for o in take(RunParams(seed=42), 5)
    ]
    assert got == _GOLDEN_SEED_42


def test_priority_ratio_zero_all_default():
    orders = list(islice(plan_orders(RunParams(seed=5), T0, RECIPES, priority_high_ratio=0.0), 50))
    assert {o.priority for o in orders} == {50}


def test_priority_ratio_one_all_high_value():
    orders = list(
        islice(
            plan_orders(
                RunParams(seed=5), T0, RECIPES, priority_high_ratio=1.0, priority_high_value=7
            ),
            50,
        )
    )
    assert {o.priority for o in orders} == {7}


def test_priority_deterministic_and_default_ratio_band():
    a = take(RunParams(seed=9), 2000)
    b = take(RunParams(seed=9), 2000)
    assert [o.priority for o in a] == [o.priority for o in b]
    high = sum(1 for o in a if o.priority == 10)
    assert 0.07 < high / 2000 < 0.13
    assert {o.priority for o in a} <= {10, 50}


def test_priority_does_not_perturb_other_streams():
    base = take(RunParams(seed=11), 30)
    other = list(islice(plan_orders(RunParams(seed=11), T0, RECIPES, priority_high_ratio=1.0), 30))
    assert [(o.created_ts, o.product, o.qty, o.due_ts) for o in base] == [
        (o.created_ts, o.product, o.qty, o.due_ts) for o in other
    ]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"priority_high_ratio": -0.1},
        {"priority_high_ratio": 1.1},
        {"priority_high_value": 0},
        {"priority_high_value": 100},
    ],
)
def test_priority_params_validated(kwargs):
    with pytest.raises(ValueError):
        plan_orders(RunParams(seed=1), T0, RECIPES, **kwargs)
