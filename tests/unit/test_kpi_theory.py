import pytest

from pizza.common.constants import PRODUCTS
from pizza.common.recipes import load_all_recipes
from pizza.kpi.theory import compute_theory, expected_qty

UNIFORM = dict.fromkeys(PRODUCTS, 1.0)


def test_expected_qty():
    assert expected_qty() == 2.0


def test_capacity_pizzas_is_lots_times_expected_qty():
    t = compute_theory(load_all_recipes(), UNIFORM, 2.0)
    assert t.capacity_per_s * expected_qty() == pytest.approx(0.076923, abs=1e-5)


def test_default_theory_matches_d09():
    t = compute_theory(load_all_recipes(), UNIFORM, 2.0)
    exp = {
        "spin": 8.0,
        "sauce": 6.0,
        "cheese": 6.6667,
        "toppings": 10.6667,
        "bake": 26.0,
        "slice": 6.0,
        "package": 8.0,
    }
    for s, v in exp.items():
        assert t.expected_station_time_s[s] == pytest.approx(v, abs=1e-3)
    assert t.bottleneck_station == "bake"
    assert t.capacity_per_s == pytest.approx(0.038462, abs=1e-5)
    assert t.raw_process_time_s == pytest.approx(71.3333, abs=1e-3)
    assert t.critical_wip == pytest.approx(2.7436, abs=1e-3)


def test_weights_change_numbers_and_normalize():
    r = load_all_recipes()
    w = {"margherita": 1, "pepperoni": 0, "veggie": 0}
    t = compute_theory(r, w, 2.0)
    assert t.expected_station_time_s["bake"] == pytest.approx(2.0 * r["bake"]["margherita"])
    t2 = compute_theory(r, {"margherita": 5, "pepperoni": 0, "veggie": 0}, 2.0)
    assert t2.expected_station_time_s == pytest.approx(t.expected_station_time_s)
    assert t.raw_process_time_s != pytest.approx(
        compute_theory(r, UNIFORM, 2.0).raw_process_time_s
    )


def test_invalid_weights():
    r = load_all_recipes()
    with pytest.raises(ValueError):
        compute_theory(r, dict.fromkeys(PRODUCTS, 0.0), 2.0)
    with pytest.raises(ValueError):
        compute_theory(r, {"margherita": -1.0, "pepperoni": 2.0}, 2.0)


def test_missing_product_raises():
    r = {s: dict(t) for s, t in load_all_recipes().items()}
    del r["sauce"]["veggie"]
    with pytest.raises(ValueError):
        compute_theory(r, UNIFORM, 2.0)


def test_tie_resolves_to_earlier_station():
    r = {s: {"margherita": 5.0} for s in load_all_recipes()}
    t = compute_theory(r, {"margherita": 1.0}, 1.0)
    assert t.bottleneck_station == "spin"
