"""Tests for the offline rule-differentiation oracle (RESEARCH Pitfall 8).

``scripts/rule_oracle.py`` is not a package module (it lives under
``scripts/``, not ``src/pizza``), so it is loaded here via ``importlib`` from
its file path rather than imported normally.
"""

import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "rule_oracle.py"


def _load_rule_oracle():
    spec = importlib.util.spec_from_file_location("rule_oracle", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


rule_oracle = _load_rule_oracle()

T0 = datetime(2024, 1, 1, tzinfo=UTC)


def _order(order_id: str, offset_s: float, due_offset_s: float):
    created = T0 + timedelta(seconds=offset_s)
    return rule_oracle.PlannedOrder(
        seq=int(offset_s) + 1,
        order_id=order_id,
        offset_s=offset_s,
        created_ts=created,
        product="p",
        qty=1,
        due_ts=T0 + timedelta(seconds=due_offset_s),
    )


def test_simulate_line_queue_fifo_hand_computed() -> None:
    """Single-station (bake, 10s/order) hand-computed FIFO completion times."""
    orders = [
        _order("o1", 0.0, 1000.0),
        _order("o2", 1.0, 200.0),
        _order("o3", 2.0, 15.0),
    ]
    recipes = {"bake": {"p": 10.0}}

    completion = rule_oracle.simulate_line_queue(orders, recipes, "fifo", route=("bake",))

    assert completion["o1"] == T0 + timedelta(seconds=10)
    assert completion["o2"] == T0 + timedelta(seconds=20)
    assert completion["o3"] == T0 + timedelta(seconds=30)


def test_simulate_line_queue_edd_overtakes_hand_computed() -> None:
    """Same case under EDD: o3 (created later, due sooner) overtakes o2."""
    orders = [
        _order("o1", 0.0, 1000.0),
        _order("o2", 1.0, 200.0),
        _order("o3", 2.0, 15.0),
    ]
    recipes = {"bake": {"p": 10.0}}

    completion = rule_oracle.simulate_line_queue(orders, recipes, "edd", route=("bake",))

    assert completion["o1"] == T0 + timedelta(seconds=10)
    # o3 was created after o2 but has the earlier due date: EDD serves it first.
    assert completion["o3"] == T0 + timedelta(seconds=20)
    assert completion["o2"] == T0 + timedelta(seconds=30)


def test_compare_rules_deterministic_same_seed_twice() -> None:
    first = rule_oracle.compare_rules(seed=7, n=8, rate_per_min=6.0, k=1.5)
    second = rule_oracle.compare_rules(seed=7, n=8, rate_per_min=6.0, k=1.5)

    assert first == second


def test_compare_rules_default_params_differentiate() -> None:
    """At shipped defaults, at least two rules differ in mean_tardiness_s by > 1s.

    Documents Pitfall 8: if this fails, print the parameters so DEFAULT_K (or
    DEFAULT_RATE_PER_MIN, capacity-bounded) can be adjusted in
    src/pizza/dashboard/comparison.py.
    """
    results = rule_oracle.compare_rules(
        seed=rule_oracle.DEFAULT_SEED,
        n=rule_oracle.DEFAULT_N,
        rate_per_min=rule_oracle.DEFAULT_RATE_PER_MIN,
        k=rule_oracle.DEFAULT_K,
    )

    tardiness = {rule: results[rule]["mean_tardiness_s"] for rule in rule_oracle.RULES}
    max_diff = max(tardiness.values()) - min(tardiness.values())

    assert max_diff > 1.0, (
        f"rules did not differentiate at defaults "
        f"(seed={rule_oracle.DEFAULT_SEED}, n={rule_oracle.DEFAULT_N}, "
        f"rate_per_min={rule_oracle.DEFAULT_RATE_PER_MIN}, k={rule_oracle.DEFAULT_K}): "
        f"{tardiness}"
    )


def test_simulate_line_queue_priority_overtakes_fifo() -> None:
    """A smaller priority number is served first even under FIFO (07.2)."""
    low = _order("o1", 0.0, 1000.0)
    waiting = _order("o2", 1.0, 1000.0)
    urgent = rule_oracle.PlannedOrder(
        seq=3,
        order_id="o3",
        offset_s=2.0,
        created_ts=T0 + timedelta(seconds=2),
        product="p",
        qty=1,
        due_ts=T0 + timedelta(seconds=1000),
        priority=10,
    )
    recipes = {"bake": {"p": 10.0}}

    completion = rule_oracle.simulate_line_queue(
        [low, waiting, urgent], recipes, "fifo", route=("bake",)
    )

    assert completion["o1"] == T0 + timedelta(seconds=10)
    assert completion["o3"] == T0 + timedelta(seconds=20)
    assert completion["o2"] == T0 + timedelta(seconds=30)
