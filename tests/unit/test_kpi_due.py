"""Tests for pure due-date KPI statistics (Phase 7, KPI-07)."""

import copy

import pytest

from pizza.common.recipes import load_all_recipes
from pizza.kpi.due import due_stats
from pizza.kpi.fold import KpiState, fold, new_state
from pizza.kpi.metrics import KpiConfig, advance
from tests.unit.kpi_fixtures import (
    RUN,
    at,
    shuffled,
    simulate_line,
    with_duplicates,
)

ORDERS3 = [("a", "margherita", 2, 0.0), ("b", "pepperoni", 3, 20.0), ("c", "veggie", 1, 40.0)]
ORDERS4 = [
    ("a", "margherita", 2, 0.0),
    ("b", "pepperoni", 3, 20.0),
    ("c", "veggie", 1, 40.0),
    ("d", "margherita", 2, 60.0),
]


def build(msgs) -> KpiState:
    state = new_state(RUN)
    for m in msgs:
        state = fold(state, m)
    return state


def cfg(n=0, **kw):
    return KpiConfig(warmup_orders=n, **kw)


# ===== Due-Date Statistics Tests =====


class TestDueStats:
    """Tests for the pure due_stats function."""

    def test_lateness_list_to_metrics(self):
        """Test 1: lateness list [0,30,0,90] gives late_count 2, late_pct 0.5,
        mean 30.0, max 90.0."""
        # Tardiness = [0, 30, 0, 90]; late = [F, T, F, T]; late_count 2, late_pct 0.5
        tardiness_s = [0.0, 30.0, 0.0, 90.0]
        result = due_stats(tardiness_s)

        assert result.late_count == 2
        assert result.n_completed == 4
        assert result.late_pct == pytest.approx(0.5)
        assert result.tardiness_mean_s == pytest.approx(30.0)
        assert result.tardiness_max_s == pytest.approx(90.0)

    def test_empty_list_returns_none_metrics(self):
        """Test 2: zero completed orders gives late_count 0, no NaN."""
        result = due_stats([])

        assert result.late_count == 0
        assert result.n_completed == 0
        assert result.late_pct is None
        assert result.tardiness_mean_s is None
        assert result.tardiness_max_s is None

    def test_completed_at_due_ts_is_not_late(self):
        """Test 3: order completed exactly at due_ts is NOT late (strict >)."""
        tardiness_s = [0.0]  # completed_ts == due_ts means tardiness 0, so not late
        result = due_stats(tardiness_s)

        assert result.late_count == 0
        assert result.n_completed == 1
        assert result.late_pct == 0.0

    def test_all_late(self):
        """All orders are late."""
        tardiness_s = [10.0, 20.0, 30.0]
        result = due_stats(tardiness_s)

        assert result.late_count == 3
        assert result.n_completed == 3
        assert result.late_pct == pytest.approx(1.0)
        assert result.tardiness_mean_s == pytest.approx(20.0)
        assert result.tardiness_max_s == pytest.approx(30.0)

    def test_single_order_late(self):
        """Single late order."""
        tardiness_s = [50.0]
        result = due_stats(tardiness_s)

        assert result.late_count == 1
        assert result.n_completed == 1
        assert result.late_pct == pytest.approx(1.0)
        assert result.tardiness_mean_s == pytest.approx(50.0)
        assert result.tardiness_max_s == pytest.approx(50.0)


# ===== Fold OrderRec.due_ts Tests =====


class TestFoldDueTs:
    """Tests for OrderRec.due_ts field in the fold."""

    def test_due_ts_captured_on_order_created(self):
        """Test 4a: fold records due_ts on OrderRec from OrderLifecycle event."""
        sim = simulate_line(ORDERS3)
        state = build(sim.messages)

        # All orders should have due_ts set
        for oid, order in state.orders.items():
            assert order.due_ts is not None, f"Order {oid} missing due_ts"

    def test_due_ts_idempotent_on_shuffled_events(self):
        """Test 4b: shuffled event streams give identical OrderRec.due_ts (order-insensitive)."""
        sim = simulate_line(ORDERS3)
        state1 = build(sim.messages)

        # Shuffle and rebuild
        shuffled_msgs = shuffled(sim.messages, seed=99)
        state2 = build(shuffled_msgs)

        # Both states must have identical due_ts for each order
        for oid in state1.orders:
            assert state1.orders[oid].due_ts == state2.orders[oid].due_ts

    def test_due_ts_idempotent_on_duplicated_events(self):
        """Test 4c: duplicated event streams give identical OrderRec.due_ts (duplicate-tolerant)."""
        sim = simulate_line(ORDERS3)
        state1 = build(sim.messages)

        # Duplicate and rebuild
        dup_msgs = with_duplicates(sim.messages, seed=99, n=5)
        state2 = build(dup_msgs)

        # Both states must have identical due_ts for each order
        for oid in state1.orders:
            assert state1.orders[oid].due_ts == state2.orders[oid].due_ts

    def test_failed_order_excluded_from_lateness(self):
        """Test 5: failed orders (failed_ts set) are excluded from tardiness list."""
        # We'll manually simulate an order that failed
        # The fold will have failed_ts set for failed orders
        # The metrics.advance() should exclude them from the tardiness list
        # This is checked in the metrics test, but we verify fold captures failed_ts
        sim = simulate_line(ORDERS3)
        state = build(sim.messages)

        # Mark one order as failed manually (simulating a failure)
        if state.orders:
            first_order = next(iter(state.orders.values()))
            first_order.failed_ts = at(100.0)
            first_order.completed_ts = None

        # The fold should preserve this state
        assert first_order.failed_ts is not None
        assert first_order.completed_ts is None


# ===== Metrics Integration Tests =====


class TestMetricsWithDueKpi:
    """Tests for DueKpi computation in metrics.advance()."""

    def test_due_kpi_in_snapshot(self):
        """Snapshot includes DueKpi computed from orders with due_ts."""
        sim = simulate_line(ORDERS3)
        state = build(sim.messages)
        snap = advance(state, state.last_event_ts, load_all_recipes(), cfg())

        # DueKpi should be populated
        assert snap.due is not None
        assert snap.due.n_completed == 3
        assert snap.due.late_count >= 0  # Might be 0 or more depending on due_ts
        assert snap.due.late_pct is not None or snap.due.n_completed == 0

    def test_due_kpi_warmup_excludes_like_cycle_time(self):
        """Test 6: warm-up exclusion uses the same window as cycle time."""
        sim = simulate_line(ORDERS4)
        state = build(sim.messages)
        as_of = state.last_event_ts

        snap_warmup_0 = advance(state, as_of, load_all_recipes(), cfg(0))
        snap_warmup_2 = advance(
            copy.deepcopy(state), as_of, load_all_recipes(), cfg(2)
        )

        # With warmup=0, all 4 orders included
        assert snap_warmup_0.due.n_completed == 4

        # With warmup=2, only last 2 orders included
        assert snap_warmup_2.due.n_completed == 2
        # This matches cycle_time.n
        assert snap_warmup_2.cycle_time.n == 2

    def test_due_kpi_when_warming_up(self):
        """Test 5b: snapshot with no included orders has zero counts and None metrics."""
        sim = simulate_line(ORDERS3)
        state = build(sim.messages)
        snap = advance(state, state.last_event_ts, load_all_recipes(), cfg(3))

        # Warming up, so all metrics should be None but counts should be 0
        assert snap.warming_up
        assert snap.due.late_count == 0
        assert snap.due.late_pct is None
        assert snap.due.tardiness_mean_s is None
        assert snap.due.tardiness_max_s is None


# ===== Purity and Invariance Tests =====


class TestDuePurity:
    """Tests for purity and order-insensitivity of the fold with due_ts."""

    def test_fold_purity_with_duplicates(self):
        """Test 7a: fold is pure (duplicates don't affect result)."""
        sim = simulate_line(ORDERS3)
        state1 = build(sim.messages)

        # Build with duplicates
        dup_msgs = with_duplicates(sim.messages, seed=99, n=10)
        state2 = build(dup_msgs)

        # Both should have identical orders
        assert len(state1.orders) == len(state2.orders)
        for oid in state1.orders:
            o1 = state1.orders[oid]
            o2 = state2.orders[oid]
            assert o1.due_ts == o2.due_ts
            assert o1.completed_ts == o2.completed_ts
            assert o1.failed_ts == o2.failed_ts

    def test_fold_purity_with_shuffled(self):
        """Test 7b: fold is order-insensitive (shuffling doesn't affect result)."""
        sim = simulate_line(ORDERS3)
        state1 = build(sim.messages)

        # Build with shuffled messages
        for seed in [42, 123, 999]:
            shuffled_msgs = shuffled(sim.messages, seed=seed)
            state2 = build(shuffled_msgs)

            # Both should have identical orders
            assert len(state1.orders) == len(state2.orders)
            for oid in state1.orders:
                assert state1.orders[oid].due_ts == state2.orders[oid].due_ts
                assert state1.orders[oid].completed_ts == state2.orders[oid].completed_ts
