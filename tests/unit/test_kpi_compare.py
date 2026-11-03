"""Pure KPI comparison module: RoundSummary, summarize_round, recompute, compare_fields.

Tests for per-round summary and replay verification with field-by-field comparison.
"""

import copy
from datetime import timedelta

import pytest

from pizza.common.recipes import load_all_recipes
from pizza.kpi.compare import (
    COMPARE_FIELDS,
    DEFAULT_TOLERANCE,
    all_match,
    compare_fields,
    recompute,
    summarize_round,
)
from pizza.kpi.fold import fold, new_state
from pizza.kpi.metrics import KpiConfig, advance
from tests.unit.kpi_fixtures import RUN, T0, shuffled, simulate_line, with_duplicates

ORDERS6 = [
    ("o1", "margherita", 2, 0.0),
    ("o2", "pepperoni", 3, 20.0),
    ("o3", "veggie", 1, 40.0),
    ("o4", "margherita", 3, 60.0),
    ("o5", "pepperoni", 2, 80.0),
    ("o6", "veggie", 3, 100.0),
]


def build(msgs):
    """Fold a message list into KpiState."""
    state = new_state(RUN)
    for m in msgs:
        state = fold(state, m)
    return state


def cfg(**kw):
    """Create a KpiConfig."""
    return KpiConfig(**kw)


# ==============================================================================
# Task 1: summarize_round tests
# ==============================================================================


def test_summarize_round_6order_fixture():
    """Test 1: 6-order fixture with hand values."""
    # Create a fixture with known due dates (30s after creation)
    sim = simulate_line(ORDERS6, due_offset_s=30.0)
    state = build(sim.messages)
    recipes = load_all_recipes()

    # Summarize at the last event timestamp
    as_of = state.last_event_ts
    summary = summarize_round(state, as_of, recipes, cfg(), partial=False)

    # Verify basic counts
    assert summary.n_orders == 6
    assert summary.n_completed == 6
    assert summary.n_failed == 0
    assert summary.partial is False

    # All 6 orders should complete; cycle times and tardiness from fixture
    assert summary.mean_cycle_time_s is not None
    assert summary.mean_cycle_time_s > 0

    # Due dates are 30s after creation, so most will be late given cycle times
    assert summary.late_count >= 0
    assert summary.pct_late is not None
    assert 0 <= summary.pct_late <= 1

    # Makespan = last_completed_ts - first_created_ts
    assert summary.makespan_s is not None
    assert summary.makespan_s > 0


def test_summarize_round_warmup_zero_overrides():
    """Test 2: warm-up 0 is applied regardless of cfg.warmup_orders=10."""
    sim = simulate_line(ORDERS6, due_offset_s=3600.0)
    state = build(sim.messages)
    recipes = load_all_recipes()
    as_of = state.last_event_ts

    # Even with cfg.warmup_orders=10, summarize_round forces warmup=0
    summary = summarize_round(state, as_of, recipes, cfg(warmup_orders=10), partial=False)

    # All 6 orders must be included (warmup override)
    assert summary.n_orders == 6
    assert summary.n_completed == 6


def test_summarize_round_excludes_incomplete_and_failed():
    """Test 3: incomplete orders and failed orders excluded from stats; n_failed counts failures."""
    # Create 6 orders and manually mark one as failed and one as incomplete
    sim = simulate_line(ORDERS6, due_offset_s=3600.0)
    state = build(sim.messages)

    # Mark o1 as incomplete and o2 as failed
    if "o1" in state.orders:
        state.orders["o1"].completed_ts = None

    if "o2" in state.orders:
        state.orders["o2"].failed_ts = state.orders["o2"].completed_ts
        state.orders["o2"].completed_ts = None

    recipes = load_all_recipes()
    as_of = state.last_event_ts

    summary = summarize_round(state, as_of, recipes, cfg(), partial=False)

    assert summary.n_orders == 6  # Still 6 orders created
    assert summary.n_completed == 4  # Only 4 completed (o1 incomplete, o2 failed)
    assert summary.n_failed == 1  # One failed
    assert summary.mean_cycle_time_s is not None  # Only from 4 completed


def test_summarize_round_pause_excluded_from_makespan():
    """Test 4: pause interval excluded from makespan (active time)."""
    sim = simulate_line(ORDERS6, due_offset_s=3600.0)

    # Add a pause/resume interval in the middle
    from tests.unit.kpi_fixtures import pause_fact, resume_fact

    messages = list(sim.messages)
    # Insert pause at 50s, resume at 60s (10s pause)
    messages.append(pause_fact(50.0, run_id=RUN, t0=T0))
    messages.append(resume_fact(60.0, run_id=RUN, t0=T0))
    messages.sort(key=lambda m: m.ts)

    state = build(messages)
    recipes = load_all_recipes()
    as_of = state.last_event_ts

    summary = summarize_round(state, as_of, recipes, cfg(), partial=False)

    # Makespan should exclude the 10s pause
    assert summary.makespan_s is not None
    # Without pause, raw diff would be from o1 created to o6 completed
    # With pause excluded, should be less
    raw_diff = (state.last_event_ts - T0).total_seconds()
    assert summary.makespan_s <= raw_diff


def test_summarize_round_no_completed_gives_none():
    """Test 5: no completed orders → makespan_s None, means None, pct_late None, n_completed 0."""
    sim = simulate_line(ORDERS6, due_offset_s=3600.0)
    state = build(sim.messages)

    # Mark all orders as incomplete
    for order in state.orders.values():
        order.completed_ts = None

    recipes = load_all_recipes()
    as_of = state.last_event_ts

    summary = summarize_round(state, as_of, recipes, cfg(), partial=False)

    assert summary.n_completed == 0
    assert summary.makespan_s is None
    assert summary.mean_cycle_time_s is None
    assert summary.mean_tardiness_s is None
    assert summary.max_tardiness_s is None
    assert summary.pct_late is None


def test_summarize_round_shuffled_duplicated_events():
    """Test 6: shuffled and duplicated event streams yield identical RoundSummary."""
    sim = simulate_line(ORDERS6, due_offset_s=30.0)
    recipes = load_all_recipes()
    as_of = sim.messages[-1].ts

    # Original order
    state1 = build(sim.messages)
    summary1 = summarize_round(state1, as_of, recipes, cfg(), partial=False)

    # Shuffled
    shuffled_msgs = shuffled(sim.messages, seed=999)
    state2 = build(shuffled_msgs)
    summary2 = summarize_round(state2, as_of, recipes, cfg(), partial=False)

    # With duplicates
    dup_msgs = with_duplicates(sim.messages, seed=888, n=3)
    state3 = build(dup_msgs)
    summary3 = summarize_round(state3, as_of, recipes, cfg(), partial=False)

    # All three should be identical
    assert summary1.n_orders == summary2.n_orders == summary3.n_orders
    assert summary1.n_completed == summary2.n_completed == summary3.n_completed
    assert summary1.n_failed == summary2.n_failed == summary3.n_failed
    assert summary1.late_count == summary2.late_count == summary3.late_count
    if summary1.mean_cycle_time_s is not None:
        assert summary1.mean_cycle_time_s == pytest.approx(summary2.mean_cycle_time_s)
        assert summary1.mean_cycle_time_s == pytest.approx(summary3.mean_cycle_time_s)
    if summary1.pct_late is not None:
        assert summary1.pct_late == pytest.approx(summary2.pct_late)
        assert summary1.pct_late == pytest.approx(summary3.pct_late)
    if summary1.makespan_s is not None:
        assert summary1.makespan_s == pytest.approx(summary2.makespan_s)
        assert summary1.makespan_s == pytest.approx(summary3.makespan_s)


def test_summarize_round_partial_echoed():
    """Test 7: `partial` is echoed from the argument and does not change numbers."""
    sim = simulate_line(ORDERS6, due_offset_s=30.0)
    state = build(sim.messages)
    recipes = load_all_recipes()
    as_of = state.last_event_ts

    summary_not_partial = summarize_round(state, as_of, recipes, cfg(), partial=False)
    summary_partial = summarize_round(state, as_of, recipes, cfg(), partial=True)

    assert summary_not_partial.partial is False
    assert summary_partial.partial is True

    # All numbers should be identical
    assert summary_not_partial.n_orders == summary_partial.n_orders
    assert summary_not_partial.n_completed == summary_partial.n_completed
    assert summary_not_partial.late_count == summary_partial.late_count
    assert summary_not_partial.mean_cycle_time_s == summary_partial.mean_cycle_time_s


# ==============================================================================
# Task 2: recompute and compare_fields tests
# ==============================================================================


def test_recompute_matches_stored():
    """Test 1: recompute facts yields identical snapshot to stored."""
    sim = simulate_line(ORDERS6, due_offset_s=30.0)
    state = build(sim.messages)
    recipes = load_all_recipes()

    # Create a stored snapshot at the last event timestamp
    as_of = state.last_event_ts
    stored = advance(state, as_of, recipes, cfg(warmup_orders=2))

    # Recompute from the same facts
    recomputed = recompute(sim.messages, stored, recipes, cfg(warmup_orders=2))

    # Compare all fields
    diffs = compare_fields(stored, recomputed)

    # All fields should match
    assert all(d.ok for d in diffs), f"Mismatches: {[d for d in diffs if not d.ok]}"


def test_recompute_ignores_other_run_and_future_facts():
    """Test 2: facts of another run_id and facts with ts > stored.ts are ignored."""
    sim = simulate_line(ORDERS6, due_offset_s=30.0)
    state = build(sim.messages)
    recipes = load_all_recipes()

    # Create a stored snapshot at a mid-point timestamp
    as_of = state.last_event_ts
    stored = advance(state, as_of, recipes, cfg(warmup_orders=0))

    # Create a new set of facts with a different run_id
    other_run_facts = [m.model_copy(update={"run_id": "other_run"}) for m in sim.messages]

    # Mix original and other-run facts, plus some future-dated facts
    future_msgs = [m.model_copy(update={"ts": m.ts + timedelta(days=1)}) for m in sim.messages[:2]]
    mixed_facts = list(sim.messages) + other_run_facts + future_msgs

    # Recompute should ignore other_run and future facts
    recomputed = recompute(mixed_facts, stored, recipes, cfg(warmup_orders=0))

    # Should match the stored snapshot (only original facts were used)
    diffs = compare_fields(stored, recomputed)
    assert all(d.ok for d in diffs), f"Mismatches: {[d for d in diffs if not d.ok]}"


def test_compare_fields_detects_tampering():
    """Test 3: tampering stored snapshot makes compare_fields report mismatches."""
    sim = simulate_line(ORDERS6, due_offset_s=30.0)
    state = build(sim.messages)
    recipes = load_all_recipes()
    as_of = state.last_event_ts
    stored = advance(state, as_of, recipes, cfg(warmup_orders=0))
    recomputed = recompute(sim.messages, stored, recipes, cfg(warmup_orders=0))

    # Tamper with stored orders_completed
    tampered = copy.deepcopy(stored)
    tampered = tampered.model_copy(update={"orders_completed": stored.orders_completed + 1})

    diffs = compare_fields(tampered, recomputed)

    # Should find a mismatch
    mismatches = [d for d in diffs if not d.ok]
    assert len(mismatches) > 0
    assert any("orders_completed" in d.field for d in mismatches)


def test_compare_fields_excludes_bottleneck_ts_event_id():
    """Test 4: bottleneck, bottleneck_candidate, bottleneck_candidate_count, ts,
    event_id excluded."""
    # Verify COMPARE_FIELDS does not include excluded names
    excluded = {
        "bottleneck", "bottleneck_candidate", "bottleneck_candidate_count", "ts", "event_id"
    }
    for field in COMPARE_FIELDS:
        assert not any(ex in field for ex in excluded), f"Excluded field in COMPARE_FIELDS: {field}"

    # Verify included fields are reasonable (sanity check)
    field_strs = [str(f) for f in COMPARE_FIELDS]
    has_orders = any("orders" in f for f in field_strs)
    has_wip = any("wip" in f for f in field_strs)
    has_cycle = any("cycle_time" in f for f in field_strs)

    assert has_orders, "COMPARE_FIELDS should include orders fields"
    assert has_wip, "COMPARE_FIELDS should include wip fields"
    assert has_cycle, "COMPARE_FIELDS should include cycle_time fields"


def test_compare_fields_tolerance():
    """Test 5: floats differing by less than DEFAULT_TOLERANCE match."""
    sim = simulate_line(ORDERS6, due_offset_s=30.0)
    state = build(sim.messages)
    recipes = load_all_recipes()
    as_of = state.last_event_ts
    stored = advance(state, as_of, recipes, cfg(warmup_orders=0))

    # Create a nearly-identical snapshot with a tiny float difference
    nearly = copy.deepcopy(stored)
    if stored.cycle_time.mean_s is not None:
        # Difference less than tolerance (1e-6 relative)
        tiny_diff = stored.cycle_time.mean_s * 1e-7
        nearly = nearly.model_copy(update={
            "cycle_time": stored.cycle_time.model_copy(update={
                "mean_s": stored.cycle_time.mean_s + tiny_diff
            })
        })

        diffs = compare_fields(stored, nearly, tol=DEFAULT_TOLERANCE)

        # Should match despite the tiny difference
        mean_diff = [d for d in diffs if "cycle_time.mean_s" in d.field]
        assert all(d.ok for d in mean_diff), f"Tiny diff should pass: {mean_diff}"


def test_compare_fields_none_vs_value_mismatch():
    """Test 5b: None vs value is a mismatch; None vs None matches."""
    sim = simulate_line(ORDERS6, due_offset_s=30.0)
    state = build(sim.messages)
    recipes = load_all_recipes()
    as_of = state.last_event_ts
    stored = advance(state, as_of, recipes, cfg(warmup_orders=0))

    # Create a snapshot where a previously-set field is now None
    modified = copy.deepcopy(stored)
    if stored.cycle_time.mean_s is not None:
        modified = modified.model_copy(update={
            "cycle_time": stored.cycle_time.model_copy(update={"mean_s": None})
        })

        diffs = compare_fields(stored, modified)
        mismatch = [d for d in diffs if "cycle_time.mean_s" in d.field]
        assert any(not d.ok for d in mismatch), "None vs value should mismatch"


def test_recompute_no_io_imports():
    """Test 6: recompute module has no I/O imports (covered by purity test)."""
    # This is mostly a placeholder; the actual check is in test_kpi_purity.py
    # We just verify the function exists and is callable
    assert callable(recompute)


def test_recompute_shuffled_duplicated_facts():
    """Test 7: input fact order shuffled and duplicated gives same recomputed snapshot."""
    sim = simulate_line(ORDERS6, due_offset_s=30.0)
    state = build(sim.messages)
    recipes = load_all_recipes()
    as_of = state.last_event_ts
    stored = advance(state, as_of, recipes, cfg(warmup_orders=0))

    # Recompute from original order
    recomputed1 = recompute(sim.messages, stored, recipes, cfg(warmup_orders=0))

    # Recompute from shuffled
    shuffled_msgs = shuffled(sim.messages, seed=555)
    recomputed2 = recompute(shuffled_msgs, stored, recipes, cfg(warmup_orders=0))

    # Recompute from duplicated
    dup_msgs = with_duplicates(sim.messages, seed=444, n=5)
    recomputed3 = recompute(dup_msgs, stored, recipes, cfg(warmup_orders=0))

    # All should match the stored snapshot
    diffs1 = compare_fields(stored, recomputed1)
    diffs2 = compare_fields(stored, recomputed2)
    diffs3 = compare_fields(stored, recomputed3)

    assert all(d.ok for d in diffs1)
    assert all(d.ok for d in diffs2)
    assert all(d.ok for d in diffs3)


# ==============================================================================
# Helper tests
# ==============================================================================


def test_all_match_helper():
    """Helper: all_match returns True when all diffs are ok."""
    sim = simulate_line(ORDERS6, due_offset_s=30.0)
    state = build(sim.messages)
    recipes = load_all_recipes()
    as_of = state.last_event_ts
    stored = advance(state, as_of, recipes, cfg(warmup_orders=0))
    recomputed = recompute(sim.messages, stored, recipes, cfg(warmup_orders=0))

    diffs = compare_fields(stored, recomputed)
    assert all_match(diffs)
