"""Unit tests for the pure ETA error statistics (Phase 7.1, D-05, KPI-08)."""

import random
from datetime import UTC, datetime, timedelta, timezone

from pizza.kpi.eta import EtaErrorStats, eta_error_s, eta_error_stats

T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def test_error_signs():
    assert eta_error_s(T0 + timedelta(seconds=90), T0) == 90.0
    assert eta_error_s(T0 - timedelta(seconds=20), T0) == -20.0
    assert eta_error_s(T0, T0) == 0.0


def test_none_inputs():
    assert eta_error_s(None, T0) is None
    assert eta_error_s(T0, None) is None
    assert eta_error_s(None, None) is None


def test_aware_offsets():
    tz8 = timezone(timedelta(hours=8))
    completed = datetime(2026, 1, 1, 20, 1, 0, tzinfo=tz8)  # 12:01 UTC
    assert eta_error_s(completed, T0) == 60.0


def test_stats_values():
    pairs = {
        "a": (T0 + timedelta(seconds=90), T0),
        "b": (T0 - timedelta(seconds=30), T0),
        "c": (T0 + timedelta(seconds=30), T0),
        "d": (None, T0),
        "e": (T0, None),
    }
    s = eta_error_stats(pairs)
    assert isinstance(s, EtaErrorStats)
    assert s.n == 3
    assert s.mean_error_s == 30.0
    assert s.mean_abs_error_s == 50.0
    assert s.max_late_s == 90.0


def test_stats_no_late_and_empty():
    s = eta_error_stats({"a": (T0 - timedelta(seconds=10), T0)})
    assert s.n == 1
    assert s.max_late_s == 0.0
    e = eta_error_stats({})
    assert e == EtaErrorStats(n=0, mean_error_s=None, mean_abs_error_s=None, max_late_s=None)


def test_stats_order_independent_and_duplicate_collapse():
    base = {
        f"o{i}": (T0 + timedelta(seconds=d), T0) for i, d in enumerate([10, -20, 45, 5])
    }
    items = list(base.items())
    random.Random(1).shuffle(items)
    shuffled = dict(items)
    # duplicates delivered twice collapse via order_id keys
    dup: dict[str, tuple] = {}
    for k, v in items + items:
        dup[k] = v
    expected = eta_error_stats(base)
    assert eta_error_stats(shuffled) == expected
    assert eta_error_stats(dup) == expected
    assert expected.n == 4
    assert expected.mean_error_s == 10.0
    assert expected.mean_abs_error_s == 20.0
    assert expected.max_late_s == 45.0
