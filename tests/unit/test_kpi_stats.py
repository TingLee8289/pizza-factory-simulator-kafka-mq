import pytest

from pizza.kpi.stats import mean, percentile


def test_percentile_empty_none():
    assert percentile([], 0.5) is None


def test_percentile_single():
    assert percentile([7.0], 0.95) == 7.0


def test_percentile_interpolation():
    assert percentile([1, 2, 3, 4], 0.5) == pytest.approx(2.5)
    assert percentile([1, 2, 3, 4], 0.95) == pytest.approx(3.85)


def test_percentile_bounds():
    assert percentile([1, 2, 3, 4, 5], 0.0) == 1
    assert percentile([1, 2, 3, 4, 5], 1.0) == 5


def test_percentile_bad_q():
    with pytest.raises(ValueError):
        percentile([1, 2], 1.5)


def test_mean():
    assert mean([]) is None
    assert mean([1, 3]) == 2.0
