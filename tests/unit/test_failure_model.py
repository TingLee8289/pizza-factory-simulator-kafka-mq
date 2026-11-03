"""Unit tests for the pure seeded failure model (FAIL-01)."""

from pizza.common.failure_model import FLOOR_S, FailureSchedule, plan_schedule


def test_same_seed_identical():
    a = plan_schedule(7, "bake", 300, 20, 50)
    b = plan_schedule(7, "bake", 300, 20, 50)
    assert len(a) == 50
    assert a == b


def test_manual_repairs_do_not_perturb():
    ref = plan_schedule(7, "bake", 300, 20, 20)
    s = FailureSchedule(7, "bake", 300, 20)
    got = []
    for _ in range(20):
        for _ in range(10):
            s.manual_repair_s()
        b = s.budget_left()
        s.consume(b)
        got.append((b, s.trip()))
    assert got == ref


def test_streams_differ_by_station_and_seed():
    base = plan_schedule(7, "bake", 300, 20, 10)
    assert base != plan_schedule(7, "slice", 300, 20, 10)
    assert base != plan_schedule(8, "bake", 300, 20, 10)


def test_disabled():
    s = FailureSchedule(7, "bake", None, 20)
    assert s.enabled is False
    assert s.budget_left() is None
    s.consume(5)
    assert s.budget_left() is None
    assert s.manual_repair_s() >= FLOOR_S
    assert plan_schedule(7, "bake", None, 20, 5) == []


def test_budget_consume_and_trip():
    s = FailureSchedule(7, "bake", 300, 20)
    b = s.budget_left()
    s.consume(b / 2)
    assert abs(s.budget_left() - b / 2) < 1e-9
    s.consume(b * 10)
    assert s.budget_left() == 0.0
    assert s.trip() >= FLOOR_S
    fresh = s.budget_left()
    assert fresh is not None and fresh > 0


def test_floor_and_mean():
    s = FailureSchedule(3, "bake", 10, 4)
    tbf, ttr = [], []
    for _ in range(5000):
        b = s.budget_left()
        s.consume(b)
        tbf.append(b)
        ttr.append(s.trip())
    assert min(tbf) >= FLOOR_S and min(ttr) >= FLOOR_S
    assert abs(sum(tbf) / 5000 - 10) < 1.0
    assert abs(sum(ttr) / 5000 - 4) < 0.4
