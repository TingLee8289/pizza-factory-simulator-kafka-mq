"""Unit tests for pure station helpers."""

import pytest

from pizza.common.envelope import JobCommand
from pizza.station.logic import UnknownRecipe, attempt_from, is_for_station, work_seconds


def _job(step="bake", qty=3, t=2.0, recipe=None):
    return JobCommand.make(
        run_id="r",
        machine_id=step,
        order_id="o1",
        step=step,
        qty=qty,
        unit_time_s=t,
        recipe=recipe,
    )


def test_work_seconds():
    assert work_seconds(_job(qty=3, t=2.0)) == 6.0


def test_work_seconds_from_recipe():
    assert work_seconds(_job(qty=3, t=None, recipe="veggie"), {"veggie": 14.0}) == 42.0


def test_override_wins_over_recipe():
    assert work_seconds(_job(qty=2, t=1.0, recipe="veggie"), {"veggie": 14.0}) == 2.0


def test_unknown_recipe_raises():
    with pytest.raises(UnknownRecipe):
        work_seconds(_job(t=None, recipe="nope"), {"veggie": 14.0})
    with pytest.raises(UnknownRecipe):
        work_seconds(_job(t=None, recipe="veggie"), None)


def test_resolved_total_bound():
    with pytest.raises(ValueError):
        work_seconds(_job(qty=2, t=None, recipe="veggie"), {"veggie": 3000.0})


@pytest.mark.parametrize(
    "redelivered,headers,expected",
    [
        (False, {}, 1),
        (False, None, 1),
        (True, {"x-delivery-count": 2}, 3),
        (True, {}, 2),
        (True, None, 2),
        (True, {"x-delivery-count": "1"}, 2),
        (True, {"x-delivery-count": b"1"}, 2),
        (True, {"x-delivery-count": "garbage"}, 2),
    ],
)
def test_attempt_from(redelivered, headers, expected):
    assert attempt_from(redelivered, headers) == expected


def test_is_for_station():
    assert is_for_station(_job("bake"), "bake")
    assert not is_for_station(_job("bake"), "slice")


def _fjob(**extra):
    return JobCommand.make(
        run_id="r", machine_id="bake", order_id="o1", step="bake", qty=1, unit_time_s=1.0, **extra
    )


def test_should_fail():
    from pizza.station.logic import should_fail

    assert should_fail(_fjob(poison=True))
    assert not should_fail(_fjob())
    assert should_fail(_fjob(fail_times=2, visit=1))
    assert should_fail(_fjob(fail_times=2, visit=2))
    assert not should_fail(_fjob(fail_times=2, visit=3))


def test_delivery_limit_exceeded():
    from pizza.station.logic import delivery_limit_exceeded

    q = "station.bake.jobs"
    hit = {"x-death": [{"reason": "delivery_limit", "queue": q}]}
    assert delivery_limit_exceeded(hit, q)
    assert delivery_limit_exceeded(
        {"x-death": [{"reason": b"delivery_limit", "queue": q.encode()}]}, q
    )
    assert not delivery_limit_exceeded(None, q)
    assert not delivery_limit_exceeded({}, q)
    assert not delivery_limit_exceeded({"x-death": []}, q)
    assert not delivery_limit_exceeded({"x-death": [{"reason": "rejected", "queue": q}]}, q)
    assert not delivery_limit_exceeded({"x-death": [{"reason": "delivery_limit", "queue": "x"}]}, q)
    assert not delivery_limit_exceeded({"x-death": "junk"}, q)
    assert not delivery_limit_exceeded({"x-death": [1, None, {"reason": 3}]}, q)
