"""Unit tests for Phase 3 message contracts and id helpers."""

import json
import uuid
from datetime import UTC, datetime

import pytest

from pizza.common.constants import MAX_ORDER_QTY, MAX_ORDERS_CAP
from pizza.common.dedupe import Deduper
from pizza.common.envelope import (
    InvalidEnvelope,
    JobCommand,
    OpDispatched,
    OrderLifecycle,
    OrderSubmitted,
    RunParams,
    RunStarted,
    SmokeFact,
    encode,
    parse_message,
)
from pizza.common.ids import job_event_id, make_order_id

DUE = datetime(2026, 10, 16, 10, 0, 0, 123000, tzinfo=UTC)
ORDER = {"order_id": "L-0001", "product": "veggie", "qty": 2, "due_ts": DUE, "created_ts": DUE}


def _raw(msg) -> dict:
    return json.loads(encode(msg))


@pytest.mark.parametrize("field", ["order_id", "product", "qty", "due_ts", "created_ts"])
def test_order_submitted_missing_field_rejected(field):
    raw = _raw(OrderSubmitted.make(run_id="r", machine_id="generator", **ORDER))
    del raw[field]
    with pytest.raises(InvalidEnvelope):
        parse_message(json.dumps(raw).encode())


def test_order_submitted_round_trip_and_ts_format():
    msg = OrderSubmitted.make(run_id="r", machine_id="generator", **ORDER)
    assert parse_message(encode(msg)) == msg
    assert _raw(msg)["due_ts"] == "2026-10-16T10:00:00.123Z"


@pytest.mark.parametrize(
    "override",
    [
        {"qty": 0},
        {"qty": MAX_ORDER_QTY + 1},
        {"product": "Veggie"},
        {"product": "a/b"},
        {"product": "a.b"},
        {"product": ""},
    ],
)
def test_order_submitted_bounds_rejected(override):
    raw = _raw(OrderSubmitted.make(run_id="r", machine_id="generator", **ORDER))
    raw.update(override)
    with pytest.raises(InvalidEnvelope):
        parse_message(json.dumps(raw).encode())


def test_recipe_only_job_command_accepted():
    msg = JobCommand.make(
        run_id="r", machine_id="bake", order_id="o", step="bake", qty=3, recipe="bake-x"
    )
    assert parse_message(encode(msg)).unit_time_s is None


def test_job_command_needs_recipe_or_time():
    with pytest.raises(ValueError):
        JobCommand.make(run_id="r", machine_id="bake", order_id="o", step="bake", qty=1)


def test_job_command_unit_time_only_and_bound_and_slug():
    JobCommand.make(run_id="r", machine_id="b", order_id="o", step="bake", qty=1, unit_time_s=5)
    with pytest.raises(ValueError):
        JobCommand.make(
            run_id="r", machine_id="b", order_id="o", step="bake", qty=1000, unit_time_s=4.0
        )
    with pytest.raises(ValueError):
        JobCommand.make(
            run_id="r", machine_id="b", order_id="o", step="bake", qty=1, recipe="../x"
        )


def test_run_started_params_optional_and_round_trip():
    assert parse_message(encode(RunStarted.make(run_id="r", machine_id="x"))).params is None
    msg = RunStarted.make(
        run_id="r", machine_id="x", params=RunParams(seed=7, k=1.5, k_max=3.0, max_orders=10)
    )
    assert parse_message(encode(msg)) == msg


@pytest.mark.parametrize(
    "kwargs",
    [
        {"weights": {"veggie": -1.0}},
        {"weights": {"veggie": 0.0}},
        {"arrival_rate": 0},
        {"arrival_rate": 101},
        {"k": 0},
        {"k": 2.0, "k_max": 1.0},
        {"max_orders": 0},
        {"max_orders": MAX_ORDERS_CAP + 1},
        {"weights": {"Bad Key": 1.0}},
    ],
)
def test_run_params_validation(kwargs):
    with pytest.raises(ValueError):
        RunParams(**kwargs)


def test_order_lifecycle_state_restricted():
    for st in ("created", "released", "in_process", "completed"):
        msg = OrderLifecycle.make(run_id="r", machine_id="scheduler", state=st, **ORDER)
        assert parse_message(encode(msg)) == msg
    with pytest.raises(ValueError):
        OrderLifecycle.make(run_id="r", machine_id="scheduler", state="bogus", **ORDER)


def test_op_dispatched_dedupable():
    msg = OpDispatched.make(
        run_id="r",
        machine_id="bake",
        order_id="o",
        step="bake",
        visit=1,
        qty=1,
        command_event_id=uuid.uuid4(),
        ready_ts=DUE,
    )
    assert parse_message(encode(msg)) == msg
    d = Deduper()
    assert d.first_time_op(msg) is True
    assert d.first_time_op(msg) is False


def test_make_ts_override():
    assert SmokeFact.make(run_id="r", machine_id="s", ts=DUE).ts == DUE
    assert SmokeFact.make(run_id="r", machine_id="s").ts != DUE


def test_job_event_id_determinism():
    base = job_event_id("r", "o", "bake", 1)
    assert base == job_event_id("r", "o", "bake", 1)
    assert base != job_event_id("r2", "o", "bake", 1)
    assert base != job_event_id("r", "o2", "bake", 1)
    assert base != job_event_id("r", "o", "slice", 1)
    assert base != job_event_id("r", "o", "bake", 2)


def test_make_order_id_stable():
    assert make_order_id(42, 7) == "L-0007"
