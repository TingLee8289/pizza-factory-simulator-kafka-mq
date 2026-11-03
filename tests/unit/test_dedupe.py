"""Unit tests for the pure Deduper (D-10/D-11)."""

import uuid

from pizza.common.dedupe import Deduper
from pizza.common.envelope import OpCompleted


def test_same_event_id_is_duplicate():
    d, eid = Deduper(), uuid.uuid4()
    assert d.first_time(eid, "o1", "bake", 1) is True
    assert d.first_time(eid, "o1", "bake", 1) is False


def test_same_op_new_event_id_is_duplicate():
    d = Deduper()
    assert d.first_time(uuid.uuid4(), "o1", "bake", 1)
    assert not d.first_time(uuid.uuid4(), "o1", "bake", 1)


def test_rework_visit_is_first_time():
    d = Deduper()
    assert d.first_time(uuid.uuid4(), "o1", "bake", 1)
    assert d.first_time(uuid.uuid4(), "o1", "bake", 2)


def test_different_order_or_step_is_first_time():
    d = Deduper()
    assert d.first_time(uuid.uuid4(), "o1", "bake", 1)
    assert d.first_time(uuid.uuid4(), "o2", "bake", 1)
    assert d.first_time(uuid.uuid4(), "o1", "slice", 1)


def test_first_time_op_with_op_completed():
    d = Deduper()
    op = OpCompleted.make(
        run_id="r", machine_id="bake", order_id="o1", step="bake", visit=1,
        attempt=1, qty=1, command_event_id=uuid.uuid4(), duration_s=1.0,
    )
    assert d.first_time_op(op) is True
    assert d.first_time_op(op) is False
