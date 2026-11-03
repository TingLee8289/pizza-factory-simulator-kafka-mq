import uuid

from pizza.common.dedupe import Deduper
from pizza.common.envelope import OpCompleted, OpStarted, StationStatus
from pizza.recorder.__main__ import record


def op(cmd_id, visit=1, run_id="r1", cls=OpCompleted):
    extra = {"duration_s": 1.0} if cls is OpCompleted else {"redelivered": False}
    return cls.make(
        run_id=run_id,
        machine_id="bake",
        order_id="o1",
        step="bake",
        visit=visit,
        attempt=1,
        qty=1,
        command_event_id=cmd_id,
        **extra,
    )


def test_record_sequence():
    a, b, c = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    res = record([op(a), op(a), op(b), op(c, visit=2)], Deduper())
    assert [r.status for r in res] == ["RECORDED", "DUPLICATE", "DUPLICATE", "RECORDED"]


def test_ignores_non_completed():
    started = op(uuid.uuid4(), cls=OpStarted)
    status = StationStatus.make(run_id="r1", machine_id="bake", status="idle")
    assert record([started, status], Deduper()) == []


def test_run_filter():
    res = record([op(uuid.uuid4(), run_id="r2"), op(uuid.uuid4())], Deduper(), run_id="r1")
    assert len(res) == 1 and res[0].run_id == "r1"
