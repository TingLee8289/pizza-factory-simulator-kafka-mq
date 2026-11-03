"""Unit tests for station breakdowns, op.failed, heartbeat and shutdown (no brokers)."""

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from pizza.common.envelope import (
    JobCommand,
    MachineBreak,
    MachineRepair,
    RunParams,
    RunReset,
    RunStarted,
    encode,
)
from pizza.common.failure_model import FailureSchedule, plan_schedule
from pizza.station import service
from pizza.station.service import Station
from tests.unit.test_station_process import (
    OrderProducer,
    _channel,
    _connection,
    _msg,
    _settings,
)

TOL = 0.15


def _station(calls: list[str], name: str = "bake") -> tuple[Station, OrderProducer]:
    producer = OrderProducer(calls)
    return Station(name, producer, _settings(), channel=_channel()), producer


def _wjob(work_s: float = 0.4, **extra) -> JobCommand:
    return JobCommand.make(
        run_id="run-1",
        machine_id="intake",
        order_id="o1",
        step="bake",
        qty=1,
        unit_time_s=work_s,
        product="pizza",
        **extra,
    )


def _labels(producer: OrderProducer) -> list[str]:
    out = []
    for ev in producer.events:
        out.append(ev["status"] if ev["type"] == "station.status" else ev["type"])
    return out


class _StubSchedule:
    """Failure schedule stub: one trip after ``budget`` busy seconds."""

    def __init__(self, budget: float, repair: float) -> None:
        self._budget: float | None = budget
        self._repair = repair
        self.consumed = 0.0

    def budget_left(self) -> float | None:
        return self._budget

    def consume(self, busy_s: float) -> None:
        self.consumed += busy_s
        if self._budget is not None:
            self._budget = max(0.0, self._budget - busy_s)

    def trip(self) -> float:
        self._budget = None
        return self._repair


def _failed(producer: OrderProducer) -> list[dict]:
    return [e for e in producer.events if e["type"] == "op.failed"]


def _states(producer: OrderProducer) -> list[str]:
    return [x for x in _labels(producer) if x in ("busy", "down", "idle")]


async def test_manual_break_mid_job_fails_after_repair() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    message = _msg(encode(_wjob(0.4, visit=2)), calls)
    task = asyncio.create_task(station.handle_job(message))
    await asyncio.sleep(0.1)
    await station.begin_outage("run-1", "manual", 60.0)
    await asyncio.sleep(0.2)
    message.ack.assert_not_awaited()
    await station.end_outage("run-1")
    await task
    failed = _failed(producer)
    assert len(failed) == 1
    assert failed[0]["cause"] == "breakdown"
    assert failed[0]["reason"] == "machine_breakdown"
    assert failed[0]["visit"] == 2
    assert "op.completed" not in _labels(producer)
    assert _states(producer) == ["busy", "down", "idle"]
    down = next(e for e in producer.events if e.get("status") == "down")
    assert down["cause"] == "manual"
    message.ack.assert_awaited_once()
    message.nack.assert_not_awaited()
    message.reject.assert_not_awaited()


async def test_job_held_while_down() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    await station.begin_outage("run-1", "manual", 60.0)
    message = _msg(encode(_wjob(0.05)), calls)
    task = asyncio.create_task(station.handle_job(message))
    await asyncio.sleep(0.2)
    assert _labels(producer) == ["down"]
    message.ack.assert_not_awaited()
    await station.end_outage("run-1")
    await task
    labels = _labels(producer)
    assert labels.index("op.started") > labels.index("idle")
    assert "op.completed" in labels
    message.ack.assert_awaited_once()


async def test_random_trip_fails_job_after_auto_repair() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    stub = _StubSchedule(0.1, 0.2)
    station.failure = stub  # type: ignore[assignment]
    message = _msg(encode(_wjob(0.5)), calls)
    start = time.monotonic()
    await station.handle_job(message)
    elapsed = time.monotonic() - start
    assert elapsed == pytest.approx(0.3, abs=TOL)
    down = next(e for e in producer.events if e.get("status") == "down")
    assert down["cause"] == "random"
    assert down["repair_in_s"] == pytest.approx(0.2)
    assert len(_failed(producer)) == 1
    assert _failed(producer)[0]["cause"] == "breakdown"
    assert "op.completed" not in _labels(producer)
    assert _states(producer) == ["busy", "down", "idle"]
    message.ack.assert_awaited_once()


async def test_repair_early_single_failure() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    message = _msg(encode(_wjob(0.4)), calls)
    task = asyncio.create_task(station.handle_job(message))
    await asyncio.sleep(0.05)
    await station.begin_outage("run-1", "manual", 0.3)
    await station.begin_outage("run-1", "manual", 5.0)  # no-op while down
    await station.end_outage("run-1")  # early repair; auto-repair cancelled
    await task
    await asyncio.sleep(0.4)
    assert len(_failed(producer)) == 1
    assert "op.completed" not in _labels(producer)
    message.ack.assert_awaited_once()


async def test_break_while_idle_not_judged() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    await station.begin_outage("run-1", "manual", 60.0)
    await station.end_outage("run-1")
    assert _failed(producer) == []
    message = _msg(encode(_wjob(0.05)), calls)
    await station.handle_job(message)
    assert "op.completed" in _labels(producer)
    assert _failed(producer) == []


async def test_trip_at_work_end_completes() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    # budget == work: strict "<" gives no trip, so the job completes (D-17).
    station.failure = _StubSchedule(0.1, 0.05)  # type: ignore[assignment]
    message = _msg(encode(_wjob(0.1)), calls)
    await station.handle_job(message)
    labels = _labels(producer)
    assert labels.count("op.completed") == 1
    assert "op.failed" not in labels
    # The budget is now exhausted (0.0): the NEXT job trips at zero progress
    # and is judged failed (RESEARCH step 4).
    assert station.failure.budget_left() == 0.0
    message2 = _msg(encode(_wjob(0.1)), calls)
    await station.handle_job(message2)
    assert len(_failed(producer)) == 1
    assert labels.count("op.completed") == 1


async def test_fail_flag_job_emits_single_quality_failure() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    station.failure = _StubSchedule(0.0, 0.05)  # type: ignore[assignment]
    message = _msg(encode(_wjob(0.3, poison=True)), calls)
    await station.handle_job(message)
    failed = _failed(producer)
    assert len(failed) == 1
    assert failed[0].get("cause") is None
    assert failed[0]["reason"] == "injected"
    assert "down" not in _states(producer)
    message.ack.assert_awaited_once()


async def test_shutdown_during_outage_nacks_requeue_no_events(monkeypatch) -> None:
    _patch_run(monkeypatch)
    calls: list[str] = []
    producer = OrderProducer(calls)
    station = Station("bake", producer, _cfg(heartbeat_interval_s=0), channel=_channel())
    connection, _ = _connection()
    message = _msg(encode(_wjob(0.5)), calls)
    job = asyncio.create_task(station.handle_job(message))
    await asyncio.sleep(0.1)
    await station.begin_outage("run-1", "manual", 60.0)
    runner = asyncio.create_task(station.run(connection))
    await asyncio.sleep(0.05)
    station.stop()
    await asyncio.wait_for(runner, timeout=3)
    await job
    labels = _labels(producer)
    assert "op.failed" not in labels
    assert "op.completed" not in labels
    message.nack.assert_awaited_once_with(requeue=True)
    message.ack.assert_not_awaited()


async def test_run_reset_during_doomed_outage_acks_no_events() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    message = _msg(encode(_wjob(5.0)), calls)
    task = asyncio.create_task(station.handle_job(message))
    await asyncio.sleep(0.05)
    await station.begin_outage("run-1", "manual", 60.0)
    await station.handle_control(_ctl(RunReset, calls))
    await task
    labels = _labels(producer)
    assert "op.failed" not in labels
    assert "op.completed" not in labels
    message.ack.assert_awaited_once()
    message.nack.assert_not_awaited()


async def test_manual_break_does_not_consume_seeded_streams() -> None:
    calls: list[str] = []
    a, _ = _station(calls)
    b, _ = _station(calls)
    a.failure = FailureSchedule(9, "bake", 100.0, 0.1)
    b.failure = FailureSchedule(9, "bake", 100.0, 0.1)
    await a.begin_outage("run-1", "manual", 0.05)
    await a.end_outage("run-1")
    assert a.failure.budget_left() == b.failure.budget_left()
    assert a.failure.trip() == b.failure.trip()
    assert plan_schedule(9, "bake", 100.0, 0.1, 3) == plan_schedule(9, "bake", 100.0, 0.1, 3)


async def test_poison_emits_op_failed_then_acks() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    message = _msg(encode(_wjob(0.01, poison=True)), calls)
    await station.handle_job(message)
    assert calls == [
        "produce:busy",
        "produce:op.started",
        "produce:op.failed",
        "produce:idle",
        "ack",
    ]
    failed = producer.events[2]
    assert failed["reason"] == "injected"
    message.nack.assert_not_awaited()
    message.reject.assert_not_awaited()


async def test_fail_times_passes_on_later_visit() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    message = _msg(encode(_wjob(0.01, fail_times=1, visit=2)), calls)
    await station.handle_job(message)
    labels = _labels(producer)
    assert "op.completed" in labels
    assert "op.failed" not in labels


async def test_delivery_limit_header_reports_failed_without_work() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    headers = {"x-death": [{"reason": "delivery_limit", "queue": "station.bake.jobs"}]}
    message = _msg(encode(_wjob(5.0)), calls, headers=headers)
    await station.handle_job(message)
    assert calls == ["produce:op.failed", "ack"]
    assert producer.events[0]["reason"] == "delivery_limit"


async def test_emit_failure_on_op_failed_nacks() -> None:
    calls: list[str] = []
    producer = OrderProducer(calls)
    station = Station("bake", producer, _settings(), channel=_channel())
    real = producer.produce

    def flaky(topic, value=None, key=None, on_delivery=None):
        if b"op.failed" in value:
            producer.fail = True
        return real(topic, value=value, key=key, on_delivery=on_delivery)

    producer.produce = flaky  # type: ignore[method-assign]
    message = _msg(encode(_wjob(0.01, poison=True)), calls)
    await station.handle_job(message)
    message.nack.assert_awaited_once_with(requeue=True)
    message.ack.assert_not_awaited()


def _fail_idle_after_op_failed(producer: OrderProducer, station: Station | None = None) -> None:
    """Make the idle status produce fail once op.failed has gone out.

    With ``station`` the idle from end_outage is treated as not sent, forcing the
    breakdown branch to emit its own idle.
    """
    real = producer.produce
    seen = {"failed": False}

    def flaky(topic, value=None, key=None, on_delivery=None):
        producer.fail = seen["failed"] and b'"idle"' in value
        if b"op.failed" in value:
            seen["failed"] = True
            if station is not None:
                station._idle_sent = False
        return real(topic, value=value, key=key, on_delivery=on_delivery)

    producer.produce = flaky  # type: ignore[method-assign]


async def test_idle_emit_failure_after_op_failed_still_acks() -> None:
    """WR-03: op.failed released the credit, so the failed command must not be redelivered."""
    calls: list[str] = []
    station, producer = _station(calls)
    _fail_idle_after_op_failed(producer)
    message = _msg(encode(_wjob(0.01, poison=True)), calls)
    await station.handle_job(message)
    assert len(_failed(producer)) == 1
    message.ack.assert_awaited_once()
    message.nack.assert_not_awaited()


async def test_idle_emit_failure_after_breakdown_failed_still_acks() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    station.failure = _StubSchedule(0.05, 0.05)  # type: ignore[assignment]
    _fail_idle_after_op_failed(producer, station)
    message = _msg(encode(_wjob(0.3)), calls)
    await station.handle_job(message)
    assert len(_failed(producer)) == 1
    assert _failed(producer)[0]["cause"] == "breakdown"
    assert calls.count("produce:idle") == 2  # end_outage idle + the failing best-effort idle
    message.ack.assert_awaited_once()
    message.nack.assert_not_awaited()


def _ctl(msg_cls, calls, **kw):
    return _msg(encode(msg_cls.make(run_id="run-1", machine_id="scheduler", **kw)), calls)


async def test_reset_during_outage_clears_state_and_emits_idle() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    message = _msg(encode(_wjob(5.0)), calls)
    task = asyncio.create_task(station.handle_job(message))
    await asyncio.sleep(0.05)
    await station.begin_outage("run-1", "manual", 60.0)
    repair = station._repair_task
    assert repair is not None
    await station.handle_control(_ctl(RunReset, calls))
    await task
    await asyncio.sleep(0)
    assert station._down is False
    assert repair.cancelled() or repair.done()
    assert not station.gate.is_paused
    assert _labels(producer)[-1] == "idle"
    assert "op.completed" not in _labels(producer)


async def test_manual_break_uses_manual_stream_and_auto_repairs() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    station.failure = FailureSchedule(7, "bake", 100.0, 0.1)
    expected = FailureSchedule(7, "bake", 100.0, 0.1).manual_repair_s()
    before = plan_schedule(7, "bake", 100.0, 0.1, 3)
    await station.handle_control(_ctl(MachineBreak, calls))
    down = producer.events[0]
    assert down["status"] == "down"
    assert down["cause"] == "manual"
    assert down["repair_in_s"] == pytest.approx(expected)
    await asyncio.sleep(expected + 0.15)
    assert _labels(producer) == ["down", "idle"]
    assert station.failure.budget_left() == FailureSchedule(7, "bake", 100.0, 0.1).budget_left()
    assert plan_schedule(7, "bake", 100.0, 0.1, 3) == before


async def test_break_duration_override_and_double_break_noop() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    await station.handle_control(_ctl(MachineBreak, calls, duration_s=0.2))
    second = _ctl(MachineBreak, calls, duration_s=5.0)
    await station.handle_control(second)
    second.ack.assert_awaited_once()
    assert _labels(producer) == ["down"]
    assert producer.events[0]["repair_in_s"] == 0.2
    await asyncio.sleep(0.35)
    assert _labels(producer) == ["down", "idle"]


async def test_repair_ends_outage_early_and_up_repair_ignored() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    up = _ctl(MachineRepair, calls)
    await station.handle_control(up)
    up.ack.assert_awaited_once()
    assert producer.events == []
    await station.handle_control(_ctl(MachineBreak, calls, duration_s=30))
    repair_task = station._repair_task
    await station.handle_control(_ctl(MachineRepair, calls))
    await asyncio.sleep(0)
    assert _labels(producer) == ["down", "idle"]
    assert repair_task is not None
    assert repair_task.cancelled()
    assert not station.gate.is_paused


async def test_repair_while_job_active_emits_busy() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    message = _msg(encode(_wjob(0.3)), calls)
    task = asyncio.create_task(station.handle_job(message))
    await asyncio.sleep(0.05)
    await station.handle_control(_ctl(MachineBreak, calls, duration_s=30))
    await station.handle_control(_ctl(MachineRepair, calls))
    await task
    # 06.1 D-01: the job is doomed, so no spurious busy after repair.
    assert _states(producer) == ["busy", "down", "idle"]
    assert len(_failed(producer)) == 1


def _cfg(**kw) -> SimpleNamespace:
    base = {"nack_delay_s": 0, "kafka_bootstrap": "x:9092", "mtbf_s": None, "mttr_s": 20.0}
    base.update(kw)
    return SimpleNamespace(**base)


async def test_run_started_configures_failure_schedule() -> None:
    calls: list[str] = []
    producer = OrderProducer(calls)
    station = Station("bake", producer, _cfg(mtbf_s=1.0), channel=_channel())
    started = RunStarted.make(
        run_id="run-1",
        machine_id="scheduler",
        params=RunParams(seed=11, random_breakdowns=True),
    )
    await station.handle_control(_msg(encode(started), calls))
    assert station.failure.enabled
    assert station.failure.budget_left() == FailureSchedule(11, "bake", 1.0, 20.0).budget_left()
    off = RunStarted.make(
        run_id="run-2", machine_id="scheduler", params=RunParams(random_breakdowns=False)
    )
    await station.handle_control(_msg(encode(off), calls))
    assert not station.failure.enabled
    station.settings = _cfg(mtbf_s=None)
    on = RunStarted.make(
        run_id="run-3", machine_id="scheduler", params=RunParams(random_breakdowns=True)
    )
    await station.handle_control(_msg(encode(on), calls))
    assert not station.failure.enabled


async def test_run_started_resets_outage() -> None:
    calls: list[str] = []
    station, _ = _station(calls)
    await station.begin_outage("run-1", "manual", 30.0)
    await station.handle_control(_ctl(RunStarted, calls))
    assert station._down is False
    assert not station.gate.is_paused


async def test_break_on_restarted_station_uses_command_run_id() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    assert station.current_run_id is None
    msg = _msg(
        encode(MachineBreak.make(run_id="run-9", machine_id="scheduler", duration_s=0.1)), calls
    )
    await station.handle_control(msg)
    assert producer.events[0]["run_id"] == "run-9"
    await station.end_outage("run-9")


async def test_heartbeat_emits_status_and_unknown_run_id() -> None:
    calls: list[str] = []
    producer = OrderProducer(calls)
    station = Station("bake", producer, _cfg(), channel=_channel())
    task = asyncio.create_task(station._heartbeat_loop(0.05))
    await asyncio.sleep(0.18)
    station._down = True
    station.current_run_id = "run-1"
    await asyncio.sleep(0.1)
    task.cancel()
    beats = [e for e in producer.events if e["type"] == "station.heartbeat"]
    assert len(beats) >= 3
    assert beats[0]["run_id"] == "unknown"
    assert beats[0]["status"] == "idle"
    assert beats[-1]["run_id"] == "run-1"
    assert beats[-1]["status"] == "down"


async def test_heartbeat_swallows_emit_failure() -> None:
    calls: list[str] = []
    station, _ = _station(calls)
    station._emit = AsyncMock(side_effect=RuntimeError("kafka down"))  # type: ignore[method-assign]
    task = asyncio.create_task(station._heartbeat_loop(0.02))
    await asyncio.sleep(0.15)
    assert not task.done()
    assert station._emit.await_count >= 2
    task.cancel()


def _patch_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(service, "verify_rabbit", AsyncMock())
    monkeypatch.setattr(service, "verify_kafka", MagicMock())
    monkeypatch.setattr(service, "AdminClient", MagicMock())


async def test_heartbeat_disabled_starts_no_task(monkeypatch) -> None:
    _patch_run(monkeypatch)
    calls: list[str] = []
    producer = OrderProducer(calls)
    station = Station("bake", producer, _cfg(heartbeat_interval_s=0), channel=_channel())
    connection, _ = _connection()
    station.stop()
    await station.run(connection)
    assert station._heartbeat_task is None


async def test_sigterm_while_down_lets_held_job_finish(monkeypatch) -> None:
    _patch_run(monkeypatch)
    calls: list[str] = []
    producer = OrderProducer(calls)
    station = Station("bake", producer, _cfg(heartbeat_interval_s=0.05), channel=_channel())
    connection, _ = _connection()
    await station.begin_outage("run-1", "manual", 60.0)
    message = _msg(encode(_wjob(0.05)), calls)
    job = asyncio.create_task(station.handle_job(message))
    await asyncio.sleep(0.05)
    runner = asyncio.create_task(station.run(connection))
    await asyncio.sleep(0.1)
    assert station._heartbeat_task is not None
    heartbeat = station._heartbeat_task
    station.stop()
    await asyncio.wait_for(runner, timeout=3)
    await job
    assert heartbeat.cancelled()
    assert "op.completed" in _labels(producer)
    assert not station._down
    message.ack.assert_awaited_once()
