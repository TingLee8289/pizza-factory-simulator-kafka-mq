"""Unit tests for Station ordering, failure, redelivery, control and stop (no brokers)."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from pizza.common.envelope import (
    JobCommand,
    RecipeChanged,
    RunPause,
    RunReset,
    RunResume,
    RunStarted,
    SetArrivalRate,
    SetRecipe,
    SetRule,
    encode,
    utc_now,
)
from pizza.station import service
from pizza.station.service import Station


class OrderProducer:
    """Stub producer recording a shared call-order list."""

    def __init__(self, calls: list[str], fail: bool = False) -> None:
        self.calls = calls
        self.fail = fail
        self.events: list[dict] = []
        self.keys: list[bytes] = []

    def produce(self, topic, value=None, key=None, on_delivery=None):
        body = json.loads(value)
        self.events.append(body)
        self.keys.append(key)
        label = body["type"]
        if label == "station.status":
            label = body["status"]
        self.calls.append(f"produce:{label}")
        if on_delivery is not None:
            on_delivery("boom" if self.fail else None, MagicMock())

    def flush(self, timeout=None):
        return 0


def _settings() -> SimpleNamespace:
    return SimpleNamespace(nack_delay_s=0, kafka_bootstrap="x:9092")


def _job(step: str = "bake", run_id: str = "run-1") -> JobCommand:
    return JobCommand.make(
        run_id=run_id,
        machine_id="intake",
        order_id="o1",
        step=step,
        qty=1,
        unit_time_s=0.001,
        product="pizza",
    )


def _msg(body: bytes, calls: list[str], redelivered: bool = False, headers=None) -> MagicMock:
    message = MagicMock()
    message.body = body
    message.redelivered = redelivered
    message.headers = headers or {}

    async def _ack():
        calls.append("ack")

    message.ack = AsyncMock(side_effect=_ack)
    message.nack = AsyncMock()
    message.reject = AsyncMock()
    return message


def _channel() -> MagicMock:
    dlx = MagicMock()
    dlx.publish = AsyncMock()
    channel = MagicMock()
    channel.get_exchange = AsyncMock(return_value=dlx)
    return channel


def _station(calls: list[str], fail: bool = False, name: str = "bake"):
    producer = OrderProducer(calls, fail=fail)
    return Station(name, producer, _settings(), channel=_channel()), producer


async def test_ack_after_flush() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    job = _job()
    message = _msg(encode(job), calls)
    await station.handle_job(message)
    assert calls == [
        "produce:busy",
        "produce:op.started",
        "produce:op.completed",
        "produce:idle",
        "ack",
    ]
    message.ack.assert_awaited_once()
    assert set(producer.keys) == {b"bake"}
    started = producer.events[1]
    assert started["command_event_id"] == str(job.event_id)
    assert started["attempt"] == 1
    assert started["redelivered"] is False
    assert station.current_run_id == "run-1"


async def test_redelivered_job_is_worked_with_attempt_2() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    message = _msg(encode(_job()), calls, redelivered=True, headers={"x-delivery-count": 1})
    await station.handle_job(message)
    started = producer.events[1]
    assert started["redelivered"] is True
    assert started["attempt"] == 2
    message.ack.assert_awaited_once()


async def test_produce_failure_nacks() -> None:
    calls: list[str] = []
    station, _ = _station(calls, fail=True)
    message = _msg(encode(_job()), calls)
    await station.handle_job(message)
    message.nack.assert_awaited_once_with(requeue=True)
    message.ack.assert_not_called()


@pytest.mark.parametrize(
    "body",
    [
        b"not json",
        encode(RunStarted.make(run_id="r", machine_id="x")),
        encode(_job(step="slice")),
    ],
)
async def test_invalid_inputs_dead_lettered(body: bytes) -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    message = _msg(body, calls)
    await station.handle_job(message)
    message.ack.assert_awaited_once()
    message.reject.assert_not_called()
    message.nack.assert_not_called()
    assert producer.events == []
    station.channel.get_exchange.assert_awaited()


def _recipe_job(recipe: str, qty: int = 3, unit_time_s: float | None = None) -> JobCommand:
    return JobCommand.make(
        run_id="run-1",
        machine_id="intake",
        order_id="o1",
        step="bake",
        qty=qty,
        unit_time_s=unit_time_s,
        recipe=recipe,
    )


async def test_unknown_recipe_dead_lettered_without_op_started() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    station._recipes = {"veggie": 14.0}
    message = _msg(encode(_recipe_job("margherita")), calls)
    await station.handle_job(message)
    message.ack.assert_awaited_once()
    message.nack.assert_not_called()
    message.reject.assert_not_called()
    assert producer.events == []
    station.channel.get_exchange.assert_awaited()


async def test_known_recipe_sleeps_recipe_time_times_qty(monkeypatch) -> None:
    calls: list[str] = []
    station, _ = _station(calls)
    station._recipes = {"veggie": 14.0}
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(station.gate, "sleep", fake_sleep)
    message = _msg(encode(_recipe_job("veggie", qty=3)), calls)
    await station.handle_job(message)
    assert sleeps == [42.0]
    message.ack.assert_awaited_once()


async def test_override_with_unknown_recipe_still_processes() -> None:
    calls: list[str] = []
    station, _ = _station(calls)
    station._recipes = {}
    message = _msg(encode(_recipe_job("margherita", qty=1, unit_time_s=0.001)), calls)
    await station.handle_job(message)
    assert "produce:op.started" in calls
    message.ack.assert_awaited_once()


async def test_control_run_started_sets_run_and_emits_idle() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    message = _msg(encode(RunStarted.make(run_id="run-9", machine_id="x")), calls)
    await station.handle_control(message)
    assert station.current_run_id == "run-9"
    assert calls == ["produce:idle", "ack"]


async def test_control_set_recipe_updates_only_own_table() -> None:
    calls: list[str] = []
    station, _ = _station(calls)
    station._recipes = {"margherita": 12.0, "pepperoni": 13.0, "veggie": 14.0}
    own = SetRecipe.make(
        run_id="r", machine_id="bake", station="bake", product="margherita", unit_time_s=20.0
    )
    await station.handle_control(_msg(encode(own), calls))
    assert station._recipes["margherita"] == 20.0
    assert calls[-1] == "ack"
    other = SetRecipe.make(
        run_id="r", machine_id="bake", station="spin", product="margherita", unit_time_s=99.0
    )
    await station.handle_control(_msg(encode(other), calls))
    assert station._recipes["margherita"] == 20.0
    assert calls[-1] == "ack"


async def test_control_set_recipe_applies_to_next_job(monkeypatch) -> None:
    calls: list[str] = []
    station, _ = _station(calls)
    station._recipes = {"margherita": 12.0}
    cmd = SetRecipe.make(
        run_id="r", machine_id="bake", station="bake", product="margherita", unit_time_s=20.0
    )
    await station.handle_control(_msg(encode(cmd), calls))
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(station.gate, "sleep", fake_sleep)
    message = _msg(encode(_recipe_job("margherita", qty=2)), calls)
    await station.handle_job(message)
    assert sleeps == [40.0]
    message.ack.assert_awaited_once()


def _recipe_cmd(unit_time_s: float, run_id: str = "run-1") -> SetRecipe:
    return SetRecipe.make(
        run_id=run_id,
        machine_id="bake",
        station="bake",
        product="margherita",
        unit_time_s=unit_time_s,
    )


async def test_control_set_recipe_emits_fact_during_active_run() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    station._recipes = {"margherita": 12.0, "pepperoni": 13.0, "veggie": 14.0}
    station.current_run_id = "run-1"
    cmd = _recipe_cmd(20.0)
    before = utc_now()
    await station.handle_control(_msg(encode(cmd), calls))
    assert calls == ["produce:recipe.changed", "ack"]
    assert producer.keys[-1] == b"bake"
    body = producer.events[-1]
    assert body["type"] == "recipe.changed"
    assert body["station"] == "bake"
    assert body["product"] == "margherita"
    assert body["old_unit_time_s"] == 12.0
    assert body["new_unit_time_s"] == 20.0
    assert body["run_id"] == "run-1"
    parsed_ts = RecipeChanged.model_validate(body).ts
    assert abs((parsed_ts - before).total_seconds()) < 1.0
    assert station._recipes["margherita"] == 20.0


async def test_control_set_recipe_pre_run_emits_nothing() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    station._recipes = {"margherita": 12.0, "pepperoni": 13.0, "veggie": 14.0}
    assert station.current_run_id is None
    await station.handle_control(_msg(encode(_recipe_cmd(20.0, run_id="config")), calls))
    assert calls == ["ack"]
    assert producer.events == []
    assert station._recipes["margherita"] == 20.0


async def test_control_set_recipe_same_value_is_noop() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    station._recipes = {"margherita": 12.0}
    station.current_run_id = "run-1"
    await station.handle_control(_msg(encode(_recipe_cmd(12.0)), calls))
    assert calls == ["ack"]
    assert producer.events == []
    assert station._recipes == {"margherita": 12.0}


async def test_control_set_recipe_produce_failure_nacks_then_redelivery_emits_once() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    station._recipes = {"margherita": 12.0}
    station.current_run_id = "run-1"
    real = producer.produce

    def flaky(topic, value=None, key=None, on_delivery=None):
        if b"recipe.changed" in value:
            producer.fail = True
        return real(topic, value=value, key=key, on_delivery=on_delivery)

    producer.produce = flaky  # type: ignore[method-assign]
    body = encode(_recipe_cmd(20.0))
    first = _msg(body, calls)
    await station.handle_control(first)
    first.nack.assert_awaited_once_with(requeue=True)
    first.ack.assert_not_awaited()
    assert station._recipes["margherita"] == 12.0

    producer.produce = real  # type: ignore[method-assign]
    producer.fail = False
    producer.events.clear()  # the stub records the failed attempt too; count only the redelivery
    calls.clear()
    second = _msg(body, calls, redelivered=True)
    await station.handle_control(second)
    assert calls == ["produce:recipe.changed", "ack"]
    changed = [e for e in producer.events if e["type"] == "recipe.changed"]
    assert len(changed) == 1
    assert station._recipes["margherita"] == 20.0


async def test_control_invalid_dead_lettered() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    message = _msg(encode(_job()), calls)
    await station.handle_control(message)
    message.ack.assert_awaited_once()
    assert producer.events == []
    args = station.channel.get_exchange.await_args
    assert args is not None


def _patch_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(service, "verify_rabbit", AsyncMock())
    monkeypatch.setattr(service, "verify_kafka", MagicMock())
    monkeypatch.setattr(service, "AdminClient", MagicMock())


def _connection() -> tuple[AsyncMock, dict]:
    queues: dict = {}

    def make_channel():
        channel = AsyncMock()
        queue = AsyncMock()
        queue.consume = AsyncMock(return_value="tag")
        channel.get_queue = AsyncMock(return_value=queue)
        queues.setdefault("channels", []).append(channel)
        return channel

    connection = AsyncMock()
    connection.channel = AsyncMock(side_effect=lambda: make_channel())
    return connection, queues


async def test_stop_without_run_id_emits_no_offline(monkeypatch) -> None:
    _patch_run(monkeypatch)
    calls: list[str] = []
    station, producer = _station(calls)
    connection, _ = _connection()
    station.stop()
    await station.run(connection)
    assert producer.events == []
    prefetch = connection.channel.await_count
    assert prefetch == 2


async def test_stop_with_run_id_emits_one_offline(monkeypatch) -> None:
    _patch_run(monkeypatch)
    calls: list[str] = []
    station, _ = _station(calls)
    station.current_run_id = "run-1"
    connection, _ = _connection()
    station.stop()
    await station.run(connection)
    assert calls == ["produce:offline"]


async def test_stop_waits_for_inflight_job_before_offline(monkeypatch) -> None:
    _patch_run(monkeypatch)
    calls: list[str] = []
    station, _ = _station(calls)
    connection, _ = _connection()
    job = JobCommand.make(
        run_id="run-1", machine_id="intake", order_id="o1", step="bake", qty=1, unit_time_s=0.2
    )
    message = _msg(encode(job), calls)
    task = asyncio.create_task(station.handle_job(message))
    await asyncio.sleep(0.05)  # job in flight
    station.stop()
    await station.run(connection)
    await task
    assert calls.index("ack") < calls.index("produce:offline")
    assert calls[-1] == "produce:offline"


async def test_jobs_consumer_uses_prefetch_1_and_manual_ack(monkeypatch) -> None:
    _patch_run(monkeypatch)
    calls: list[str] = []
    station, _ = _station(calls)
    connection, state = _connection()
    station.stop()
    await station.run(connection)
    jobs_channel = state["channels"][0]
    jobs_channel.set_qos.assert_awaited_once_with(prefetch_count=1)
    queue = jobs_channel.get_queue.return_value
    queue.consume.assert_awaited_once_with(station.handle_job, no_ack=False)


# -- Phase 5: pause / resume / reset ---------------------------------------
def _timed_job(seconds: float, run_id: str = "run-1") -> JobCommand:
    return JobCommand.make(
        run_id=run_id,
        machine_id="intake",
        order_id="o1",
        step="bake",
        qty=1,
        unit_time_s=seconds,
        product="pizza",
    )


async def _control(station: Station, cmd, calls: list[str]) -> MagicMock:
    message = _msg(encode(cmd), calls)
    await station.handle_control(message)
    return message


async def test_pause_keeps_remaining_time() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    message = _msg(encode(_timed_job(0.4)), calls)
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    task = asyncio.create_task(station.handle_job(message))
    await asyncio.sleep(0.1)
    await _control(station, RunPause.make(run_id="run-1", machine_id="x"), calls)
    await asyncio.sleep(0.5)
    assert "produce:op.completed" not in calls
    await _control(station, RunResume.make(run_id="run-1", machine_id="x"), calls)
    await task
    elapsed = loop.time() - t0
    assert elapsed >= 0.85
    assert [e["type"] for e in producer.events].count("op.completed") == 1
    message.ack.assert_awaited_once()


async def test_job_while_paused_waits_for_resume() -> None:
    calls: list[str] = []
    station, _ = _station(calls)
    await _control(station, RunPause.make(run_id="run-1", machine_id="x"), calls)
    message = _msg(encode(_timed_job(0.01)), calls)
    task = asyncio.create_task(station.handle_job(message))
    await asyncio.sleep(0.1)
    assert not [c for c in calls if c.startswith("produce:")]
    await _control(station, RunResume.make(run_id="run-1", machine_id="x"), calls)
    await task
    assert "produce:op.started" in calls
    message.ack.assert_awaited_once()


async def test_reset_aborts_inflight_job() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    message = _msg(encode(_timed_job(5.0)), calls)
    task = asyncio.create_task(station.handle_job(message))
    await asyncio.sleep(0.1)
    await _control(station, RunReset.make(run_id="run-1", machine_id="x"), calls)
    await asyncio.wait_for(task, timeout=1.0)
    types = [e["type"] for e in producer.events]
    assert "op.completed" not in types
    assert calls[-2:] == ["produce:idle", "ack"]
    message.ack.assert_awaited_once()
    assert not station.gate.is_paused
    assert station.current_run_id is None


async def test_reset_while_paused_releases_job() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    message = _msg(encode(_timed_job(5.0)), calls)
    task = asyncio.create_task(station.handle_job(message))
    await asyncio.sleep(0.05)
    await _control(station, RunPause.make(run_id="run-1", machine_id="x"), calls)
    await _control(station, RunReset.make(run_id="run-1", machine_id="x"), calls)
    await asyncio.wait_for(task, timeout=1.0)
    assert "op.completed" not in [e["type"] for e in producer.events]
    message.ack.assert_awaited_once()


async def test_stale_run_job_dropped_after_control() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    await _control(station, RunStarted.make(run_id="run-2", machine_id="x"), calls)
    producer.events.clear()
    message = _msg(encode(_timed_job(0.01, run_id="run-1")), calls)
    await station.handle_job(message)
    message.ack.assert_awaited_once()
    assert producer.events == []


async def test_stale_job_dropped_after_reset_until_new_run() -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    await _control(station, RunReset.make(run_id="run-1", machine_id="x"), calls)
    message = _msg(encode(_timed_job(0.01, run_id="run-1")), calls)
    await station.handle_job(message)
    message.ack.assert_awaited_once()
    assert producer.events == []


@pytest.mark.parametrize(
    "cmd",
    [
        RunPause.make(run_id="r", machine_id="x"),
        RunResume.make(run_id="r", machine_id="x"),
        RunReset.make(run_id="r", machine_id="x"),
        SetArrivalRate.make(run_id="r", machine_id="x", arrival_rate=0.5),
        SetRule.make(run_id="r", machine_id="x", rule="fifo"),
    ],
)
async def test_control_commands_acked_not_dead_lettered(cmd) -> None:
    calls: list[str] = []
    station, producer = _station(calls)
    message = _msg(encode(cmd), calls)
    await station.handle_control(message)
    message.ack.assert_awaited_once()
    station.channel.get_exchange.assert_not_called()


async def test_double_pause_and_stray_resume_harmless() -> None:
    calls: list[str] = []
    station, _ = _station(calls)
    await _control(station, RunResume.make(run_id="r", machine_id="x"), calls)
    await _control(station, RunPause.make(run_id="r", machine_id="x"), calls)
    await _control(station, RunPause.make(run_id="r", machine_id="x"), calls)
    assert station.gate.is_paused
    await _control(station, RunResume.make(run_id="r", machine_id="x"), calls)
    assert not station.gate.is_paused
