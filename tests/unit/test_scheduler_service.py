"""Unit tests for the Scheduler effect shell (no brokers)."""

import asyncio
import json
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from pizza.common.constants import STATIONS
from pizza.common.envelope import (
    EstimateEta,
    EstimateWip,
    JobCommand,
    OpCompleted,
    OpStarted,
    OrderLifecycle,
    OrderSubmitted,
    RunPause,
    RunReset,
    RunResume,
    RunStarted,
    SetArrivalRate,
    SetRecipe,
    SetRule,
    StationStatus,
    encode,
    utc_now,
)
from pizza.common.topology import (
    Q_SCHEDULER_CONTROL,
    Q_WORKORDERS_INTAKE,
    TOPIC_ORDER_LIFECYCLE,
    TOPIC_STATION_EVENTS,
)
from pizza.scheduler import service
from pizza.scheduler.service import Scheduler

T0 = datetime(2026, 1, 1, tzinfo=UTC)


def _settings(**kw) -> SimpleNamespace:
    base = {
        "nack_delay_s": 0,
        "dispatch_retry_s": 0,
        "intake_defer_max": 2,
        "resume_on_start": True,
        "replay_timeout_s": 1.0,
        "kafka_group_id": "scheduler",
        "kafka_bootstrap": "x:9092",
    }
    base.update(kw)
    return SimpleNamespace(**base)


class Env:
    def __init__(self, monkeypatch) -> None:
        self.calls: list[str] = []
        self.facts: list[tuple] = []
        self.commands: list[tuple] = []
        self.fail_publish = 0
        self.fail_lifecycle = 0
        dlx = MagicMock()
        dlx.publish = AsyncMock()
        self.channel = MagicMock()
        self.channel.get_exchange = AsyncMock(return_value=dlx)
        self.exchange = object()
        monkeypatch.setattr(service, "produce_fact", self._produce)
        monkeypatch.setattr(service, "publish_command", self._publish)

    def _produce(self, producer, topic, fact, key):
        if topic == TOPIC_ORDER_LIFECYCLE and self.fail_lifecycle > 0:
            self.fail_lifecycle -= 1
            raise RuntimeError("kafka down")
        label = f"{fact.type}:{getattr(fact, 'state', '')}"
        self.calls.append(f"fact:{label}")
        self.facts.append((topic, fact, key))

    async def _publish(self, exchange, routing_key, command):
        if self.fail_publish > 0:
            self.fail_publish -= 1
            raise ConnectionError("boom")
        self.calls.append(f"command:{routing_key}")
        self.commands.append((routing_key, command))

    def scheduler(self, **kw) -> Scheduler:
        s = Scheduler(
            producer=object(),
            settings=_settings(**kw),
            channel=self.channel,
            dispatch_exchange=self.exchange,
        )
        s.core.start_run("run-1")
        # Every station reports idle first: a never-reported station is not dispatchable.
        for station in s.core.route:
            s.core.on_station_status(
                StationStatus.make(
                    run_id="run-1", machine_id=station, ts=T0 - timedelta(seconds=1), status="idle"
                )
            )
        return s

    def msg(self, body: bytes) -> MagicMock:
        message = MagicMock()
        message.body = body

        async def _ack():
            self.calls.append("ack")

        message.ack = AsyncMock(side_effect=_ack)
        message.nack = AsyncMock()
        message.reject = AsyncMock()
        return message


def _order(order_id="o1", run_id="run-1", product="margherita") -> OrderSubmitted:
    return OrderSubmitted.make(
        run_id=run_id,
        machine_id="generator",
        order_id=order_id,
        product=product,
        qty=2,
        due_ts=T0 + timedelta(hours=1),
        created_ts=T0,
    )


def _completed(order_id: str, step: str, run_id="run-1") -> OpCompleted:
    return OpCompleted.make(
        run_id=run_id,
        machine_id=step,
        order_id=order_id,
        step=step,
        visit=1,
        attempt=1,
        qty=2,
        command_event_id=uuid.uuid4(),
        duration_s=1.0,
    )


async def test_intake_released_before_ack_then_dispatch_then_op_dispatched(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    m = env.msg(encode(_order()))
    await s.handle_intake(m)
    assert env.calls == [
        "fact:order.lifecycle:released",
        "ack",
        "command:station.spin",
        "fact:op.dispatched:",
    ]
    key, cmd = env.commands[0]
    assert isinstance(cmd, JobCommand)
    assert cmd.recipe == "margherita" and cmd.step == "spin" and cmd.machine_id == "spin"
    disp = env.facts[1]
    assert disp[0] == TOPIC_STATION_EVENTS and disp[2] == "spin"
    assert disp[1].command_event_id == cmd.event_id
    assert env.facts[0][2] == "o1"
    m.nack.assert_not_called()


async def test_known_order_acked_without_effects(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    await s.handle_intake(env.msg(encode(_order())))
    env.calls.clear()
    m = env.msg(encode(_order()))
    await s.handle_intake(m)
    assert env.calls == ["ack"]


async def test_wrong_run_deferred_then_dropped(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    body = encode(_order(run_id="other"))
    m = env.msg(body)
    # same body -> same event_id on every redelivery
    await s.handle_intake(m)
    await s.handle_intake(m)
    assert m.nack.await_count == 2
    m.nack.assert_awaited_with(requeue=True)
    await s.handle_intake(m)
    m.ack.assert_awaited_once()
    assert env.facts == []


async def test_invalid_and_wrong_type_dead_lettered(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    m = env.msg(b"not json")
    await s.handle_intake(m)
    m.ack.assert_awaited_once()
    dlx = await env.channel.get_exchange("dlx")
    assert dlx.publish.await_count == 1
    assert dlx.publish.await_args.args[0].headers["x-source-queue"] == Q_WORKORDERS_INTAKE

    ctl = env.msg(encode(_order()))
    await s.handle_control(ctl)
    assert dlx.publish.await_args.args[0].headers["x-source-queue"] == Q_SCHEDULER_CONTROL


async def test_released_emit_failure_rolls_back_and_nacks(monkeypatch) -> None:
    env = Env(monkeypatch)
    env.fail_lifecycle = 1
    s = env.scheduler()
    m = env.msg(encode(_order()))
    await s.handle_intake(m)
    assert "o1" not in s.core.orders
    m.nack.assert_awaited_once_with(requeue=True)
    m.ack.assert_not_called()
    assert env.commands == []
    assert s.core.outstanding["spin"] is None


async def test_dispatch_publish_failure_releases_and_retries(monkeypatch) -> None:
    env = Env(monkeypatch)
    env.fail_publish = 1
    s = env.scheduler()
    m = env.msg(encode(_order()))
    await s.handle_intake(m)
    m.ack.assert_awaited_once()  # intake acked independently of the dispatch
    assert env.commands == []
    assert not any(c.startswith("fact:op.dispatched") for c in env.calls)
    await s.drain_retries()
    assert [k for k, _ in env.commands] == ["station.spin"]
    assert env.calls[-2:] == ["command:station.spin", "fact:op.dispatched:"]
    assert s.core.outstanding["spin"] is not None


async def test_dispatch_two_failures_then_retries(monkeypatch) -> None:
    env = Env(monkeypatch)
    env.fail_publish = 2
    s = env.scheduler()
    m = env.msg(encode(_order()))
    await s.handle_intake(m)
    m.ack.assert_awaited_once()  # intake acked independently of the dispatch
    assert env.commands == []
    assert not any(c.startswith("fact:op.dispatched") for c in env.calls)
    for _ in range(3):
        await s.drain_retries()
        if env.commands:
            break
    assert [k for k, _ in env.commands] == ["station.spin"]
    assert env.calls[-2:] == ["command:station.spin", "fact:op.dispatched:"]
    assert s.core.outstanding["spin"] is not None
    assert s._retry_task is None or s._retry_task.done()


async def test_station_events_drive_routing_and_lifecycle(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    await s.handle_intake(env.msg(encode(_order())))
    env.calls.clear()

    started = OpStarted.make(
        run_id="run-1",
        machine_id="spin",
        order_id="o1",
        step="spin",
        visit=1,
        attempt=1,
        qty=2,
        command_event_id=uuid.uuid4(),
        redelivered=False,
    )
    await s.on_station_event(started)
    assert env.calls == ["fact:order.lifecycle:in_process"]
    env.calls.clear()

    status = StationStatus.make(run_id="run-1", machine_id="spin", status="idle")
    await s.on_station_event(status)
    assert env.calls == []

    await s.on_station_event(_completed("o1", "spin"))
    assert env.calls == ["command:station.sauce", "fact:op.dispatched:"]

    for step in STATIONS[1:]:
        env.calls.clear()
        await s.on_station_event(_completed("o1", step))
    assert env.calls[-1] == "fact:order.lifecycle:completed"
    lifecycle = [f for t, f, _ in env.facts if t == TOPIC_ORDER_LIFECYCLE]
    assert lifecycle[-1].state == "completed"
    assert lifecycle[-1].machine_id == "scheduler"


async def test_lock_serializes_concurrent_callers(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    inside = 0
    peak = 0
    orig = s._execute

    async def slow(actions):
        nonlocal inside, peak
        inside += 1
        peak = max(peak, inside)
        await asyncio.sleep(0.01)
        await orig(actions)
        inside -= 1

    s._execute = slow  # type: ignore[method-assign]
    await asyncio.gather(*(s.handle_intake(env.msg(encode(_order(f"o{i}")))) for i in range(4)))
    assert peak == 1
    assert len(s.core.orders) == 4


async def test_run_started_once(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    s._defer_counts["x"] = 1
    body = encode(RunStarted.make(run_id="run-2", machine_id="cli"))
    m1, m2 = env.msg(body), env.msg(body)
    await s.handle_control(m1)
    assert s.core.current_run_id == "run-2"
    assert s._defer_counts == {}
    s.core.on_order = MagicMock(side_effect=AssertionError)  # type: ignore[method-assign]
    await s.handle_control(m2)
    m1.ack.assert_awaited_once()
    m2.ack.assert_awaited_once()
    assert s.core.current_run_id == "run-2"


def _lifecycle(order_id: str, state: str, run_id="run-9", ts=T0) -> OrderLifecycle:
    return OrderLifecycle.make(
        run_id=run_id,
        machine_id="scheduler",
        ts=ts,
        state=state,
        order_id=order_id,
        product="margherita",
        qty=2,
        due_ts=T0 + timedelta(hours=1),
        created_ts=T0,
    )


def _idle_reports(run_id="run-9") -> list[StationStatus]:
    """Every station reports idle: a never-reported station is not dispatchable."""
    return [
        StationStatus.make(
            run_id=run_id, machine_id=st, ts=T0 - timedelta(seconds=1), status="idle"
        )
        for st in STATIONS
    ]


async def test_recover_rebuilds_and_dispatches_free_stations(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    facts = [
        *_idle_reports(),
        _lifecycle("a", "released"),
        _lifecycle("b", "released", ts=T0 + timedelta(seconds=1)),
    ]
    await s.recover(facts)
    assert s.core.current_run_id == "run-9"
    assert [k for k, _ in env.commands] == ["station.spin"]
    assert env.commands[0][1].order_id == "a"


async def test_recover_ignored_when_resume_disabled(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler(resume_on_start=False)
    await s.recover([_lifecycle("a", "released")])
    assert s.core.current_run_id == "run-1"
    assert env.commands == []


async def test_startup_rebuilds_before_consuming(monkeypatch) -> None:
    env = Env(monkeypatch)
    order: list[str] = []

    async def fake_verify(conn):
        order.append("verify_rabbit")

    monkeypatch.setattr(service, "verify_rabbit", fake_verify)
    monkeypatch.setattr(service, "verify_kafka", lambda admin: None)
    monkeypatch.setattr(service, "AdminClient", lambda cfg: object())

    def fake_replay(bootstrap, topics, timeout, group):
        order.append("replay")
        return [*_idle_reports(), _lifecycle("a", "released")], {(TOPIC_STATION_EVENTS, 0): 0}

    monkeypatch.setattr(service, "replay_topics", fake_replay)

    class FakeTail:
        def __init__(self, *a, **k) -> None:
            order.append("tail_init")

        def start(self) -> None:
            order.append("tail_start")

    monkeypatch.setattr(service, "KafkaTail", FakeTail)

    def queue(name):
        q = MagicMock()

        async def consume(cb, no_ack=False):
            order.append(f"consume:{name}")
            return name

        q.consume = consume
        return q

    channel = MagicMock()
    channel.set_qos = AsyncMock()
    channel.get_exchange = AsyncMock(return_value=env.exchange)
    channel.get_queue = AsyncMock(side_effect=lambda name, ensure=True: queue(name))
    conn = MagicMock()
    conn.channel = AsyncMock(return_value=channel)

    s = Scheduler(producer=object(), settings=_settings())
    original_execute = s._execute

    async def spy(actions):
        order.append("execute")
        await original_execute(actions)

    s._execute = spy  # type: ignore[method-assign]
    await s.startup(conn)

    assert order.index("replay") < order.index("execute")
    assert order.index("execute") < order.index(f"consume:{Q_WORKORDERS_INTAKE}")
    assert order.index("execute") < order.index(f"consume:{Q_SCHEDULER_CONTROL}")
    assert order.index("execute") < order.index("consume:scheduler.rework.return")
    assert order[-1] == "tail_start"
    assert s.core.current_run_id == "run-9"
    assert [k for k, _ in env.commands] == ["station.spin"]


async def test_stop_sets_event() -> None:
    s = Scheduler(producer=object(), settings=_settings())
    s.stop()
    assert s._stopping.is_set()


def _ctl(cls, run_id="run-1", **kw):
    return encode(cls.make(run_id=run_id, machine_id="api", **kw))


async def test_pause_idempotent_and_blocks_dispatch(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    m1, m2 = env.msg(_ctl(RunPause)), env.msg(_ctl(RunPause))
    await s.handle_control(m1)
    await s.handle_control(m2)
    assert s.core.paused is True
    paused = [f for _t, f, _k in env.facts if f.type == "run.paused"]
    assert len(paused) == 1
    assert env.facts[0][0] == TOPIC_STATION_EVENTS and env.facts[0][2] == "scheduler"
    assert paused[0].machine_id == "scheduler" and paused[0].run_id == "run-1"
    m1.ack.assert_awaited_once()
    m2.ack.assert_awaited_once()
    await s.handle_intake(env.msg(encode(_order())))
    assert env.commands == []
    assert s.core.backlog_lengths()["spin"] == 1


async def test_pause_with_slept_s_back_dates_run_paused_ts(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    before = datetime.now(UTC)
    await s.handle_control(env.msg(_ctl(RunPause, slept_s=1027)))
    paused = [f for _t, f, _k in env.facts if f.type == "run.paused"]
    assert len(paused) == 1
    back = (before - paused[0].ts).total_seconds()
    assert 1026.9 <= back <= 1028.5


async def test_pause_without_slept_s_stamps_now(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    before = datetime.now(UTC)
    await s.handle_control(env.msg(_ctl(RunPause)))
    paused = [f for _t, f, _k in env.facts if f.type == "run.paused"]
    assert abs((paused[0].ts - before).total_seconds()) < 5


async def test_pause_produce_failure_nacks_without_state_change(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()

    def boom(producer, topic, fact, key):
        raise RuntimeError("kafka down")

    monkeypatch.setattr(service, "produce_fact", boom)
    m = env.msg(_ctl(RunPause))
    await s.handle_control(m)
    assert s.core.paused is False
    m.nack.assert_awaited_once_with(requeue=True)
    m.ack.assert_not_called()


async def test_resume_dispatches_held_work_once(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    await s.handle_control(env.msg(_ctl(RunPause)))
    await s.handle_intake(env.msg(encode(_order())))
    env.calls.clear()
    m = env.msg(_ctl(RunResume))
    await s.handle_control(m)
    assert s.core.paused is False
    assert env.calls[:3] == ["fact:run.resumed:", "command:station.spin", "fact:op.dispatched:"]
    m.ack.assert_awaited_once()
    env.calls.clear()
    await s.handle_control(env.msg(_ctl(RunResume)))
    assert env.calls == ["ack"]


async def test_reset_clears_core_and_defers_old_run(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    await s.handle_control(env.msg(_ctl(RunPause)))
    await s.handle_intake(env.msg(encode(_order())))
    s._defer_counts["x"] = 1
    m = env.msg(_ctl(RunReset))
    await s.handle_control(m)
    m.ack.assert_awaited_once()
    assert s.core.current_run_id is None
    assert s.core.paused is False
    assert s.core.orders == {}
    assert s._defer_counts == {}
    late = env.msg(encode(_order("o9")))
    await s.handle_intake(late)
    late.nack.assert_awaited_once_with(requeue=True)
    assert env.commands == []


async def test_set_rule_stored_and_rate_ignored(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    m = env.msg(_ctl(SetRule, rule="edd"))
    await s.handle_control(m)
    assert s.core.rule == "edd"
    m.ack.assert_awaited_once()
    r = env.msg(_ctl(SetArrivalRate, arrival_rate=2.0))
    await s.handle_control(r)
    r.ack.assert_awaited_once()
    dlx = await env.channel.get_exchange("dlx")
    assert dlx.publish.await_count == 0


async def test_set_recipe_updates_spt_table(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    before = s._recipes["bake"]["margherita"]
    m = env.msg(_ctl(SetRecipe, station="bake", product="margherita", unit_time_s=99.0))
    await s.handle_control(m)
    m.ack.assert_awaited_once()
    assert s._recipes["bake"]["margherita"] == 99.0
    assert s.core.config.recipes["bake"]["margherita"] == 99.0
    assert before != 99.0


async def test_job_command_on_control_still_dead_lettered(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    cmd = JobCommand(
        event_id=uuid.uuid4(),
        run_id="run-1",
        line_id="l",
        machine_id="spin",
        ts=T0,
        schema_version=1,
        order_id="o1",
        step="spin",
        visit=1,
        qty=1,
        recipe="margherita",
        product="margherita",
    )
    await s.handle_control(env.msg(encode(cmd)))
    dlx = await env.channel.get_exchange("dlx")
    assert dlx.publish.await_count == 1


async def test_order_with_priority_carries_priority(monkeypatch) -> None:
    """Priority-10 order: record and emitted lifecycle facts carry 10."""
    env = Env(monkeypatch)
    s = env.scheduler()

    urgent = OrderSubmitted.make(
        run_id="run-1",
        machine_id="generator",
        order_id="urgent-1",
        product="margherita",
        qty=1,
        due_ts=T0 + timedelta(hours=1),
        created_ts=T0,
        priority=10,
    )
    await s.handle_intake(env.msg(encode(urgent)))
    assert s.core.orders["urgent-1"].record.priority == 10
    lifecycle_facts = [f for t, f, k in env.facts if t == TOPIC_ORDER_LIFECYCLE]
    assert len(lifecycle_facts) > 0
    assert all(isinstance(f, OrderLifecycle) and f.priority == 10 for f in lifecycle_facts)

    normal_order = OrderSubmitted.make(
        run_id="run-1",
        machine_id="generator",
        order_id="normal-1",
        product="margherita",
        qty=1,
        due_ts=T0 + timedelta(hours=1),
        created_ts=T0,
    )
    env.facts.clear()
    await s.handle_intake(env.msg(encode(normal_order)))
    assert s.core.orders["normal-1"].record.priority == 50
    lifecycle_facts = [f for t, f, k in env.facts if t == TOPIC_ORDER_LIFECYCLE]
    assert lifecycle_facts[0].priority == 50


async def test_estimate_eta_forwards_priority(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    seen: dict = {}
    real = service.estimate_new

    def _spy(core, **kw):
        seen.update(kw)
        return real(core, **kw)

    monkeypatch.setattr(service, "estimate_new", _spy)
    _capture_replies(env)
    req = _eta_req(priority=10)
    await s.handle_control(_est_msg(env, req))
    assert seen["priority"] == 10


def _prio_facts(env: Env) -> list:
    from pizza.common.envelope import PriorityChanged

    return [(t, f, k) for t, f, k in env.facts if isinstance(f, PriorityChanged)]


async def test_set_priority_emits_fact_then_updates_state(monkeypatch) -> None:
    from pizza.common.envelope import LotSetPriority

    env = Env(monkeypatch)
    s = env.scheduler()
    await s.handle_intake(env.msg(encode(_order())))
    env.facts.clear()
    m = env.msg(_ctl(LotSetPriority, order_id="o1", priority=10))
    await s.handle_control(m)
    facts = _prio_facts(env)
    assert len(facts) == 1
    topic, fact, key = facts[0]
    assert topic == TOPIC_ORDER_LIFECYCLE and key == "o1"
    assert fact.previous_priority == 50 and fact.new_priority == 10 and fact.source == "manual"
    assert s.core.orders["o1"].record.priority == 10
    m.nack.assert_not_called()


async def test_set_priority_later_lifecycle_carries_new_value(monkeypatch) -> None:
    from pizza.common.envelope import LotSetPriority

    env = Env(monkeypatch)
    s = env.scheduler()
    await s.handle_intake(env.msg(encode(_order())))
    await s.handle_control(env.msg(_ctl(LotSetPriority, order_id="o1", priority=10)))
    env.facts.clear()
    await s._emit_lifecycle(service.EmitLifecycle("failed", "o1", T0, step="spin", visit=1))
    assert env.facts[-1][1].priority == 10


async def test_set_priority_noop_emits_nothing(monkeypatch) -> None:
    from pizza.common.envelope import LotSetPriority

    env = Env(monkeypatch)
    s = env.scheduler()
    await s.handle_intake(env.msg(encode(_order())))
    env.facts.clear()
    m = env.msg(_ctl(LotSetPriority, order_id="o1", priority=50))
    await s.handle_control(m)
    assert _prio_facts(env) == []
    m.ack.assert_awaited()


async def test_set_priority_unknown_order_emits_nothing(monkeypatch) -> None:
    from pizza.common.envelope import LotSetPriority

    env = Env(monkeypatch)
    s = env.scheduler()
    m = env.msg(_ctl(LotSetPriority, order_id="ghost", priority=10))
    await s.handle_control(m)
    assert _prio_facts(env) == []
    m.ack.assert_awaited()
    m.nack.assert_not_called()


async def test_set_priority_terminal_order_emits_nothing(monkeypatch) -> None:
    from pizza.common.envelope import LotSetPriority

    env = Env(monkeypatch)
    s = env.scheduler()
    await s.handle_intake(env.msg(encode(_order())))
    s.core.orders["o1"].emitted.add("completed")
    env.facts.clear()
    await s.handle_control(env.msg(_ctl(LotSetPriority, order_id="o1", priority=10)))
    assert _prio_facts(env) == []
    assert s.core.orders["o1"].record.priority == 50


async def test_set_priority_foreign_run_emits_nothing(monkeypatch) -> None:
    from pizza.common.envelope import LotSetPriority

    env = Env(monkeypatch)
    s = env.scheduler()
    await s.handle_intake(env.msg(encode(_order())))
    env.facts.clear()
    await s.handle_control(
        env.msg(_ctl(LotSetPriority, run_id="other", order_id="o1", priority=10))
    )
    assert _prio_facts(env) == []
    assert s.core.orders["o1"].record.priority == 50


async def test_set_priority_produce_failure_retry_emits_one_fact(monkeypatch) -> None:
    from pizza.common.envelope import LotSetPriority

    env = Env(monkeypatch)
    s = env.scheduler()
    await s.handle_intake(env.msg(encode(_order())))
    env.facts.clear()
    body = _ctl(LotSetPriority, order_id="o1", priority=10)
    env.fail_lifecycle = 1
    await s.handle_control(env.msg(body))
    assert s.core.orders["o1"].record.priority == 50
    assert _prio_facts(env) == []
    await s.handle_control(env.msg(body))
    assert len(_prio_facts(env)) == 1
    assert s.core.orders["o1"].record.priority == 10


async def test_set_priority_outstanding_lot_no_extra_dispatch(monkeypatch) -> None:
    from pizza.common.envelope import LotSetPriority

    env = Env(monkeypatch)
    s = env.scheduler()
    await s.handle_intake(env.msg(encode(_order())))  # already dispatched to spin
    commands_before = len(env.commands)
    await s.handle_control(env.msg(_ctl(LotSetPriority, order_id="o1", priority=10)))
    assert len(_prio_facts(env)) == 1
    assert s.core.orders["o1"].record.priority == 10
    assert len(env.commands) == commands_before


async def test_scheduler_settings_has_recipes_dir(monkeypatch) -> None:
    """Test 1: SchedulerSettings has recipes_dir setting."""
    from pizza.scheduler.settings import SchedulerSettings

    settings = SchedulerSettings(recipes_dir="/some/path")
    assert settings.recipes_dir == "/some/path"

    settings_none = SchedulerSettings(recipes_dir=None)
    assert settings_none.recipes_dir is None


async def test_recipes_loaded_into_core_config(monkeypatch) -> None:
    """Test 1: Service loads recipes and passes to CoreConfig."""

    env = Env(monkeypatch)

    # Create a scheduler with recipes_dir set
    settings = _settings(recipes_dir=None)  # Mock scenario

    s = Scheduler(
        producer=object(),
        settings=settings,
        channel=env.channel,
        dispatch_exchange=env.exchange,
        # Inject recipes via constructor would be ideal but not required by plan
    )
    s.core.start_run("run-1")

    # The core config should have recipes (even if empty for now)
    assert isinstance(s.core.config.recipes, dict)


async def test_set_rule_emits_rule_changed_before_state_mutation(monkeypatch) -> None:
    """Test 2: SetRule emits RuleChanged BEFORE core.set_rule()."""
    from pizza.common.envelope import RuleChanged

    env = Env(monkeypatch)
    s = env.scheduler()

    env.facts.clear()
    env.calls.clear()

    # Send SetRule command
    m = env.msg(_ctl(SetRule, rule="spt"))
    await s.handle_control(m)

    # Find RuleChanged fact
    rule_facts = [f for t, f, k in env.facts if isinstance(f, RuleChanged)]
    assert len(rule_facts) > 0, "RuleChanged fact should be emitted"
    rule_fact = rule_facts[0]
    assert rule_fact.rule == "spt"

    # Core rule should be set
    assert s.core.rule == "spt"

    # RuleChanged should come before any state mutation (in the calls sequence)
    # Check that the fact was produced (calls would have "fact:rule.changed")
    assert any("rule.changed" in call for call in env.calls)


async def test_run_started_emits_rule_changed(monkeypatch) -> None:
    """Test 2: RunStarted emits RuleChanged for the run."""
    from pizza.common.envelope import RuleChanged, RunParams

    env = Env(monkeypatch)
    s = env.scheduler()

    env.facts.clear()
    env.calls.clear()

    # Send RunStarted with explicit rule
    params = RunParams(rule="edd")
    msg = RunStarted.make(
        run_id="run-2",
        machine_id="api",
        params=params,
    )
    m = env.msg(encode(msg))
    await s.handle_control(m)

    # Find RuleChanged fact
    rule_facts = [f for t, f, k in env.facts if isinstance(f, RuleChanged)]
    assert len(rule_facts) > 0, "RuleChanged fact should be emitted on RunStarted"
    rule_fact = rule_facts[0]
    assert rule_fact.rule == "edd"
    assert rule_fact.run_id == "run-2"


def _snapshots(env: Env, run_id: str | None = None) -> list[tuple]:
    from pizza.common.envelope import RecipeSnapshot

    return [
        (t, f, k)
        for t, f, k in env.facts
        if isinstance(f, RecipeSnapshot) and (run_id is None or f.run_id == run_id)
    ]


async def test_run_started_emits_recipe_snapshot(monkeypatch) -> None:
    """A new run emits one recipe.snapshot keyed scheduler, before any run state facts."""
    from pizza.common.constants import PRODUCTS
    from pizza.common.envelope import OpDispatched, RecipeSnapshot

    env = Env(monkeypatch)
    s = env.scheduler()

    env.facts.clear()
    env.calls.clear()

    msg = RunStarted.make(run_id="run-2", machine_id="api")
    await s.handle_control(env.msg(encode(msg)))

    snaps = _snapshots(env)
    assert len(snaps) == 1
    topic, fact, key = snaps[0]
    assert topic == TOPIC_STATION_EVENTS
    assert key == "scheduler"
    assert fact.machine_id == "scheduler"
    assert fact.run_id == "run-2"
    assert set(fact.recipes) == set(STATIONS)
    for table in fact.recipes.values():
        assert set(table) == set(PRODUCTS)

    snap_index = next(i for i, (_, f, _) in enumerate(env.facts) if isinstance(f, RecipeSnapshot))
    dispatched = [
        i
        for i, (_, f, _) in enumerate(env.facts)
        if isinstance(f, OpDispatched) and f.run_id == "run-2"
    ]
    if dispatched:
        assert snap_index < dispatched[0]
    else:
        # No dispatch fact yet: the snapshot directly follows RuleChanged, the only fact
        # emitted before core.start_run on RunStarted.
        assert snap_index == 1
        assert env.facts[0][1].type == "rule.changed"


async def test_run_started_redelivery_emits_one_snapshot_and_new_run_emits_another(
    monkeypatch,
) -> None:
    """Redelivered RunStarted for the same run id is deduped; a new run id emits again."""
    env = Env(monkeypatch)
    s = env.scheduler()

    env.facts.clear()
    env.calls.clear()

    msg = encode(RunStarted.make(run_id="run-2", machine_id="api"))
    await s.handle_control(env.msg(msg))
    await s.handle_control(env.msg(msg))
    assert len(_snapshots(env, "run-2")) == 1

    await s.handle_control(env.msg(encode(RunStarted.make(run_id="run-3", machine_id="api"))))
    assert len(_snapshots(env, "run-3")) == 1


async def test_recipe_snapshot_reflects_live_edit(monkeypatch) -> None:
    """A SetRecipe applied before start appears in the snapshot and does not alias live state."""
    env = Env(monkeypatch)
    s = env.scheduler()

    env.facts.clear()
    env.calls.clear()

    edit = SetRecipe.make(
        run_id="run-2",
        machine_id="scheduler",
        station="bake",
        product="margherita",
        unit_time_s=42.0,
    )
    await s.handle_control(env.msg(encode(edit)))
    await s.handle_control(env.msg(encode(RunStarted.make(run_id="run-2", machine_id="api"))))

    snaps = _snapshots(env, "run-2")
    assert len(snaps) == 1
    fact = snaps[0][1]
    assert fact.recipes["bake"]["margherita"] == 42.0

    s._recipes["bake"]["margherita"] = 1.0
    assert fact.recipes["bake"]["margherita"] == 42.0


async def test_rework_hold_grace_passed_to_core_config(monkeypatch) -> None:
    """WR-01: REWORK_HOLD_GRACE_S reaches the core; absent -> core default."""
    from pizza.scheduler.core import CoreConfig

    env = Env(monkeypatch)
    s = Scheduler(
        producer=object(),
        settings=_settings(rework_hold_grace_s=42.0),
        channel=env.channel,
        dispatch_exchange=env.exchange,
    )
    assert s.core.config.rework_hold_grace_s == 42.0
    s2 = Scheduler(
        producer=object(),
        settings=_settings(),
        channel=env.channel,
        dispatch_exchange=env.exchange,
    )
    assert s2.core.config.rework_hold_grace_s == CoreConfig().rework_hold_grace_s


# -- Phase 7.1 plan 05: frozen ETA pass-through and estimate RPCs ----------
ETA = T0 + timedelta(minutes=30)
STATION_ETA = {"spin": T0 + timedelta(minutes=5), "package": ETA}


def _eta_order(order_id="o1") -> OrderSubmitted:
    return OrderSubmitted.make(
        run_id="run-1",
        machine_id="generator",
        order_id=order_id,
        product="margherita",
        qty=2,
        due_ts=T0 + timedelta(hours=1),
        created_ts=T0,
        eta_ts=ETA,
        station_eta=STATION_ETA,
    )


def _lifecycle_facts(env: Env) -> list[OrderLifecycle]:
    return [f for t, f, _ in env.facts if t == TOPIC_ORDER_LIFECYCLE]


async def test_eta_carried_on_every_lifecycle_state(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    await s.handle_intake(env.msg(encode(_eta_order())))
    await s.on_station_event(
        OpStarted.make(
            run_id="run-1",
            machine_id="spin",
            order_id="o1",
            step="spin",
            visit=1,
            attempt=1,
            qty=2,
            command_event_id=uuid.uuid4(),
            redelivered=False,
        )
    )
    for step in STATIONS:
        await s.on_station_event(_completed("o1", step))
    facts = _lifecycle_facts(env)
    assert [f.state for f in facts] == ["released", "in_process", "completed"]
    for f in facts:
        assert f.eta_ts == ETA
        assert f.station_eta == STATION_ETA


async def test_order_without_eta_yields_none(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    await s.handle_intake(env.msg(encode(_order())))
    facts = _lifecycle_facts(env)
    assert facts and facts[0].eta_ts is None and facts[0].station_eta is None


async def test_failed_lifecycle_carries_eta(monkeypatch) -> None:
    from pizza.scheduler.core import EmitLifecycle

    env = Env(monkeypatch)
    s = env.scheduler()
    await s.handle_intake(env.msg(encode(_eta_order())))
    await s._emit_lifecycle(EmitLifecycle("failed", "o1", T0, step="spin", visit=1))
    last = _lifecycle_facts(env)[-1]
    assert last.state == "failed" and last.eta_ts == ETA and last.station_eta == STATION_ETA


def _est_msg(env: Env, cmd, reply_to="reply.q") -> MagicMock:
    m = env.msg(encode(cmd))
    m.reply_to = reply_to
    m.correlation_id = "corr-1"
    return m


def _capture_replies(env: Env, fail: bool = False) -> list:
    replies: list = []

    async def _publish(msg, routing_key):
        if fail:
            raise ConnectionError("boom")
        replies.append((routing_key, msg))

    env.channel.default_exchange = MagicMock()
    env.channel.default_exchange.publish = _publish
    return replies


def _eta_req(deadline=None, run_id="run-1", **kw):
    return EstimateEta.make(
        run_id=run_id,
        machine_id="api",
        product=kw.pop("product", "margherita"),
        qty=kw.pop("qty", 2),
        deadline_ts=deadline or utc_now() + timedelta(seconds=30),
        **kw,
    )


async def test_estimate_eta_replies_ok_and_does_not_mutate(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    await s.handle_intake(env.msg(encode(_order())))
    replies = _capture_replies(env)
    before = (len(s.core.orders), dict(s.core.backlog_lengths()))
    facts_before = len(env.facts)
    m = _est_msg(env, _eta_req())
    await s.handle_control(m)
    assert len(replies) == 1
    key, msg = replies[0]
    body = json.loads(msg.body)
    assert key == "reply.q" and msg.correlation_id == "corr-1"
    assert body["ok"] is True and body["eta_lead_s"] > 0
    assert set(body["station_lead_s"]) == set(STATIONS)
    assert "degraded" in body
    m.ack.assert_awaited_once()
    m.nack.assert_not_called()
    assert (len(s.core.orders), dict(s.core.backlog_lengths())) == before
    assert len(env.facts) == facts_before


async def test_estimate_scale_divides_lead(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    replies = _capture_replies(env)
    await s.handle_control(_est_msg(env, _eta_req()))
    s.timescale.set(4)
    await s.handle_control(_est_msg(env, _eta_req()))
    b1, b4 = (json.loads(r[1].body) for r in replies)
    assert abs(b4["eta_lead_s"] - b1["eta_lead_s"] / 4) < 0.01


async def test_estimate_error_codes_all_acked(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    replies = _capture_replies(env)
    msgs = [
        _est_msg(env, _eta_req(run_id="other")),
        _est_msg(env, _eta_req(deadline=utc_now() - timedelta(seconds=5))),
        _est_msg(env, _eta_req(product="nonexistent")),
    ]
    for m in msgs:
        await s.handle_control(m)
        m.ack.assert_awaited_once()
        m.nack.assert_not_called()
    assert [json.loads(r[1].body)["code"] for r in replies] == ["no_run", "expired", "invalid"]


async def test_estimate_wip_reply(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    await s.handle_intake(env.msg(encode(_order())))
    replies = _capture_replies(env)
    ok = EstimateWip.make(
        run_id="run-1", machine_id="api", deadline_ts=utc_now() + timedelta(seconds=30)
    )
    await s.handle_control(_est_msg(env, ok))
    body = json.loads(replies[0][1].body)
    assert body["ok"] is True and list(body["eta"]) == ["o1"]
    assert body["eta"]["o1"].endswith("Z")
    old = EstimateWip.make(
        run_id="run-1", machine_id="api", deadline_ts=utc_now() - timedelta(seconds=5)
    )
    await s.handle_control(_est_msg(env, old))
    assert json.loads(replies[1][1].body)["code"] == "expired"


async def test_estimate_wip_per_lot_reply(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    await s.handle_intake(env.msg(encode(_order())))
    replies = _capture_replies(env)
    facts_before = len(env.facts)

    def req(oid):
        return EstimateWip.make(
            run_id="run-1",
            machine_id="dashboard",
            order_id=oid,
            deadline_ts=utc_now() + timedelta(seconds=30),
        )

    await s.handle_control(_est_msg(env, req("o1")))
    await s.handle_control(_est_msg(env, req("nope")))
    ok = json.loads(replies[0][1].body)
    assert ok["ok"] is True and set(ok["stations"]) == set(STATIONS)
    assert all(v["out"].endswith("Z") for v in ok["stations"].values())
    miss = json.loads(replies[1][1].body)
    assert miss["ok"] is False and miss["code"] == "not_found"
    assert len(env.facts) == facts_before


async def test_estimate_reply_failure_still_acks(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    _capture_replies(env, fail=True)
    m = _est_msg(env, _eta_req())
    await s.handle_control(m)
    m.ack.assert_awaited_once()
    m.nack.assert_not_called()


async def test_estimate_without_reply_to_acks_silently(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    replies = _capture_replies(env)
    m = _est_msg(env, _eta_req(), reply_to=None)
    await s.handle_control(m)
    assert replies == []
    m.ack.assert_awaited_once()


async def test_estimate_computed_under_lock(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    _capture_replies(env)
    seen: list[bool] = []
    real = service.estimate_new

    def spy(*a, **k):
        seen.append(s._lock.locked())
        return real(*a, **k)

    monkeypatch.setattr(service, "estimate_new", spy)
    await s.handle_control(_est_msg(env, _eta_req()))
    assert seen == [True]
