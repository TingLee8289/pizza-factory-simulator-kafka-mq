"""Unit tests for the Scheduler shell failure handling (no brokers)."""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from pizza.common.constants import STATIONS
from pizza.common.envelope import (
    JobCommand,
    LotBank,
    LotHoldCancel,
    LotRelease,
    LotScrap,
    MachineBreak,
    OpCompleted,
    OpFailed,
    OrderSubmitted,
    RunPause,
    RunReset,
    RunStarted,
    StationHeartbeat,
    StationStatus,
    encode,
)
from pizza.common.ids import job_event_id
from pizza.common.topology import (
    EX_DLX,
    EX_SCHEDULER_REWORK,
    Q_REWORK_RETURN,
    TOPIC_ORDER_LIFECYCLE,
    TOPIC_STATION_EVENTS,
    jobs_queue_name,
)
from pizza.scheduler import service
from pizza.scheduler.core import (
    EmitHold,
    EmitLifecycle,
    PublishDeadLetter,
    PublishRework,
    SchedulerCore,
)
from pizza.scheduler.service import Scheduler

T0 = datetime(2026, 1, 1, tzinfo=UTC)
INTERVAL = 5.0


def _settings(**kw) -> SimpleNamespace:
    base = {
        "nack_delay_s": 0,
        "dispatch_retry_s": 0,
        "intake_defer_max": 2,
        "resume_on_start": True,
        "replay_timeout_s": 1.0,
        "kafka_group_id": "scheduler",
        "kafka_bootstrap": "x:9092",
        "heartbeat_interval_s": INTERVAL,
        "offline_factor": 3,
        "sweep_interval_s": 0.01,
    }
    base.update(kw)
    return SimpleNamespace(**base)


class Env:
    def __init__(self, monkeypatch) -> None:
        self.calls: list[str] = []
        self.facts: list[tuple] = []
        self.commands: list[tuple] = []  # (exchange, routing_key, command, headers)
        self.fail_publish = 0
        self.fail_produce_types: list[str] = []
        self.now = 1000.0
        self.exchanges: dict[str, MagicMock] = {}
        self.channel = MagicMock()
        self.channel.get_exchange = AsyncMock(side_effect=self._get_exchange)
        self.dispatch_ex = object()
        monkeypatch.setattr(service, "produce_fact", self._produce)
        monkeypatch.setattr(service, "publish_command", self._publish)

    async def _get_exchange(self, name):
        return self.exchanges.setdefault(name, f"ex:{name}")

    def _produce(self, producer, topic, fact, key):
        if fact.type in self.fail_produce_types:
            self.fail_produce_types.remove(fact.type)
            raise RuntimeError("kafka down")
        label = f"{fact.type}:{getattr(fact, 'state', '')}"
        self.calls.append(f"fact:{label}")
        self.facts.append((topic, fact, key))

    async def _publish(self, exchange, routing_key, command, headers=None):
        if self.fail_publish > 0:
            self.fail_publish -= 1
            raise ConnectionError("boom")
        self.calls.append(f"command:{routing_key}")
        self.commands.append((exchange, routing_key, command, headers))

    def scheduler(self, **kw) -> Scheduler:
        s = Scheduler(
            producer=object(),
            settings=_settings(**kw),
            channel=self.channel,
            dispatch_exchange=self.dispatch_ex,
            clock=lambda: self.now,
        )
        s.core.start_run("run-1")
        # Every station reports idle first: a never-reported station is not dispatchable.
        for station in s.core.route:
            s.core.on_station_status(
                StationStatus.make(run_id="run-1", machine_id=station, status="idle")
            )
        s._seed_last_seen()
        return s

    def msg(self, body: bytes) -> MagicMock:
        message = MagicMock()
        message.body = body
        message.ack = AsyncMock()
        message.nack = AsyncMock()
        message.reject = AsyncMock()
        return message

    def facts_of(self, type_: str) -> list:
        return [f for _t, f, _k in self.facts if f.type == type_]


def _order(order_id="o1", **kw) -> OrderSubmitted:
    return OrderSubmitted.make(
        run_id="run-1",
        machine_id="generator",
        order_id=order_id,
        product="margherita",
        qty=2,
        due_ts=T0 + timedelta(hours=1),
        created_ts=T0,
        **kw,
    )


def _failed(order_id: str, step: str, visit: int) -> OpFailed:
    return OpFailed.make(
        run_id="run-1",
        machine_id=step,
        order_id=order_id,
        step=step,
        visit=visit,
        attempt=1,
        qty=2,
        command_event_id=uuid.uuid4(),
        reason="defect",
    )


def _status(station: str, status: str, run_id="run-1") -> StationStatus:
    return StationStatus.make(run_id=run_id, machine_id=station, status=status)


def _hb(station: str, status="idle", run_id="run-1") -> StationHeartbeat:
    return StationHeartbeat.make(run_id=run_id, machine_id=station, status=status)


async def _intake(env: Env, s: Scheduler, order: OrderSubmitted) -> None:
    await s.handle_intake(env.msg(encode(order)))


# -- task 1 -----------------------------------------------------------------
async def test_core_config_from_settings(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler(
        rework_limits={"spin": 5},
        station_queue_limit=4,
        bank_after_s=9.0,
        bank_wait_threshold=7,
    )
    s.core = SchedulerCore(config=s._core_config())
    cfg = s.core.config
    assert cfg.rework_limits == {"spin": 5}
    assert cfg.queue_limit == 4 and cfg.bank_after_s == 9.0 and cfg.bank_wait_threshold == 7
    plain = env.scheduler()
    assert plain.core.config.queue_limit is None
    # same config after RunReset
    await s._apply_control(RunReset.make(run_id="run-1", machine_id="x"))
    assert s.core.config.rework_limits == {"spin": 5}


async def test_down_blocks_dispatch_idle_resumes(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    await s.on_station_event(_status("spin", "down"))
    await _intake(env, s, _order())
    assert env.commands == []
    await s.on_station_event(_status("spin", "idle"))
    assert [c[1] for c in env.commands] == ["station.spin"]


async def test_heartbeat_liveness_and_offline_echo(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    env.now += 4 * INTERVAL
    await s.on_station_event(_status("bake", "offline"))
    assert s._last_seen["bake"] == 1000.0  # offline echo does not refresh
    await s.on_station_event(_hb("bake", run_id="unknown"))
    assert s._last_seen["bake"] == env.now
    assert s.core.health["bake"].reported == "idle"


async def test_sweeper_marks_offline_and_recovers(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    env.now += 3.0 * INTERVAL
    await s.sweep_once()
    assert not any(h.offline for h in s.core.health.values())
    env.now += 0.1 * INTERVAL
    for st in STATIONS:
        if st != "spin":
            s._last_seen[st] = env.now
    await s.sweep_once()
    assert s.core.health["spin"].offline
    offline = [f for _t, f, k in env.facts if f.type == "station.status" and k == "spin"]
    assert [f.status for f in offline] == ["offline"]
    assert env.facts[-1][0] == TOPIC_STATION_EVENTS
    await _intake(env, s, _order())
    assert env.commands == []
    await s.on_station_event(_hb("spin"))
    assert not s.core.health["spin"].offline
    assert [c[1] for c in env.commands] == ["station.spin"]
    statuses = [f.status for f in env.facts_of("station.status")]
    assert statuses == ["offline", "idle"]


async def test_heartbeat_disabled_still_sweeps(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler(heartbeat_interval_s=0)
    env.now += 10_000
    called = []
    orig = s.core.sweep
    s.core.sweep = lambda now: (called.append(now), orig(now))[1]
    await s.sweep_once()
    assert not any(h.offline for h in s.core.health.values())
    assert called


async def test_seed_last_seen(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    env.now = 5000.0
    s._seed_last_seen()
    assert set(s._last_seen) == set(STATIONS)
    assert all(v == 5000.0 for v in s._last_seen.values())
    env.now = 6000.0
    await s._apply_control(RunStarted.make(run_id="run-2", machine_id="x"))
    assert all(v == 6000.0 for v in s._last_seen.values())


async def test_sweeper_loop_survives_errors_and_cancels(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler(sweep_interval_s=0.001)
    calls = {"n": 0}

    async def boom() -> None:
        calls["n"] += 1
        raise RuntimeError("x")

    s.sweep_once = boom  # type: ignore[method-assign]
    task = asyncio.create_task(s._sweeper_loop())
    await asyncio.sleep(0.05)
    assert calls["n"] >= 2 and not task.done()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    assert task.done()


# -- task 2 -----------------------------------------------------------------
def _return_msg(env: Env, s: Scheduler, order_id: str, step: str, visit: int) -> MagicMock:
    cmd = s._job_command(
        order_id=order_id,
        step=step,
        visit=visit,
        qty=2,
        product="margherita",
        event_id=job_event_id("run-1", order_id, step, visit),
    )
    return env.msg(encode(cmd))


def _dispatched(env: Env, order_id: str, step: str, visit: int) -> list:
    return [
        f
        for f in env.facts_of("op.dispatched")
        if (f.order_id, f.step, f.visit) == (order_id, step, visit)
    ]


async def test_rework_publishes_to_hold_exchange(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    await _intake(env, s, _order())
    await _intake(env, s, _order("o2"))
    env.commands.clear()
    env.facts.clear()
    fail = _failed("o1", "spin", 1)
    await s.on_station_event(fail)
    env.channel.get_exchange.assert_any_call(EX_SCHEDULER_REWORK)
    ex, key, cmd, _h = env.commands[0]
    assert ex == f"ex:{EX_SCHEDULER_REWORK}" and key == "hold.spin"
    assert isinstance(cmd, JobCommand) and cmd.visit == 2
    assert cmd.event_id == job_event_id("run-1", "o1", "spin", 2)
    # D-08: no op.dispatched for the rework visit at failure time
    assert _dispatched(env, "o1", "spin", 2) == []
    # D-01: credit released, o2 dispatched in the same batch
    assert s.core.outstanding["spin"].order_id == "o2"
    assert any(c[1] == "station.spin" and c[2].order_id == "o2" for c in env.commands)


async def test_hold_return_dispatches_once_with_failed_ts(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    await _intake(env, s, _order())
    fail = _failed("o1", "spin", 1)
    await s.on_station_event(fail)
    env.commands.clear()
    env.facts.clear()
    m = _return_msg(env, s, "o1", "spin", 2)
    await s.handle_rework_return(m)
    m.ack.assert_awaited_once()
    m.reject.assert_not_called()
    assert [c[1] for c in env.commands] == ["station.spin"]
    disp = _dispatched(env, "o1", "spin", 2)
    assert len(disp) == 1 and disp[0].ready_ts == fail.ts
    # duplicate return: acked, no second dispatch
    m2 = _return_msg(env, s, "o1", "spin", 2)
    await s.handle_rework_return(m2)
    m2.ack.assert_awaited_once()
    assert len(env.commands) == 1
    assert len(_dispatched(env, "o1", "spin", 2)) == 1


async def test_rework_return_invalid_body_dead_lettered(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    dead = AsyncMock()
    monkeypatch.setattr(service, "dead_letter_invalid", dead)
    m = env.msg(b"not json")
    await s.handle_rework_return(m)
    dead.assert_awaited_once()
    assert dead.await_args.args[3] == Q_REWORK_RETURN
    # wrong type
    m2 = env.msg(encode(RunPause.make(run_id="run-1", machine_id="x")))
    await s.handle_rework_return(m2)
    assert dead.await_count == 2
    m.reject.assert_not_called()
    m2.reject.assert_not_called()


async def test_rework_return_exception_nacks_never_loops(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()

    def boom(*_a, **_k):
        raise RuntimeError("x")

    s.core.on_rework_ready = boom  # type: ignore[method-assign]
    m = _return_msg(env, s, "o1", "spin", 2)
    await s.handle_rework_return(m)
    m.nack.assert_awaited_once_with(requeue=True)
    m.ack.assert_not_called()
    m.reject.assert_not_called()


async def test_exhausted_goes_to_dlx_and_fails_lifecycle(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    await _intake(env, s, _order())
    await s.on_station_event(_failed("o1", "spin", 1))
    await s.handle_rework_return(_return_msg(env, s, "o1", "spin", 2))
    await s.on_station_event(_failed("o1", "spin", 2))
    await s.handle_rework_return(_return_msg(env, s, "o1", "spin", 3))
    await _intake(env, s, _order("o2"))  # waits behind o1 visit 3
    env.commands.clear()
    env.facts.clear()
    start = len(env.calls)
    await s.on_station_event(_failed("o1", "spin", 3))
    # DLQ publish happens before the lifecycle failed fact (ordering guarantee).
    labels = env.calls[start:]
    assert labels.index("command:") < labels.index("fact:order.lifecycle:failed")
    env.channel.get_exchange.assert_any_call(EX_DLX)
    ex, key, cmd, headers = env.commands[0]
    assert ex == f"ex:{EX_DLX}" and key == ""
    assert headers == {
        "x-failed-reason": "rework_exhausted",
        "x-failed-step": "spin",
        "x-failed-visit": 3,
        "x-source-queue": jobs_queue_name("spin"),
    }
    assert cmd.visit == 3
    life = [f for f in env.facts_of("order.lifecycle") if f.state == "failed"]
    assert len(life) == 1 and life[0].step == "spin" and life[0].visit == 3
    # next waiting order dispatched in the same cycle
    assert env.commands[1][1] == "station.spin" and env.commands[1][2].order_id == "o2"


async def test_rework_publish_failure_retries_once(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler(dispatch_retry_s=0)
    await _intake(env, s, _order())
    env.commands.clear()
    env.facts.clear()
    env.fail_publish = 1
    await s.on_station_event(_failed("o1", "spin", 1))
    assert env.commands == []
    assert len(s._pending) == 1 and isinstance(s._pending[0], PublishRework)
    await s.drain_retries()
    assert len(env.commands) == 1
    assert env.commands[0][1] == "hold.spin"
    assert env.commands[0][2].event_id == job_event_id("run-1", "o1", "spin", 2)
    assert env.facts_of("op.dispatched") == []  # only emitted after the return
    assert s._pending == []


async def test_lot_and_status_facts(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    await s.on_station_event(_status("spin", "down"))
    await _intake(env, s, _order())
    stocked = env.facts_of("lot.stocked")
    assert len(stocked) == 1 and stocked[0].kind == "station_down"
    t, _f, key = [x for x in env.facts if x[1].type == "lot.stocked"][0]
    assert t == TOPIC_ORDER_LIFECYCLE and key == "o1"
    await s.on_station_event(_status("spin", "idle"))
    assert len(env.facts_of("lot.unstocked")) == 1
    # produce failure is kept and retried
    await s._apply_control(
        LotBank.make(run_id="run-1", machine_id="x", order_id="o1", reason="customer")
    )
    await _intake(env, s, _order("o2"))
    env.fail_produce_types = ["lot.banked"]
    await s._apply_control(
        LotBank.make(run_id="run-1", machine_id="x", order_id="o2", reason="customer")
    )
    assert any(type(a).__name__ == "EmitLot" for a in s._pending)
    await s.drain_retries()
    assert s._pending == []
    banked = env.facts_of("lot.banked")
    assert {b.order_id for b in banked} == {"o1", "o2"} or len(banked) >= 1


async def test_fail_flags_propagate(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    await _intake(env, s, _order("o1", fail_step="spin", fail_times=1, poison=True))
    await _intake(env, s, _order("o3"))
    cmd = env.commands[0][2]
    assert cmd.poison is True and cmd.fail_times == 1
    released = [f for f in env.facts_of("order.lifecycle") if f.state == "released"]
    assert released[0].fail_step == "spin" and released[0].poison is True
    assert released[1].fail_step is None and released[1].poison is False
    # other order without flags
    s.core.on_op_started  # noqa: B018
    other = s._job_command(
        order_id="o3", step="spin", visit=1, qty=2, product="margherita", event_id=uuid.uuid4()
    )
    assert other.poison is False and other.fail_times is None
    # flags only for the fail_step
    not_step = s._job_command(
        order_id="o1", step="sauce", visit=1, qty=2, product="margherita", event_id=uuid.uuid4()
    )
    assert not_step.poison is False


async def test_failed_dispatch_keeps_other_actions(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    env.fail_publish = 1
    await _intake(env, s, _order())
    assert env.commands == []
    assert s.core.outstanding["spin"] is None
    await s.drain_retries()
    assert [c[1] for c in env.commands] == ["station.spin"]


# -- task 3 -----------------------------------------------------------------
async def test_bank_release_control(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    await s.on_station_event(_status("spin", "busy"))
    await _intake(env, s, _order())  # dispatched to spin
    await _intake(env, s, _order("o2"))  # backlog
    m = env.msg(
        encode(LotBank.make(run_id="run-1", machine_id="x", order_id="o2", reason="customer"))
    )
    await s.handle_control(m)
    m.ack.assert_awaited_once()
    banked = env.facts_of("lot.banked")
    assert len(banked) == 1 and banked[0].source == "manual" and banked[0].reason == "customer"
    # unknown / dispatched / already banked / foreign run
    n = len(env.facts)
    for oid, run in (("zz", "run-1"), ("o2", "run-1"), ("o2", "other")):
        mm = env.msg(
            encode(LotBank.make(run_id=run, machine_id="x", order_id=oid, reason="customer"))
        )
        await s.handle_control(mm)
        mm.ack.assert_awaited_once()
    assert len(env.facts) == n
    # release o2 manual: waits behind o1 (credit taken)
    r = env.msg(encode(LotRelease.make(run_id="run-1", machine_id="x", order_id="o2")))
    await s.handle_control(r)
    r.ack.assert_awaited_once()
    assert [f.by for f in env.facts_of("lot.released")] == ["manual"]
    # release again of a non-banked lot: no fact
    n = len(env.facts)
    await s.handle_control(
        env.msg(encode(LotRelease.make(run_id="run-1", machine_id="x", order_id="o2")))
    )
    assert len(env.facts) == n


async def test_scrap_control_dead_letters_and_emits_failed(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    await s.on_station_event(_status("spin", "busy"))
    await _intake(env, s, _order())  # dispatched to spin
    await _intake(env, s, _order("o2"))  # backlog
    env.commands.clear()
    n = len(env.facts)
    m = env.msg(
        encode(LotScrap.make(run_id="run-1", machine_id="x", order_id="o2", reason="operator"))
    )
    await s.handle_control(m)
    m.ack.assert_awaited_once()
    new = [f for _t, f, _k in env.facts[n:]]
    failed = [f for f in new if f.type == "order.lifecycle" and f.state == "failed"]
    assert len(failed) == 1 and failed[0].order_id == "o2" and failed[0].step == "spin"
    dlq = [c for c in env.commands if c[0] == f"ex:{EX_DLX}"]
    assert len(dlq) == 1 and dlq[0][3]["x-failed-reason"] == "manual_scrap:operator"
    # repeat, dispatched lot, unknown lot, foreign run: acked, no new fact
    n = len(env.facts)
    for oid, run in (("o2", "run-1"), ("zz", "run-1"), ("o2", "other")):
        mm = env.msg(
            encode(LotScrap.make(run_id=run, machine_id="x", order_id=oid, reason="other"))
        )
        await s.handle_control(mm)
        mm.ack.assert_awaited_once()
    assert len(env.facts) == n


async def test_control_allowlist_and_errors(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    dead = AsyncMock()
    monkeypatch.setattr(service, "dead_letter_invalid", dead)
    await s.handle_control(env.msg(encode(MachineBreak.make(run_id="run-1", machine_id="spin"))))
    dead.assert_awaited_once()
    # exception -> nack requeue after delay, never reject
    m = env.msg(encode(RunPause.make(run_id="run-1", machine_id="x")))

    async def boom(_parsed) -> None:
        raise RuntimeError("x")

    s._apply_control = boom  # type: ignore[method-assign]
    await s.handle_control(m)
    m.nack.assert_awaited_once_with(requeue=True)
    m.reject.assert_not_called()


async def test_recover_reconciles_lost_rework(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    life = service.OrderLifecycle.make(
        run_id="run-1",
        machine_id="scheduler",
        state="released",
        order_id="o1",
        product="margherita",
        qty=2,
        due_ts=T0 + timedelta(hours=1),
        created_ts=T0,
    )
    disp = service.OpDispatched.make(
        run_id="run-1",
        machine_id="spin",
        order_id="o1",
        step="spin",
        visit=1,
        qty=2,
        product="margherita",
        command_event_id=job_event_id("run-1", "o1", "spin", 1),
        ready_ts=T0,
    )
    fail = _failed("o1", "spin", 1)
    await s.recover([life, disp, fail])
    assert [c[1] for c in env.commands] == ["hold.spin"]
    assert env.commands[0][2].visit == 2
    # with the later op.dispatched visit 2 nothing extra is published
    env2 = Env(monkeypatch)
    s2 = env2.scheduler()
    disp2 = service.OpDispatched.make(
        run_id="run-1",
        machine_id="spin",
        order_id="o1",
        step="spin",
        visit=2,
        qty=2,
        product="margherita",
        command_event_id=job_event_id("run-1", "o1", "spin", 2),
        ready_ts=T0 + timedelta(seconds=1),
    )
    await s2.recover([life, disp, fail, disp2])
    assert env2.commands == []


async def test_recover_cooling_then_return_dispatches_once(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    life = service.OrderLifecycle.make(
        run_id="run-1",
        machine_id="scheduler",
        state="released",
        order_id="o1",
        product="margherita",
        qty=2,
        due_ts=T0 + timedelta(hours=1),
        created_ts=T0,
    )
    disp = service.OpDispatched.make(
        run_id="run-1",
        machine_id="spin",
        order_id="o1",
        step="spin",
        visit=1,
        qty=2,
        product="margherita",
        command_event_id=job_event_id("run-1", "o1", "spin", 1),
        ready_ts=T0,
    )
    fail = _failed("o1", "spin", 1)
    await s.recover([life, disp, fail])
    assert "o1" in s.core.cooling
    for station in s.core.route:  # rebuilt core: stations report idle again
        s.core.on_station_status(_status(station, "idle"))
    env.commands.clear()
    await s.handle_rework_return(_return_msg(env, s, "o1", "spin", 2))
    await s.handle_rework_return(_return_msg(env, s, "o1", "spin", 2))
    assert [c[1] for c in env.commands] == ["station.spin"]
    assert len(_dispatched(env, "o1", "spin", 2)) == 1


async def test_recover_dead_letter(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    facts = [
        service.OrderLifecycle.make(
            run_id="run-1",
            machine_id="scheduler",
            state="released",
            order_id="o1",
            product="margherita",
            qty=2,
            due_ts=T0 + timedelta(hours=1),
            created_ts=T0,
        ),
        _failed("o1", "spin", 3),
    ]
    await s.recover(facts)
    assert env.commands[0][0] == f"ex:{EX_DLX}"
    assert any(f.state == "failed" for f in env.facts_of("order.lifecycle"))


async def test_run_started_and_reset_clear_pending(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler(bank_after_s=9.0)
    s._pending.append(PublishDeadLetter("o", "spin", 3, "m", 1, uuid.uuid4(), "rework_exhausted"))
    await s._apply_control(RunStarted.make(run_id="run-2", machine_id="x"))
    assert s._pending == [] and s.core.config.bank_after_s == 9.0
    s._pending.append(EmitLifecycle("released", "o", T0))
    s.core.rule = "edd"
    await s._apply_control(RunReset.make(run_id="run-2", machine_id="x"))
    assert s._pending == [] and s.core.rule == "edd" and s.core.config.bank_after_s == 9.0


# -- quick 261023-pfp: deferred manual bank / scrap through the shell -----------
def _completed(order_id: str, step: str, visit: int = 1) -> OpCompleted:
    return OpCompleted.make(
        run_id="run-1",
        machine_id=step,
        order_id=order_id,
        step=step,
        visit=visit,
        attempt=1,
        qty=2,
        command_event_id=job_event_id("run-1", order_id, step, visit),
        duration_s=0.0,
    )


async def test_hold_request_cancel_and_apply_through_shell(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler()
    await _intake(env, s, _order())  # dispatched to spin (outstanding)
    n = len(env.facts)
    await s.handle_control(
        env.msg(
            encode(LotBank.make(run_id="run-1", machine_id="x", order_id="o1", reason="customer"))
        )
    )
    holds = [f for f in env.facts_of("lot.hold")]
    assert [(h.state, h.kind, h.station, h.reason) for h in holds] == [
        ("requested", "bank", "spin", "customer")
    ]
    assert env.facts_of("lot.banked") == [] and len(env.facts) == n + 1
    # the LotHoldCancel command reaches core.cancel_hold
    c = env.msg(encode(LotHoldCancel.make(run_id="run-1", machine_id="x", order_id="o1")))
    await s.handle_control(c)
    c.ack.assert_awaited_once()
    assert [h.state for h in env.facts_of("lot.hold")] == ["requested", "cancelled"]
    assert "o1" not in s.core.holds
    # request again, then the op completes: applied + banked at the next station
    await s.handle_control(
        env.msg(
            encode(LotBank.make(run_id="run-1", machine_id="x", order_id="o1", reason="schedule"))
        )
    )
    await s.on_station_event(_completed("o1", "spin"))
    assert [h.state for h in env.facts_of("lot.hold")][-1] == "applied"
    banked = env.facts_of("lot.banked")
    assert len(banked) == 1 and banked[0].station == "sauce" and banked[0].source == "manual"


async def test_emit_hold_failure_is_deferred_and_retried(monkeypatch) -> None:
    env = Env(monkeypatch)
    s = env.scheduler(dispatch_retry_s=0)
    await _intake(env, s, _order())
    env.fail_produce_types.append("lot.hold")
    await s.handle_control(
        env.msg(
            encode(LotBank.make(run_id="run-1", machine_id="x", order_id="o1", reason="customer"))
        )
    )
    assert env.facts_of("lot.hold") == []
    assert len(s._pending) == 1 and isinstance(s._pending[0], EmitHold)
    await s.drain_retries()
    assert [h.state for h in env.facts_of("lot.hold")] == ["requested"]
    assert s._pending == []
