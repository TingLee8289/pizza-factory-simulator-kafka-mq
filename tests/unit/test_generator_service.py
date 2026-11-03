"""Unit tests for the Generator service (no brokers)."""

import asyncio
import itertools
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from structlog.testing import capture_logs

from pizza.common.envelope import (
    JobCommand,
    ManualOrderRequest,
    OrderLifecycle,
    RunParams,
    RunPause,
    RunReset,
    RunResume,
    RunStarted,
    SetArrivalRate,
    SetRule,
    encode,
    parse_message,
    utc_now,
)
from pizza.common.recipes import load_all_recipes
from pizza.common.topology import EX_WORKORDERS, TOPIC_ORDER_LIFECYCLE
from pizza.generator import service
from pizza.generator.plan import plan_orders
from pizza.generator.service import Generator

RECIPES = load_all_recipes()


async def _no_estimate(*_a):
    return None


def _settings(**kw) -> SimpleNamespace:
    base = {
        "nack_delay_s": 0,
        "publish_retry_s": 0.0,
        "kafka_bootstrap": "x:9092",
        "recipes_dir": None,
        "priority_high_ratio": 0.10,
        "priority_high_value": 10,
    }
    base.update(kw)
    return SimpleNamespace(**base)


class Env:
    def __init__(self, monkeypatch) -> None:
        self.calls: list[str] = []
        self.facts: list[tuple] = []
        self.published: list[tuple] = []
        self.fail_publish = 0
        self.dlx = MagicMock()
        self.dlx.publish = AsyncMock()
        self.channel = MagicMock()
        self.channel.get_exchange = AsyncMock(return_value=self.dlx)
        self.replies: list[tuple] = []

        async def _reply_publish(message, routing_key=None):
            self.replies.append((json.loads(message.body), routing_key, message.correlation_id))

        self.channel.default_exchange = MagicMock()
        self.channel.default_exchange.publish = AsyncMock(side_effect=_reply_publish)
        self.exchange = MagicMock()
        self.exchange.publish = AsyncMock(side_effect=self._publish)
        monkeypatch.setattr(service, "produce_fact", self._produce)

    def _produce(self, producer, topic, fact, key):
        self.calls.append("fact")
        self.facts.append((topic, fact, key))

    async def _publish(self, message, routing_key=None):
        if self.fail_publish > 0:
            self.fail_publish -= 1
            raise ConnectionError("boom")
        self.calls.append("command")
        self.published.append((json.loads(message.body), routing_key))

    def generator(self, **kw) -> Generator:
        clock = iter(range(0, 10**6))
        return Generator(
            producer=object(),
            settings=_settings(),
            channel=self.channel,
            exchange=self.exchange,
            recipes=RECIPES,
            sleep=kw.pop("sleep", self._sleep),
            clock=lambda: float(next(clock)),
            estimator=kw.pop("estimator", _no_estimate),
            **kw,
        )

    async def _sleep(self, _s):
        await asyncio.sleep(0)


def _run_msg(run_id="run-1", params=None) -> MagicMock:
    cmd = RunStarted.make(
        run_id=run_id,
        machine_id="cli",
        ts=datetime(2026, 1, 1, tzinfo=UTC),
        params=params,
    )
    return _msg(encode(cmd))


def _msg(body: bytes) -> MagicMock:
    message = MagicMock()
    message.body = body
    message.ack = AsyncMock()
    message.nack = AsyncMock()
    message.reject = AsyncMock()
    return message


async def _finish(gen: Generator) -> None:
    assert gen._task is not None
    await asyncio.wait_for(gen._task, 5)


@pytest.mark.asyncio
async def test_publishes_created_fact_before_command(monkeypatch):
    env = Env(monkeypatch)
    gen = env.generator()
    msg = _run_msg(params=RunParams(max_orders=3, arrival_rate=100))
    await gen.handle_control(msg)
    msg.ack.assert_awaited_once()
    await _finish(gen)
    assert env.calls == ["fact", "command"] * 3
    assert len(env.facts) == 3 and len(env.published) == 3
    for (topic, fact, key), (body, rk) in zip(env.facts, env.published, strict=True):
        assert topic == TOPIC_ORDER_LIFECYCLE
        assert key == fact.order_id == body["order_id"]
        assert fact.state == "created"
        assert rk == "intake"
        assert body["type"] == "order.submitted"
        assert body["run_id"] == fact.run_id == "run-1"
        assert body["machine_id"] == fact.machine_id == "generator"
    assert EX_WORKORDERS == "workorders"


@pytest.mark.asyncio
async def test_default_params_used_when_none(monkeypatch):
    env = Env(monkeypatch)
    gen = env.generator()
    await gen.handle_control(_run_msg())
    assert gen._task is not None and gen.current_run_id == "run-1"
    gen.stop()
    await gen._cancel_task()


@pytest.mark.asyncio
async def test_duplicate_run_id_ignored_and_new_run_replaces(monkeypatch):
    env = Env(monkeypatch)
    gen = env.generator()
    await gen.handle_control(_run_msg("run-1", RunParams(max_orders=2, arrival_rate=100)))
    first = gen._task
    dup = _run_msg("run-1", RunParams(max_orders=2, arrival_rate=100))
    await gen.handle_control(dup)
    dup.ack.assert_awaited_once()
    assert gen._task is first
    await gen.handle_control(_run_msg("run-2", RunParams(max_orders=2, arrival_rate=100)))
    assert gen._task is not first
    assert gen.current_run_id == "run-2"
    await _finish(gen)


@pytest.mark.asyncio
async def test_invalid_inputs_dead_lettered(monkeypatch):
    env = Env(monkeypatch)
    gen = env.generator()
    bad = _msg(b"not json")
    await gen.handle_control(bad)
    bad.ack.assert_awaited_once()
    job = JobCommand.make(
        run_id="r", machine_id="x", order_id="o", step="bake", qty=1, unit_time_s=1.0
    )
    wrong = _msg(encode(job))
    await gen.handle_control(wrong)
    wrong.ack.assert_awaited_once()
    unknown = _run_msg("run-9", RunParams(weights={"nonexistent": 1.0}))
    await gen.handle_control(unknown)
    unknown.ack.assert_awaited_once()
    assert env.dlx.publish.await_count == 3
    assert gen._task is None
    for m in (bad, wrong, unknown):
        m.nack.assert_not_awaited()
        m.reject.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_failure_retries_same_order(monkeypatch):
    env = Env(monkeypatch)
    env.fail_publish = 2
    gen = env.generator()
    await gen.handle_control(_run_msg(params=RunParams(max_orders=2, arrival_rate=100)))
    await _finish(gen)
    assert [b["order_id"] for b, _ in env.published] == ["L-0001", "L-0002"]
    # the created fact is produced once per order even when the command is retried
    assert len(env.facts) == 2


@pytest.mark.asyncio
async def test_stop_cancels_order_task(monkeypatch):
    env = Env(monkeypatch)
    never = asyncio.Event()

    async def slow_sleep(_s):
        await never.wait()

    gen = env.generator(sleep=slow_sleep)
    await gen.handle_control(_run_msg(params=RunParams(max_orders=5)))
    await asyncio.sleep(0)
    gen.stop()
    await asyncio.wait_for(gen._task, 5)
    assert env.published == []
    assert gen._stopping.is_set()


def _ctl(cmd) -> MagicMock:
    return _msg(encode(cmd))


def _real_generator(env: Env) -> Generator:
    return Generator(
        producer=object(),
        settings=_settings(),
        channel=env.channel,
        exchange=env.exchange,
        recipes=RECIPES,
        estimator=_no_estimate,
    )


@pytest.mark.asyncio
async def test_pause_blocks_publish_and_shifts_timestamps(monkeypatch):
    env = Env(monkeypatch)
    gen = _real_generator(env)
    params = RunParams(max_orders=1, arrival_rate=20)  # first order due ~50 ms
    t0 = utc_now()
    start = RunStarted.make(run_id="run-1", machine_id="cli", ts=t0, params=params)
    await gen.handle_control(_msg(encode(start)))
    await gen.handle_control(_ctl(RunPause.make(run_id="run-1", machine_id="x")))
    await asyncio.sleep(0.4)
    assert env.published == []
    resumed = utc_now()
    await gen.handle_control(_ctl(RunResume.make(run_id="run-1", machine_id="x")))
    await _finish(gen)
    assert len(env.published) == 1
    planned = next(plan_orders(params, t0, RECIPES))
    body = env.published[0][0]
    created = datetime.fromisoformat(body["created_ts"])
    # created_ts is the actual release wall time: after the resume, so shifted by the pause
    assert created >= resumed - timedelta(milliseconds=1)
    assert (created - planned.created_ts).total_seconds() >= 0.3
    # the pause is part of the planned release, so it is not counted as release lag
    assert env.facts[0][1].release_lag_s < 0.3


@pytest.mark.asyncio
async def test_set_rate_keeps_run_and_task(monkeypatch):
    env = Env(monkeypatch)
    never = asyncio.Event()

    async def slow_sleep(_s):
        await never.wait()

    gen = env.generator(sleep=slow_sleep)
    await gen.handle_control(_run_msg(params=RunParams(max_orders=5)))
    task = gen._task
    msg = _ctl(SetArrivalRate.make(run_id="run-1", machine_id="x", arrival_rate=2.0))
    await gen.handle_control(msg)
    msg.ack.assert_awaited_once()
    assert gen._task is task and gen.current_run_id == "run-1" and gen._rate == 2.0
    await asyncio.sleep(0)
    gen.stop()
    await asyncio.wait_for(task, 5)


@pytest.mark.asyncio
async def test_set_rate_ignored_without_run(monkeypatch):
    env = Env(monkeypatch)
    gen = env.generator()
    msg = _ctl(SetArrivalRate.make(run_id="r", machine_id="x", arrival_rate=2.0))
    await gen.handle_control(msg)
    msg.ack.assert_awaited_once()
    assert gen._rate is None
    env.dlx.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_reset_cancels_task_and_clears_state(monkeypatch):
    env = Env(monkeypatch)
    never = asyncio.Event()

    async def slow_sleep(_s):
        await never.wait()

    gen = env.generator(sleep=slow_sleep)
    await gen.handle_control(_run_msg(params=RunParams(max_orders=5)))
    await gen.handle_control(
        _ctl(SetArrivalRate.make(run_id="r", machine_id="x", arrival_rate=2.0))
    )
    await gen.handle_control(_ctl(RunPause.make(run_id="r", machine_id="x")))
    msg = _ctl(RunReset.make(run_id="r", machine_id="x"))
    await gen.handle_control(msg)
    msg.ack.assert_awaited_once()
    assert gen._task is None and gen.current_run_id is None
    assert gen._rate is None and not gen.gate.is_paused


@pytest.mark.asyncio
async def test_started_clears_rate_and_pause(monkeypatch):
    env = Env(monkeypatch)
    gen = env.generator()
    gen._rate = 3.0
    gen.gate.pause("run")
    await gen.handle_control(_run_msg("run-5", RunParams(max_orders=1, arrival_rate=100)))
    assert gen._rate is None and not gen.gate.is_paused
    await _finish(gen)


@pytest.mark.asyncio
async def test_irrelevant_commands_acked_job_still_dead_lettered(monkeypatch):
    env = Env(monkeypatch)
    gen = env.generator()
    for cmd in (
        RunResume.make(run_id="r", machine_id="x"),
        SetRule.make(run_id="r", machine_id="x", rule="edd"),
    ):
        msg = _ctl(cmd)
        await gen.handle_control(msg)
        msg.ack.assert_awaited_once()
    env.dlx.publish.assert_not_awaited()
    job = JobCommand.make(
        run_id="r", machine_id="x", order_id="o", step="bake", qty=1, unit_time_s=1.0
    )
    await gen.handle_control(_ctl(job))
    assert env.dlx.publish.await_count == 1


# -- CR-01: per-run pause baseline (fake clock, no wall-clock sleeping) -------
_T0 = datetime(2026, 1, 1, tzinfo=UTC)


def _fake_clock_generator(env: Env, hold: asyncio.Event | None = None, **kw):
    # now[0] = fake sim clock; now[1] = sim time at which the wall clock reads _T0
    # (tests that start a later run with ts=_T0 move it to that run's start).
    now = [1000.0, 1000.0]
    sleeps: list[float] = []
    # Wall clock tied to the fake sim clock (no host sleep).
    wall = kw.pop("wall", lambda: _T0 + timedelta(seconds=now[0] - now[1]))

    async def fake_sleep(s):
        first = not sleeps
        sleeps.append(s)
        if hold is not None and first:
            await hold.wait()
            return
        now[0] += s
        await asyncio.sleep(0)

    gen = Generator(
        producer=object(),
        settings=_settings(),
        channel=env.channel,
        exchange=env.exchange,
        recipes=RECIPES,
        sleep=fake_sleep,
        clock=lambda: now[0],
        estimator=kw.pop("estimator", _no_estimate),
        wall=wall,
    )
    return gen, now, sleeps


def _planned_two(params: RunParams):
    return list(itertools.islice(plan_orders(params, _T0, RECIPES), 2))


def _assert_ts(env: Env, planned, extra_s: float) -> None:
    assert len(env.published) == len(planned)
    for (body, _rk), exp in zip(env.published, planned, strict=True):
        created = datetime.fromisoformat(body["created_ts"])
        due = datetime.fromisoformat(body["due_ts"])
        assert (created - exp.created_ts).total_seconds() == pytest.approx(extra_s, abs=1e-6)
        assert (due - exp.due_ts).total_seconds() == pytest.approx(extra_s, abs=1e-6)
    # Pause shift is part of the plan, so the release itself is on time.
    for _t, fact, _k in env.facts[-len(planned) :]:
        assert fact.release_lag_s == pytest.approx(0.0, abs=1e-3)


@pytest.mark.asyncio
async def test_pause_in_previous_run_does_not_delay_next_run(monkeypatch):
    env = Env(monkeypatch)
    gen, now, sleeps = _fake_clock_generator(env)
    params = RunParams(max_orders=2, arrival_rate=0.5)
    planned = _planned_two(params)
    await gen.handle_control(_run_msg("run-1", params))
    await gen.handle_control(_ctl(RunPause.make(run_id="run-1", machine_id="x")))
    assert sleeps == []
    now[0] += 30.0
    await gen.handle_control(_ctl(RunResume.make(run_id="run-1", machine_id="x")))
    await gen.handle_control(_ctl(RunReset.make(run_id="run-1", machine_id="x")))
    sleeps.clear()
    env.published.clear()
    now[1] = now[0]  # run-2 starts now, and its RunStarted.ts is _T0
    await gen.handle_control(_run_msg("run-2", params))
    await _finish(gen)
    assert sleeps[0] == pytest.approx(planned[0].offset_s)
    _assert_ts(env, planned, 0.0)


@pytest.mark.asyncio
@pytest.mark.parametrize("via_reset", [True, False])
async def test_pause_while_paused_does_not_delay_next_run(monkeypatch, via_reset):
    env = Env(monkeypatch)
    gen, now, sleeps = _fake_clock_generator(env)
    params = RunParams(max_orders=2, arrival_rate=0.5)
    planned = _planned_two(params)
    await gen.handle_control(_run_msg("run-1", params))
    await gen.handle_control(_ctl(RunPause.make(run_id="run-1", machine_id="x")))
    now[0] += 30.0
    if via_reset:
        await gen.handle_control(_ctl(RunReset.make(run_id="run-1", machine_id="x")))
    sleeps.clear()
    env.published.clear()
    now[1] = now[0]  # run-2 starts now, and its RunStarted.ts is _T0
    await gen.handle_control(_run_msg("run-2", params))
    await _finish(gen)
    assert sleeps[0] == pytest.approx(planned[0].offset_s)
    _assert_ts(env, planned, 0.0)


@pytest.mark.asyncio
async def test_pause_within_run_still_delays_and_shifts(monkeypatch):
    env = Env(monkeypatch)
    hold = asyncio.Event()
    gen, now, sleeps = _fake_clock_generator(env, hold=hold)
    params = RunParams(max_orders=2, arrival_rate=0.5)
    planned = _planned_two(params)
    await gen.handle_control(_run_msg("run-1", params))
    for _ in range(5):
        if sleeps:
            break
        await asyncio.sleep(0)
    assert sleeps == [pytest.approx(planned[0].offset_s)]
    await gen.handle_control(_ctl(RunPause.make(run_id="run-1", machine_id="x")))
    hold.set()
    for _ in range(5):
        await asyncio.sleep(0)
    assert env.published == []
    now[0] += 30.0
    await gen.handle_control(_ctl(RunResume.make(run_id="run-1", machine_id="x")))
    await _finish(gen)
    _assert_ts(env, planned, 30.0)


# -- quick-wrj: manual orders get L-NNNN from the generator ------------------
def _manual(
    run_id="run-1",
    *,
    deadline_s=30.0,
    due_ts=None,
    priority=50,
    product="veggie",
    qty=2,
    buffer_k=1.0,
):
    cmd = ManualOrderRequest.make(
        run_id=run_id,
        machine_id="dashboard",
        product=product,
        qty=qty,
        priority=priority,
        due_ts=due_ts,
        buffer_k=buffer_k,
        deadline_ts=utc_now() + timedelta(seconds=deadline_s),
    )
    msg = _msg(encode(cmd))
    msg.reply_to = "amq.gen-reply"
    msg.correlation_id = str(cmd.event_id)
    return cmd, msg


async def _started(env: Env, max_orders=1):
    """Run with one auto order published (L-0001), then hold the generator."""
    gen = env.generator()
    await gen.handle_control(_run_msg(params=RunParams(max_orders=max_orders, arrival_rate=100)))
    await _finish(gen)
    return gen


@pytest.mark.asyncio
async def test_manual_shares_seq_with_auto(monkeypatch):
    env = Env(monkeypatch)
    gen = await _started(env, 1)
    _, msg = _manual()
    await gen.handle_control(msg)
    assert env.replies[0][0]["order_id"] == "L-0002"
    assert gen._alloc_seq() == 3  # the next auto order would be L-0003


@pytest.mark.asyncio
async def test_auto_skips_seq_taken_by_manual(monkeypatch):
    env = Env(monkeypatch)
    gate = asyncio.Event()

    async def slow_sleep(_s):
        await gate.wait()

    gen = env.generator(sleep=slow_sleep)
    await gen.handle_control(_run_msg(params=RunParams(max_orders=2, arrival_rate=100)))
    _, msg = _manual()
    await gen.handle_control(msg)
    assert env.replies[0][0]["order_id"] == "L-0001"
    gate.set()
    await _finish(gen)
    ids = [b["order_id"] for b, _ in env.published]
    assert ids == ["L-0001", "L-0002", "L-0003"]


@pytest.mark.asyncio
async def test_manual_fact_before_command_and_reply(monkeypatch):
    env = Env(monkeypatch)
    gen = await _started(env, 1)
    env.calls.clear()
    cmd, msg = _manual(priority=10)
    await gen.handle_control(msg)
    assert env.calls == ["fact", "command"]
    topic, fact, key = env.facts[-1]
    body, rk = env.published[-1]
    assert topic == TOPIC_ORDER_LIFECYCLE and rk == "intake"
    assert key == fact.order_id == body["order_id"]
    assert fact.state == "created" and fact.source == "manual" and fact.priority == 10
    assert fact.machine_id == "generator" and body["source"] == "manual" and body["priority"] == 10
    reply, routing_key, corr = env.replies[0]
    assert routing_key == "amq.gen-reply" and corr == str(cmd.event_id)
    assert reply["ok"] is True and reply["run_id"] == "run-1" and reply["priority"] == 10
    assert reply["order_id"] == body["order_id"] and reply["due_ts"] == body["due_ts"]
    msg.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_manual_default_due_and_past_due_rejected(monkeypatch):
    env = Env(monkeypatch)
    gen = await _started(env, 1)
    _, msg = _manual(product="margherita", qty=2)
    await gen.handle_control(msg)
    body = env.published[-1][0]
    expected = RECIPES_TOTAL("margherita", 2) * 2.0
    delta = (
        datetime.fromisoformat(body["due_ts"]) - datetime.fromisoformat(body["created_ts"])
    ).total_seconds()
    assert delta == pytest.approx(expected, abs=0.002)
    n_pub, n_facts = len(env.published), len(env.facts)
    _, bad = _manual(due_ts=utc_now() - timedelta(minutes=1))
    await gen.handle_control(bad)
    assert env.replies[-1][0]["ok"] is False and env.replies[-1][0]["code"] == "invalid"
    assert (len(env.published), len(env.facts)) == (n_pub, n_facts)
    assert gen._alloc_seq() == 3  # failed request consumed no seq
    bad.ack.assert_awaited_once()


def RECIPES_TOTAL(product, qty):
    from pizza.common.recipes import theoretical_total_s

    return theoretical_total_s(RECIPES, product, qty)


@pytest.mark.asyncio
async def test_manual_no_run_wrong_run_and_expired(monkeypatch):
    env = Env(monkeypatch)
    gen = env.generator()
    _, none_msg = _manual()
    await gen.handle_control(none_msg)
    assert env.replies[-1][0]["code"] == "no_run"
    gen = await _started(env, 1)
    _, wrong = _manual(run_id="other")
    await gen.handle_control(wrong)
    assert env.replies[-1][0]["code"] == "no_run"
    _, late = _manual(deadline_s=-1.0)
    await gen.handle_control(late)
    assert env.replies[-1][0]["code"] == "expired"
    assert len(env.published) == 1 and len(env.facts) == 1
    assert gen._next_seq == 1
    for m in (none_msg, wrong, late):
        m.ack.assert_awaited_once()
        m.nack.assert_not_awaited()


@pytest.mark.asyncio
async def test_manual_redelivery_returns_cached_reply(monkeypatch):
    env = Env(monkeypatch)
    gen = await _started(env, 1)
    cmd, first = _manual()
    await gen.handle_control(first)
    again = _msg(first.body)
    again.reply_to = first.reply_to
    again.correlation_id = first.correlation_id
    await gen.handle_control(again)
    assert env.replies[0][0] == env.replies[1][0]
    assert len(env.published) == 2  # one auto + one manual, not three
    assert gen._next_seq == 2
    again.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_seq_counter_resets_on_new_run_and_reset(monkeypatch):
    env = Env(monkeypatch)
    gen = await _started(env, 2)
    assert gen._next_seq == 2
    await gen.handle_control(_ctl(RunReset.make(run_id="run-1", machine_id="x")))
    assert gen._next_seq == 0
    await gen.handle_control(_run_msg("run-2", RunParams(max_orders=1, arrival_rate=100)))
    await _finish(gen)
    assert [b["order_id"] for b, _ in env.published][-1] == "L-0001"


# -- Phase 7.1: ETA at lot release ---------------------------------------------
from pizza.common.constants import STATIONS  # noqa: E402

_S_LEADS = {st: 10.0 * (i + 1) for i, st in enumerate(STATIONS)}


class FakeEstimator:
    def __init__(self, lead=100.0, reply="ok"):
        self.lead = lead
        self.reply = reply
        self.calls: list[tuple] = []

    async def __call__(self, *args):
        self.calls.append(args)
        if self.reply == "none":
            return None
        if self.reply == "raise":
            raise RuntimeError("rpc down")
        if self.reply == "bad":
            return {"ok": True, "eta_lead_s": float("nan"), "station_lead_s": {}}
        return {
            "ok": True,
            "eta_lead_s": self.lead,
            "station_lead_s": dict(_S_LEADS),
            "degraded": False,
        }

    @property
    def call_count(self):
        return len(self.calls)


def _secs(a: str, b: str) -> float:
    return (datetime.fromisoformat(a) - datetime.fromisoformat(b)).total_seconds()


def _z(value: datetime) -> str:
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


@pytest.mark.asyncio
async def test_auto_due_from_eta_and_frozen_on_both_facts(monkeypatch):
    env = Env(monkeypatch)
    est = FakeEstimator(100.0)
    gen = env.generator(estimator=est)
    params = RunParams(max_orders=2, arrival_rate=100, buffer_k=1.5)
    await gen.handle_control(_run_msg(params=params))
    await _finish(gen)
    assert est.call_count == 2
    for (_t, fact, _k), (body, _rk) in zip(env.facts, env.published, strict=True):
        created = fact.created_ts
        assert (fact.due_ts - created).total_seconds() == pytest.approx(150.0, abs=0.002)
        assert (fact.eta_ts - created).total_seconds() == pytest.approx(100.0, abs=0.002)
        for st, lead in _S_LEADS.items():
            assert (fact.station_eta[st] - created).total_seconds() == pytest.approx(
                lead, abs=0.002
            )
        assert _secs(body["due_ts"], body["created_ts"]) == pytest.approx(150.0, abs=0.002)
        assert body["eta_ts"] == _z(fact.eta_ts)
        assert set(body["station_eta"]) == set(_S_LEADS)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["none", "raise", "bad"])
async def test_estimator_failure_falls_back_to_theory_due(monkeypatch, mode):
    env = Env(monkeypatch)
    est = FakeEstimator(reply=mode)
    gen = env.generator(estimator=est)
    params = RunParams(max_orders=2, arrival_rate=100)
    await gen.handle_control(_run_msg(params=params))
    await _finish(gen)
    planned = list(itertools.islice(plan_orders(params, _T0, RECIPES), 2))
    assert [b["order_id"] for b, _ in env.published] == ["L-0001", "L-0002"]
    for (_t, fact, _k), (body, _rk), exp in zip(env.facts, env.published, planned, strict=True):
        assert fact.eta_ts is None and fact.station_eta is None
        assert body.get("eta_ts") is None and body.get("station_eta") is None
        lead = (fact.due_ts - fact.created_ts).total_seconds()
        assert lead == pytest.approx((exp.due_ts - exp.created_ts).total_seconds(), abs=0.002)


@pytest.mark.asyncio
async def test_estimator_called_once_per_order_across_publish_retries(monkeypatch):
    env = Env(monkeypatch)
    env.fail_publish = 2
    est = FakeEstimator(60.0)
    gen = env.generator(estimator=est)
    await gen.handle_control(_run_msg(params=RunParams(max_orders=1, arrival_rate=100)))
    await _finish(gen)
    assert est.call_count == 1
    assert len(env.published) == 1 and len(env.facts) == 1
    assert env.published[0][0]["eta_ts"] == _z(env.facts[0][1].eta_ts)


@pytest.mark.asyncio
async def test_theory_mode_never_calls_estimator(monkeypatch):
    env = Env(monkeypatch)
    est = FakeEstimator(reply="raise")
    gen = env.generator(estimator=est)
    params = RunParams(max_orders=2, arrival_rate=100, due_mode="theory")
    await gen.handle_control(_run_msg(params=params))
    await _finish(gen)
    assert est.call_count == 0
    planned = list(itertools.islice(plan_orders(params, _T0, RECIPES), 2))
    for (_t, fact, _k), exp in zip(env.facts, planned, strict=True):
        assert fact.eta_ts is None
        assert (fact.due_ts - fact.created_ts).total_seconds() == pytest.approx(
            (exp.due_ts - exp.created_ts).total_seconds(), abs=0.002
        )


@pytest.mark.asyncio
async def test_estimator_receives_run_context_and_due_not_before_created(monkeypatch):
    env = Env(monkeypatch)
    est = FakeEstimator(0.0)
    gen = env.generator(estimator=est)
    params = RunParams(max_orders=1, arrival_rate=100, buffer_k=0.5)
    await gen.handle_control(_run_msg("run-7", params))
    await _finish(gen)
    planned = next(plan_orders(params, _T0, RECIPES))
    assert est.calls[0] == ("run-7", planned.product, planned.qty, planned.priority, None, 0.5)
    fact = env.facts[0][1]
    assert fact.due_ts >= fact.created_ts


@pytest.mark.asyncio
async def test_manual_due_from_eta_buffer(monkeypatch):
    env = Env(monkeypatch)
    est = FakeEstimator(80.0)
    gen = await _started(env, 1)
    gen._estimator = est
    _, msg = _manual(buffer_k=1.25)
    await gen.handle_control(msg)
    body = env.published[-1][0]
    fact = env.facts[-1][1]
    assert _secs(body["due_ts"], body["created_ts"]) == pytest.approx(100.0, abs=0.002)
    assert _secs(body["eta_ts"], body["created_ts"]) == pytest.approx(80.0, abs=0.002)
    assert set(fact.station_eta) == set(_S_LEADS)
    reply = env.replies[-1][0]
    assert reply["ok"] is True and reply["due_ts"] == body["due_ts"]
    assert reply["eta_ts"] == body["eta_ts"]
    assert est.calls[0][5] == 1.25


@pytest.mark.asyncio
async def test_manual_explicit_due_wins_but_eta_frozen(monkeypatch):
    env = Env(monkeypatch)
    est = FakeEstimator(80.0)
    gen = await _started(env, 1)
    gen._estimator = est
    due = utc_now() + timedelta(minutes=10)
    _, msg = _manual(due_ts=due)
    await gen.handle_control(msg)
    body = env.published[-1][0]
    assert abs(_secs(body["due_ts"], due.isoformat())) < 0.002
    assert body["eta_ts"] is not None and body["station_eta"]
    _, bad = _manual(due_ts=utc_now() - timedelta(minutes=1))
    await gen.handle_control(bad)
    assert env.replies[-1][0]["code"] == "invalid"
    assert gen._next_seq == 2


@pytest.mark.asyncio
async def test_manual_estimator_none_falls_back(monkeypatch):
    env = Env(monkeypatch)
    gen = await _started(env, 1)
    gen._estimator = FakeEstimator(reply="none")
    _, msg = _manual(product="margherita", qty=2)
    await gen.handle_control(msg)
    body = env.published[-1][0]
    assert _secs(body["due_ts"], body["created_ts"]) == pytest.approx(
        RECIPES_TOTAL("margherita", 2) * 2.0, abs=0.002
    )
    assert env.facts[-1][1].eta_ts is None
    assert env.replies[-1][0]["eta_ts"] is None


@pytest.mark.asyncio
async def test_manual_error_paths_do_not_call_estimator(monkeypatch):
    env = Env(monkeypatch)
    est0 = FakeEstimator(80.0)
    gen = env.generator(estimator=est0)
    _, none_msg = _manual()
    await gen.handle_control(none_msg)
    assert env.replies[-1][0]["code"] == "no_run"
    assert est0.call_count == 0
    gen = await _started(env, 1)
    est = FakeEstimator(80.0)
    gen._estimator = est
    _, wrong = _manual(run_id="other")
    await gen.handle_control(wrong)
    _, late = _manual(deadline_s=-1.0)
    await gen.handle_control(late)
    assert est.call_count == 0
    assert gen._next_seq == 1
    _, first = _manual()
    await gen.handle_control(first)
    assert est.call_count == 1
    again = _msg(first.body)
    again.reply_to = first.reply_to
    again.correlation_id = first.correlation_id
    await gen.handle_control(again)
    assert est.call_count == 1


@pytest.mark.asyncio
async def test_manual_theory_mode_skips_estimator(monkeypatch):
    env = Env(monkeypatch)
    gen = env.generator()
    await gen.handle_control(
        _run_msg(params=RunParams(max_orders=1, arrival_rate=100, due_mode="theory"))
    )
    await _finish(gen)
    est = FakeEstimator(80.0)
    gen._estimator = est
    _, msg = _manual(product="margherita", qty=2)
    await gen.handle_control(msg)
    assert est.call_count == 0
    body = env.published[-1][0]
    assert _secs(body["due_ts"], body["created_ts"]) == pytest.approx(
        RECIPES_TOTAL("margherita", 2) * 2.0, abs=0.002
    )


# -- Phase 07.2: numeric priority (D-07, D-08) --------------------------------
@pytest.mark.asyncio
async def test_auto_order_priority_reaches_fact_and_command(monkeypatch):
    env = Env(monkeypatch)
    gen = env.generator()
    gen.settings.priority_high_ratio = 1.0
    gen.settings.priority_high_value = 10
    await gen.handle_control(_run_msg(params=RunParams(max_orders=2, arrival_rate=100)))
    await _finish(gen)
    assert len(env.facts) == 2 and len(env.published) == 2
    for (_topic, fact, _key), (body, _rk) in zip(env.facts, env.published, strict=True):
        assert fact.source is None and fact.priority == 10
        assert body["priority"] == 10 and body.get("source") is None


@pytest.mark.asyncio
async def test_auto_order_default_priority_when_ratio_zero(monkeypatch):
    env = Env(monkeypatch)
    gen = env.generator()
    gen.settings.priority_high_ratio = 0.0
    await gen.handle_control(_run_msg(params=RunParams(max_orders=2, arrival_rate=100)))
    await _finish(gen)
    assert [body["priority"] for body, _rk in env.published] == [50, 50]


@pytest.mark.asyncio
async def test_manual_priority_independent_of_due_ts(monkeypatch):
    env = Env(monkeypatch)
    gen = await _started(env, 1)
    due = utc_now() + timedelta(minutes=30)
    _, msg = _manual(priority=10, due_ts=due)
    await gen.handle_control(msg)
    body = env.published[-1][0]
    assert body["priority"] == 10
    # D-07: high priority does not rewrite due_ts to "now"; the explicit due is kept.
    assert abs(datetime.fromisoformat(body["due_ts"]) - due) < timedelta(milliseconds=2)


# -- debug generator-late-host-sleep: created/due/eta anchored at actual release ---
_SLEEP_S = 1027.0


def _host_sleep_generator(env: Env, monkeypatch, **kw):
    """Fake clocks where the host 'sleeps' right after the first order is released:
    the sim clock (monotonic) does not advance, the UTC wall clock jumps _SLEEP_S."""
    jump = [0.0]
    holder: dict = {}

    def wall():
        now = holder["now"]
        return _T0 + timedelta(seconds=now[0] - now[1] + jump[0])

    gen, now, sleeps = _fake_clock_generator(env, wall=wall, **kw)
    holder["now"] = now
    produce = service.produce_fact

    def produce_then_sleep(producer, topic, fact, key):
        produce(producer, topic, fact, key)
        jump[0] = _SLEEP_S

    monkeypatch.setattr(service, "produce_fact", produce_then_sleep)
    return gen


@pytest.mark.asyncio
async def test_host_sleep_created_ts_is_actual_release_time(monkeypatch):
    env = Env(monkeypatch)
    gen = _host_sleep_generator(env, monkeypatch)
    params = RunParams(max_orders=3, arrival_rate=0.5)
    planned = list(itertools.islice(plan_orders(params, _T0, RECIPES), 3))
    with capture_logs() as logs:
        await gen.handle_control(_run_msg("run-1", params))
        await _finish(gen)
    assert len(env.facts) == 3
    expected_lag = [0.0, _SLEEP_S, _SLEEP_S]
    for (_t, fact, _k), (body, _rk), exp, lag in zip(
        env.facts, env.published, planned, expected_lag, strict=True
    ):
        # created_ts = wall clock at release = planned + host sleep
        assert (fact.created_ts - exp.created_ts).total_seconds() == pytest.approx(lag, abs=1e-3)
        assert datetime.fromisoformat(body["created_ts"]) == fact.created_ts
        # the planned lead is kept, so due follows the actual release time
        lead = (fact.due_ts - fact.created_ts).total_seconds()
        assert lead == pytest.approx((exp.due_ts - exp.created_ts).total_seconds(), abs=2e-3)
        assert fact.release_lag_s == pytest.approx(lag, abs=1e-3)
    warnings = [e for e in logs if e["event"] == "generator_release_lag"]
    assert len(warnings) == 1  # one warning for the jump, not one per later order
    assert warnings[0]["release_lag_s"] == pytest.approx(_SLEEP_S, abs=1e-3)


@pytest.mark.asyncio
async def test_host_sleep_eta_and_due_anchor_at_release(monkeypatch):
    env = Env(monkeypatch)
    est = FakeEstimator(100.0)
    gen = _host_sleep_generator(env, monkeypatch, estimator=est)
    params = RunParams(max_orders=2, arrival_rate=0.5, buffer_k=1.5)
    planned = list(itertools.islice(plan_orders(params, _T0, RECIPES), 2))
    await gen.handle_control(_run_msg("run-1", params))
    await _finish(gen)
    second = env.facts[1][1]
    assert (second.created_ts - planned[1].created_ts).total_seconds() == pytest.approx(
        _SLEEP_S, abs=1e-3
    )
    assert (second.eta_ts - second.created_ts).total_seconds() == pytest.approx(100.0, abs=2e-3)
    assert (second.due_ts - second.created_ts).total_seconds() == pytest.approx(150.0, abs=2e-3)
    for st, lead in _S_LEADS.items():
        assert (second.station_eta[st] - second.created_ts).total_seconds() == pytest.approx(
            lead, abs=2e-3
        )


def test_lifecycle_release_lag_optional_for_old_facts():
    fact = OrderLifecycle.make(
        run_id="r",
        machine_id="generator",
        state="created",
        order_id="L-0001",
        product="veggie",
        qty=1,
        due_ts=_T0,
        created_ts=_T0,
    )
    assert fact.release_lag_s is None
    raw = json.loads(encode(fact))
    raw.pop("release_lag_s", None)
    assert parse_message(json.dumps(raw).encode()).release_lag_s is None


# -- host sleep watch (quick-261022-wsj) --------------------------------------
from pizza.common.envelope import HostSleepDetected  # noqa: E402
from pizza.common.sleepwatch import HostSleepDetector  # noqa: E402
from pizza.common.topology import TOPIC_STATION_EVENTS  # noqa: E402


def _sleep_generator(env: Env):
    gen = env.generator()
    clk = {"mono": 0.0, "wall": datetime(2026, 1, 1, tzinfo=UTC)}
    gen._sleepwatch = HostSleepDetector(5.0, mono=lambda: clk["mono"], wall=lambda: clk["wall"])

    def jump(wall_s: float) -> None:
        clk["wall"] += timedelta(seconds=wall_s)

    return gen, jump


@pytest.mark.asyncio
async def test_host_sleep_emits_one_fact_when_running(monkeypatch):
    env = Env(monkeypatch)
    gen, jump = _sleep_generator(env)
    gen.current_run_id = "run-1"
    jump(1027)
    await gen._sleep_tick()
    await gen._sleep_tick()  # no further jump
    sleeps = [f for f in env.facts if isinstance(f[1], HostSleepDetected)]
    assert len(sleeps) == 1
    topic, fact, _key = sleeps[0]
    assert topic == TOPIC_STATION_EVENTS
    assert fact.run_id == "run-1"
    assert fact.slept_s == pytest.approx(1027, abs=0.01)


@pytest.mark.asyncio
async def test_host_sleep_silent_when_paused_or_no_run(monkeypatch):
    env = Env(monkeypatch)
    gen, jump = _sleep_generator(env)
    jump(100)
    await gen._sleep_tick()  # no run
    gen.current_run_id = "run-1"
    gen.gate.pause("run")
    jump(100)
    await gen._sleep_tick()  # paused
    assert env.facts == []
