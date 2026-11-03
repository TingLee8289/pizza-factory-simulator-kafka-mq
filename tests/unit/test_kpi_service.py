"""Unit tests for the KPI effect shell (no brokers)."""

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from pizza.common.envelope import (
    ComparisonResult,
    KpiSnapshot,
    OrderLifecycle,
    RoundClose,
    RunParams,
    RunPause,
    RunPaused,
    RunReset,
    RunResume,
    RunStarted,
    SetArrivalRate,
    SetRecipe,
    encode,
)
from pizza.common.recipes import load_all_recipes
from pizza.common.topology import (
    Q_KPI_CONTROL,
    TOPIC_KPI_SNAPSHOTS,
    TOPIC_ORDER_LIFECYCLE,
    TOPIC_STATION_EVENTS,
    TOPIC_TIMESCALE,
)
from pizza.kpi import service
from pizza.kpi.service import Kpi
from pizza.kpi.settings import KpiSettings

T0 = datetime(2026, 1, 1, tzinfo=UTC)
RECIPES = load_all_recipes()


def _settings(**kw) -> SimpleNamespace:
    base = {
        "warmup_orders": 10,
        "little_tolerance": 0.10,
        "bottleneck_margin": 0.05,
        "bottleneck_confirm_n": 3,
        "bottleneck_min_util": 0.80,
        "snapshot_interval_s": 0.01,
        "kafka_group_id": "kpi",
        "kafka_bootstrap": "x:9092",
        "replay_timeout_s": 1.0,
        "nack_delay_s": 0,
        "resume_on_start": True,
        "round_close_wait_s": 0.1,  # Fast polling for tests
    }
    base.update(kw)
    return SimpleNamespace(**base)


class Env:
    def __init__(self, monkeypatch) -> None:
        self.published: list[tuple] = []
        self.calls: list[str] = []
        self.fail_publish = 0
        self.dead: list = []
        monkeypatch.setattr(service, "produce_fact", self._produce)
        monkeypatch.setattr(service, "dead_letter_invalid", self._dead)

    def _produce(self, producer, topic, fact, key):
        if self.fail_publish > 0:
            self.fail_publish -= 1
            raise RuntimeError("kafka down")
        self.published.append((topic, fact, key))
        self.calls.append("publish")

    async def _dead(self, channel, message, reason, queue):
        self.dead.append((reason, queue))

    def kpi(self, **kw) -> Kpi:
        return Kpi(object(), _settings(**kw), RECIPES, channel=MagicMock())

    @staticmethod
    def msg(body: bytes) -> MagicMock:
        message = MagicMock()
        message.body = body
        message.ack = AsyncMock()
        message.nack = AsyncMock()
        return message


def _life(order_id: str, state: str, run_id: str, ts=T0, created_ts=None) -> OrderLifecycle:
    return OrderLifecycle.make(
        run_id=run_id,
        machine_id="scheduler",
        ts=ts,
        state=state,
        order_id=order_id,
        product="margherita",
        qty=1,
        due_ts=T0 + timedelta(hours=1),
        created_ts=created_ts or T0,
    )


async def test_recover_folds_only_newest_run(monkeypatch) -> None:
    k = Env(monkeypatch).kpi()
    facts = [
        _life("old", "released", "r1", T0),
        _life("a", "released", "r2", T0 + timedelta(seconds=5)),
        _life("b", "released", "r2", T0 + timedelta(seconds=6)),
    ]
    await k.recover(facts)
    assert k.state.run_id == "r2"
    assert set(k.state.orders) == {"a", "b"}
    assert k.state.weights_source == "observed"


async def test_recover_without_facts_keeps_no_run(monkeypatch) -> None:
    k = Env(monkeypatch).kpi()
    await k.recover([])
    assert k.state.run_id is None


async def test_on_event_folds_and_swallows_errors(monkeypatch) -> None:
    k = Env(monkeypatch).kpi()
    await k.recover([_life("a", "released", "r1")])
    await k.on_event(_life("b", "released", "r1", T0 + timedelta(seconds=1)))
    assert set(k.state.orders) == {"a", "b"}
    await k.on_event(_life("z", "released", "other"))
    assert "z" not in k.state.orders  # foreign released ignored by fold (CR-01)

    def boom(state, msg):
        raise RuntimeError("bad")

    monkeypatch.setattr(service, "fold", boom)
    await k.on_event(_life("c", "released", "r1"))  # must not raise


async def test_on_event_adopts_newer_run_created(monkeypatch) -> None:
    """CR-01: a newer run's created event reaches fold() and is adopted."""
    k = Env(monkeypatch).kpi()
    await k.recover([_life("a", "released", "r1")])
    later = T0 + timedelta(seconds=60)
    await k.on_event(_life("n", "created", "r2", ts=later, created_ts=later))
    assert k.state.run_id == "r2"
    assert set(k.state.orders) == {"n"}


async def test_on_event_rejects_stale_foreign_event(monkeypatch) -> None:
    """CR-01: stale/foreign-run events are declined by fold(); state unchanged."""
    k = Env(monkeypatch).kpi()
    await k.recover([_life("a", "released", "r1", T0 + timedelta(seconds=10))])
    await k.on_event(_life("x", "released", "other", T0 + timedelta(seconds=11)))
    await k.on_event(_life("y", "created", "other", T0 + timedelta(seconds=12), created_ts=T0))
    assert k.state.run_id == "r1"
    assert set(k.state.orders) == {"a"}


async def test_on_event_adopts_run_when_none(monkeypatch) -> None:
    """CR-01: with no run yet, a created event adopts its run."""
    k = Env(monkeypatch).kpi()
    assert k.state.run_id is None
    await k.on_event(_life("a", "created", "r1"))
    assert k.state.run_id == "r1"
    assert "a" in k.state.orders


async def test_control_run_started_resets_and_acks(monkeypatch) -> None:
    env = Env(monkeypatch)
    k = env.kpi()
    await k.recover([_life("a", "released", "r1")])
    m = env.msg(encode(RunStarted.make(run_id="r2", machine_id="cli")))
    await k.handle_control(m)
    m.ack.assert_awaited_once()
    assert k.state.run_id == "r2"
    assert k.state.orders == {}
    await k.tick()
    topic, snap, key = env.published[-1]
    assert topic == TOPIC_KPI_SNAPSHOTS
    assert key == "r2"
    assert snap.run_id == "r2"
    assert snap.warming_up is True


async def test_control_invalid_and_wrong_type_dead_lettered(monkeypatch) -> None:
    env = Env(monkeypatch)
    k = env.kpi()
    bad = env.msg(b"not json")
    await k.handle_control(bad)
    wrong = env.msg(encode(_life("a", "released", "r1")))
    await k.handle_control(wrong)
    assert len(env.dead) == 2
    assert all(q == Q_KPI_CONTROL for _, q in env.dead)
    bad.ack.assert_not_awaited()
    wrong.ack.assert_not_awaited()


async def test_control_unexpected_exception_nacks(monkeypatch) -> None:
    env = Env(monkeypatch)
    k = env.kpi()

    def boom(state, msg):
        raise RuntimeError("bad")

    monkeypatch.setattr(service, "fold", boom)
    m = env.msg(encode(RunStarted.make(run_id="r2", machine_id="cli")))
    await k.handle_control(m)
    m.nack.assert_awaited_once_with(requeue=True)
    m.ack.assert_not_awaited()


async def test_tick_no_publish_while_run_none(monkeypatch) -> None:
    env = Env(monkeypatch)
    k = env.kpi()
    await k.tick()
    assert env.published == []


async def test_tick_publishes_keyed_snapshot(monkeypatch) -> None:
    env = Env(monkeypatch)
    k = env.kpi()
    await k.recover([_life("a", "released", "r1")])
    await k.tick()
    assert len(env.published) == 1
    topic, snap, key = env.published[0]
    assert (topic, key, snap.run_id) == (TOPIC_KPI_SNAPSHOTS, "r1", "r1")


async def test_publish_failure_not_fatal_and_ticker_survives(monkeypatch) -> None:
    env = Env(monkeypatch)
    k = env.kpi()
    await k.recover([_life("a", "released", "r1")])
    env.fail_publish = 2
    await k.tick()  # swallowed
    task = asyncio.create_task(k._tick_loop())
    for _ in range(200):
        if env.published:
            break
        await asyncio.sleep(0.01)
    k.stop()
    task.cancel()
    assert not task.done() or task.cancelled()
    assert env.published


async def test_run_replays_before_consuming_and_tails_both_topics(monkeypatch) -> None:
    env = Env(monkeypatch)
    order: list[str] = []

    async def fake_verify(conn):
        order.append("verify")

    monkeypatch.setattr(service, "verify_rabbit", fake_verify)
    monkeypatch.setattr(service, "verify_kafka", lambda admin: None)
    monkeypatch.setattr(service, "AdminClient", lambda cfg: object())

    def fake_replay(bootstrap, topics, timeout, group):
        assert group == "kpi"
        if topics == [TOPIC_KPI_SNAPSHOTS]:
            order.append("replay_snapshots")
            return [], {(TOPIC_KPI_SNAPSHOTS, 0): 99}
        order.append("replay")
        assert topics == [TOPIC_STATION_EVENTS, TOPIC_ORDER_LIFECYCLE, TOPIC_TIMESCALE]
        return [_life("a", "released", "r9")], {(TOPIC_STATION_EVENTS, 0): 3}

    monkeypatch.setattr(service, "replay_topics", fake_replay)
    tails: list = []

    class FakeTail:
        def __init__(self, *a, **k) -> None:
            tails.append((a, k))

        def start(self) -> None:
            order.append("tail_start")

        def stop(self) -> None:
            order.append("tail_stop")

    monkeypatch.setattr(service, "KafkaTail", FakeTail)

    queue = MagicMock()

    async def consume(cb, no_ack=False):
        order.append("consume")
        return "tag"

    queue.consume = consume
    queue.cancel = AsyncMock()
    channel = MagicMock()
    channel.get_queue = AsyncMock(return_value=queue)
    channel.close = AsyncMock()
    conn = MagicMock()
    conn.channel = AsyncMock(return_value=channel)

    k = env.kpi()
    runner = asyncio.create_task(k.run(conn))
    for _ in range(200):
        if "tail_start" in order:
            break
        await asyncio.sleep(0.01)
    k.stop()
    await asyncio.wait_for(runner, 5)

    assert order.index("replay_snapshots") < order.index("replay")
    assert order.index("replay") < order.index("consume") < order.index("tail_start")
    assert k.state.run_id == "r9"
    args, kwargs = tails[0]
    assert args[3] == [TOPIC_STATION_EVENTS, TOPIC_ORDER_LIFECYCLE, TOPIC_TIMESCALE]
    assert args[2] == {(TOPIC_STATION_EVENTS, 0): 3}  # snapshot watermarks excluded
    assert kwargs["thread_name"] == "kpi-tail"
    assert "tail_stop" in order


def test_settings_defaults_and_validation() -> None:
    s = KpiSettings()
    assert (s.warmup_orders, s.little_tolerance, s.bottleneck_margin) == (10, 0.10, 0.05)
    assert (s.bottleneck_confirm_n, s.bottleneck_min_util) == (3, 0.80)
    assert s.snapshot_interval_s == 1.0
    assert s.kafka_group_id == "kpi"
    assert s.replay_timeout_s == 60.0
    assert s.nack_delay_s == 1.0
    assert s.resume_on_start is True
    assert s.recipes_dir is None
    with pytest.raises(ValidationError):
        KpiSettings(bottleneck_confirm_n=0)
    with pytest.raises(ValidationError):
        KpiSettings(bottleneck_min_util=1.5)
    with pytest.raises(ValidationError):
        KpiSettings(little_tolerance=-0.1)
    with pytest.raises(ValidationError):
        KpiSettings(snapshot_interval_s=-1)


def _run_started(run_id: str, weights: dict[str, float]) -> RunStarted:
    return RunStarted.make(
        run_id=run_id, machine_id="cli", ts=T0, params=RunParams(weights=weights)
    )


async def test_control_publishes_snapshot_before_ack(monkeypatch) -> None:
    env = Env(monkeypatch)
    k = env.kpi()
    m = env.msg(encode(_run_started("r2", {"margherita": 3.0, "pepperoni": 1.0, "veggie": 1.0})))
    m.ack.side_effect = lambda: env.calls.append("ack")
    await k.handle_control(m)
    assert env.calls == ["publish", "ack"]
    topic, snap, key = env.published[0]
    assert (topic, key) == (TOPIC_KPI_SNAPSHOTS, "r2")
    assert snap.run_weights == {"margherita": 3.0, "pepperoni": 1.0, "veggie": 1.0}
    assert snap.run_started_ts == T0


async def test_control_publish_failure_nacks_not_acks(monkeypatch) -> None:
    env = Env(monkeypatch)
    env.fail_publish = 1
    k = env.kpi()
    m = env.msg(encode(_run_started("r2", {"margherita": 1.0})))
    await k.handle_control(m)
    m.ack.assert_not_awaited()
    m.nack.assert_awaited_once_with(requeue=True)


async def test_recover_restores_params_from_latest_snapshot(monkeypatch) -> None:
    env = Env(monkeypatch)
    k = env.kpi()
    facts = [_life("a", "released", "r2", T0 + timedelta(seconds=5))]
    k0 = env.kpi()
    await k0.handle_control(env.msg(encode(_run_started("r2", {"margherita": 2.0}))))
    old = env.published[-1][1]
    k1 = env.kpi()
    await k1.handle_control(env.msg(encode(_run_started("r2", {"veggie": 5.0}))))
    newer = env.published[-1][1]
    other = KpiSnapshot.model_validate(
        {**newer.model_dump(mode="json"), "run_id": "r1", "run_weights": {"x": 9.0}}
    )
    plain = k.state  # noqa: F841
    no_params = KpiSnapshot.model_validate(
        {**newer.model_dump(mode="json"), "run_weights": None, "run_started_ts": None}
    )
    assert old.ts <= newer.ts
    await k.recover(facts, snapshots=[newer, old, other, no_params])
    assert k.state.weights == {"veggie": 5.0}
    assert k.state.weights_source == "run_started"
    assert k.state.run_started_ts == T0


async def test_recover_without_params_snapshots_stays_observed(monkeypatch) -> None:
    env = Env(monkeypatch)
    k = env.kpi()
    await k.recover([_life("a", "released", "r2")], snapshots=[])
    assert k.state.weights_source == "observed"


async def test_theory_identical_across_recover(monkeypatch) -> None:
    env = Env(monkeypatch)
    k = env.kpi()
    weights = {"margherita": 3.0, "pepperoni": 1.0, "veggie": 1.0}
    await k.handle_control(env.msg(encode(_run_started("r2", weights))))
    facts = [
        _life(
            "a", "created", "r2", T0 + timedelta(seconds=1), created_ts=T0 + timedelta(seconds=1)
        ),
        _life("b", "released", "r2", T0 + timedelta(seconds=2)),
    ]
    for f in facts:
        await k.on_event(f)
    await k.tick()
    s1 = env.published[-1][1]
    assert s1.theory.weights_source == "run_started"

    k2 = env.kpi()
    await k2.recover(facts, snapshots=[s1])
    await k2.tick()
    s2 = env.published[-1][1]
    assert s2.theory == s1.theory
    assert s2.run_weights == s1.run_weights

    # Control: without snapshots the theory block silently degrades (the CR-02 bug).
    k3 = env.kpi()
    await k3.recover(facts)
    await k3.tick()
    assert env.published[-1][1].theory.weights_source == "observed"


async def test_control_reset_clears_state_and_stops_publishing(monkeypatch) -> None:
    env = Env(monkeypatch)
    k = env.kpi()
    await k.recover([_life("a", "released", "r1")])
    m = env.msg(encode(RunReset.make(run_id="r1", machine_id="cli")))
    await k.handle_control(m)
    m.ack.assert_awaited_once()
    assert k.state.run_id is None
    assert k.state.orders == {}
    await k.tick()
    assert env.published == []
    assert env.dead == []


async def test_control_reset_then_start_adopts_new_run(monkeypatch) -> None:
    env = Env(monkeypatch)
    k = env.kpi()
    await k.recover([_life("a", "released", "r1")])
    await k.handle_control(env.msg(encode(RunReset.make(run_id="r1", machine_id="cli"))))
    m = env.msg(encode(RunStarted.make(run_id="r2", machine_id="cli")))
    await k.handle_control(m)
    m.ack.assert_awaited_once()
    assert k.state.run_id == "r2"
    assert env.published[-1][1].run_id == "r2"


async def test_control_irrelevant_commands_acked_not_dead_lettered(monkeypatch) -> None:
    env = Env(monkeypatch)
    k = env.kpi()
    await k.recover([_life("a", "released", "r1")])
    cmds = [
        RunPause.make(run_id="r1", machine_id="cli"),
        RunResume.make(run_id="r1", machine_id="cli"),
        SetArrivalRate.make(run_id="r1", machine_id="cli", arrival_rate=1.0),
    ]
    for c in cmds:
        m = env.msg(encode(c))
        await k.handle_control(m)
        m.ack.assert_awaited_once()
    assert env.dead == []
    assert k.state.run_id == "r1"


async def test_control_set_recipe_updates_theory_table(monkeypatch) -> None:
    env = Env(monkeypatch)
    k = env.kpi()
    before = k.recipes["bake"]["margherita"]
    m = env.msg(
        encode(
            SetRecipe.make(
                run_id="r1",
                machine_id="kpi-service",
                station="bake",
                product="margherita",
                unit_time_s=99.0,
            )
        )
    )
    await k.handle_control(m)
    m.ack.assert_awaited_once()
    assert k.recipes["bake"]["margherita"] == 99.0
    # The service owns a copy; the caller-supplied table is untouched.
    assert RECIPES["bake"]["margherita"] == before != 99.0
    assert env.dead == []


async def test_control_job_command_still_dead_lettered(monkeypatch) -> None:
    env = Env(monkeypatch)
    k = env.kpi()
    m = env.msg(encode(_life("a", "released", "r1")))
    await k.handle_control(m)
    assert len(env.dead) == 1
    m.ack.assert_not_awaited()


async def test_on_event_folds_pause_facts(monkeypatch) -> None:
    k = Env(monkeypatch).kpi()
    await k.recover([_life("a", "released", "r1")])
    await k.on_event(RunPaused.make(run_id="r1", machine_id="scheduler", ts=T0))
    assert len(k.state.pause_marks) == 1


# ==============================================================================
# Task 1: RoundClose handling and run_params storage


async def test_run_params_stored_in_snapshot_after_run_started(monkeypatch) -> None:
    """Test 1: after RunStarted with params, next snapshot carries those params."""
    env = Env(monkeypatch)
    k = env.kpi()
    params = RunParams(
        seed=7,
        k=1.5,
        max_orders=3,
        rule="edd",
        comparison_id="c-t-1",
        round=2,
    )
    m = env.msg(encode(RunStarted.make(run_id="r1", machine_id="cli", params=params)))
    await k.handle_control(m)
    m.ack.assert_awaited_once()
    # Next snapshot should carry run_params
    await k.tick()
    topic, snap, key = env.published[-1]
    assert snap.run_params == params
    # After RunReset, no stale params
    await k.handle_control(env.msg(encode(RunReset.make(run_id="r1", machine_id="cli"))))
    await k.tick()
    # No snapshot published when no run


async def test_recover_restores_run_params_from_snapshot(monkeypatch) -> None:
    """Test 2: recover restores run_params from newest snapshot carrier."""
    env = Env(monkeypatch)
    k = env.kpi()
    params = RunParams(seed=7, k=1.5, max_orders=3, comparison_id="c-t-1", round=2)
    m = env.msg(encode(RunStarted.make(run_id="r1", machine_id="cli", params=params)))
    await k.handle_control(m)
    snap1 = env.published[0][1]  # snapshot with params

    k2 = env.kpi()
    facts = [_life("a", "released", "r1")]
    await k2.recover(facts, snapshots=[snap1])
    # run_params should be restored
    assert k2.run_params == params


async def test_round_close_publishes_comparison_result(monkeypatch) -> None:
    """Test 3: RoundClose publishes ComparisonResult with correct data."""
    env = Env(monkeypatch)
    k = env.kpi()
    params = RunParams(
        seed=7,
        k=1.5,
        max_orders=1,
        rule="edd",
        arrival_rate=1.0 / 35.0,
        comparison_id="c-t-1",
        round=2,
        random_breakdowns=True,
    )
    # Start run with params
    await k.handle_control(
        env.msg(encode(RunStarted.make(run_id="r1", machine_id="cli", params=params)))
    )
    # Add completed order
    await k.on_event(_life("o1", "created", "r1", T0 + timedelta(seconds=1)))
    await k.on_event(_life("o1", "released", "r1", T0 + timedelta(seconds=2)))
    await k.on_event(_life("o1", "completed", "r1", T0 + timedelta(seconds=5)))
    # RoundClose
    round_close = RoundClose.make(run_id="r1", machine_id="dashboard", partial=False)
    await k.handle_control(env.msg(encode(round_close)))
    # Find ComparisonResult in published
    results = [f for _, f, _ in env.published if isinstance(f, ComparisonResult)]
    assert len(results) > 0
    result = results[0]
    assert result.comparison_id == "c-t-1"
    assert result.round == 2
    assert result.rule == "edd"
    assert result.seed == 7
    assert result.k == 1.5
    assert result.random_breakdowns is True
    assert result.partial is False
    assert result.n_completed == 1


async def test_round_close_failure_nacks(monkeypatch) -> None:
    """Test 4: if producer raises, message is nacked after delay."""
    env = Env(monkeypatch)
    k = env.kpi()
    params = RunParams(
        seed=7,
        comparison_id="c-t-1",
        round=2,
        max_orders=1,
    )
    await k.handle_control(
        env.msg(encode(RunStarted.make(run_id="r1", machine_id="cli", params=params)))
    )
    await k.on_event(_life("o1", "created", "r1", T0))
    await k.on_event(_life("o1", "completed", "r1", T0 + timedelta(seconds=1)))
    # Make next publish fail
    env.fail_publish = 1
    round_close_msg = env.msg(encode(RoundClose.make(run_id="r1", machine_id="dashboard")))
    await k.handle_control(round_close_msg)
    round_close_msg.nack.assert_awaited_once_with(requeue=True)
    round_close_msg.ack.assert_not_awaited()


async def test_round_close_waits_for_orders_to_complete(monkeypatch) -> None:
    """Test 5: partial=False waits for orders; partial=True never waits."""
    env = Env(monkeypatch)
    k = env.kpi()
    params = RunParams(
        seed=7,
        comparison_id="c-t-1",
        round=2,
        max_orders=2,
    )
    await k.handle_control(
        env.msg(encode(RunStarted.make(run_id="r1", machine_id="cli", params=params)))
    )
    await k.on_event(_life("o1", "created", "r1", T0))
    await k.on_event(_life("o1", "completed", "r1", T0 + timedelta(seconds=1)))
    # Only 1/2 orders completed
    round_close = RoundClose.make(run_id="r1", machine_id="dashboard", partial=False)
    m = env.msg(encode(round_close))
    # Add callback to complete order during handling
    async def finish_later():
        await asyncio.sleep(0.01)
        await k.on_event(_life("o2", "created", "r1", T0 + timedelta(seconds=2)))
        await k.on_event(_life("o2", "completed", "r1", T0 + timedelta(seconds=3)))

    task = asyncio.create_task(finish_later())
    await k.handle_control(m)
    await task
    m.ack.assert_awaited_once()


async def test_round_close_ignores_wrong_run(monkeypatch) -> None:
    """Test 6: RoundClose for wrong run or without comparison_id is ignored."""
    env = Env(monkeypatch)
    k = env.kpi()
    params = RunParams(seed=7, comparison_id="c-t-1", round=2)
    await k.handle_control(
        env.msg(encode(RunStarted.make(run_id="r1", machine_id="cli", params=params)))
    )
    count_before = len(env.published)
    # RoundClose for different run
    round_close = RoundClose.make(run_id="r2", machine_id="dashboard")
    await k.handle_control(env.msg(encode(round_close)))
    # No new ComparisonResult published
    results = [f for _, f, _ in env.published[count_before:] if isinstance(f, ComparisonResult)]
    assert len(results) == 0


async def test_round_close_with_no_comparison_id_ignored(monkeypatch) -> None:
    """Test 6b: RoundClose without comparison_id in params is ignored."""
    env = Env(monkeypatch)
    k = env.kpi()
    params = RunParams(seed=7)  # No comparison_id
    await k.handle_control(
        env.msg(encode(RunStarted.make(run_id="r1", machine_id="cli", params=params)))
    )
    count_before = len(env.published)
    # RoundClose
    round_close = RoundClose.make(run_id="r1", machine_id="dashboard")
    await k.handle_control(env.msg(encode(round_close)))
    # No new ComparisonResult published
    results = [f for _, f, _ in env.published[count_before:] if isinstance(f, ComparisonResult)]
    assert len(results) == 0
