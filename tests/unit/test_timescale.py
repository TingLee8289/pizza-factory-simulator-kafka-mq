"""Unit tests for TimeScale, TimeScaleFeed and TimeScaleChanged (quick-et6)."""

import asyncio
import time

import pytest
from pydantic import ValidationError

from pizza.common import timescale as ts_mod
from pizza.common.envelope import InvalidEnvelope, TimeScaleChanged, encode, parse_message
from pizza.common.timescale import ALLOWED_SCALES, TimeScale, TimeScaleFeed
from pizza.common.topology import KAFKA_TOPICS, TOPIC_TIMESCALE


class FakeClock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def test_default_scale_and_piecewise_integral():
    clock = FakeClock()
    ts = TimeScale(clock=clock)
    assert ts.scale == 1.0
    clock.now += 10
    assert ts.sim_now() == pytest.approx(10)
    ts.set(2)
    clock.now += 10
    assert ts.sim_now() == pytest.approx(30)  # earlier increment not rewritten
    ts.set(8)
    clock.now += 5
    assert ts.sim_now() == pytest.approx(70)
    # lag = wall - sim: 0 at 1x, -10 over the 2x leg, -35 over the 8x leg
    assert ts.wall_lag() == pytest.approx(-45)


def test_wall_lag_is_zero_at_constant_1x():
    clock = FakeClock()
    ts = TimeScale(clock=clock)
    clock.now += 123.4
    assert ts.wall_lag() == 0.0


def test_set_rejects_invalid_and_notifies():
    ts = TimeScale()
    seen: list[float] = []
    ts.on_change(seen.append)
    for bad in (0, -1, float("nan"), float("inf")):
        with pytest.raises(ValueError):
            ts.set(bad)
    ts.set(4)
    ts.set(4)  # unchanged: no second notification
    assert seen == [4.0]


async def _elapsed(coro) -> float:
    t0 = time.monotonic()
    await coro
    return time.monotonic() - t0


async def test_sleep_is_divided_by_scale():
    ts = TimeScale()
    ts.set(4)
    elapsed = await _elapsed(ts.sleep(0.8))
    assert 0.15 <= elapsed <= 0.4


async def test_sleep_rescales_mid_flight():
    ts = TimeScale()
    task = asyncio.create_task(_elapsed(ts.sleep(2.0)))
    await asyncio.sleep(0.2)  # 0.2 sim s consumed at 1x
    ts.set(8)  # remaining ~1.8 sim s -> ~0.11 s wall
    elapsed = await asyncio.wait_for(task, 2.0)
    assert 0.25 <= elapsed <= 0.8


async def test_sleep_zero_returns_immediately():
    await asyncio.wait_for(TimeScale().sleep(0), 0.5)


@pytest.mark.parametrize("scale", ALLOWED_SCALES)
def test_envelope_accepts_allowed(scale):
    msg = TimeScaleChanged.make(run_id="unknown", machine_id="dashboard", scale=scale)
    parsed = parse_message(encode(msg))
    assert isinstance(parsed, TimeScaleChanged) and parsed.scale == scale


@pytest.mark.parametrize("scale", [0, -1, 3, 32, float("nan"), float("inf")])
def test_envelope_rejects_invalid(scale):
    with pytest.raises(ValidationError):
        TimeScaleChanged.make(run_id="unknown", machine_id="dashboard", scale=scale)


def test_parse_ignores_unknown_fields_and_rejects_bad_scale():
    msg = TimeScaleChanged.make(run_id="unknown", machine_id="dashboard", scale=8)
    import json

    raw = json.loads(encode(msg))
    raw["future_field"] = 1
    assert parse_message(json.dumps(raw).encode()).scale == 8  # type: ignore[union-attr]
    raw["scale"] = 5
    with pytest.raises(InvalidEnvelope):
        parse_message(json.dumps(raw).encode())


def test_topic_is_delete_policy():
    spec = next(t for t in KAFKA_TOPICS if t.name == TOPIC_TIMESCALE)
    assert spec.config["cleanup.policy"] == "delete"


class _FakeTail:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.started = False
        self.stopped = False

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True


async def test_feed_replays_last_value_then_tails(monkeypatch):
    monkeypatch.setattr(ts_mod, "FEED_ENABLED", True)
    ts = TimeScale()
    msgs = [
        TimeScaleChanged.make(run_id="unknown", machine_id="dashboard", scale=2),
        TimeScaleChanged.make(run_id="unknown", machine_id="dashboard", scale=8),
    ]
    tails: list[_FakeTail] = []

    def tail_factory(*a, **k):
        tails.append(_FakeTail(*a, **k))
        return tails[-1]

    feed = TimeScaleFeed(
        "x:9092",
        "svc",
        ts,
        replay=lambda *a: (msgs, {(TOPIC_TIMESCALE, 0): 2}),
        tail_factory=tail_factory,
    )
    await feed.start()
    assert ts.scale == 8.0
    assert tails[0].started and tails[0].args[1] == "timescale-svc"
    await tails[0].args[4](TimeScaleChanged.make(run_id="unknown", machine_id="d", scale=8))
    assert ts.scale == 8.0
    await feed.stop()
    assert tails[0].stopped


async def test_feed_failure_falls_back_to_1x(monkeypatch):
    monkeypatch.setattr(ts_mod, "FEED_ENABLED", True)
    ts = TimeScale()

    def boom(*a):
        raise TimeoutError("no kafka")

    feed = TimeScaleFeed("x:9092", "svc", ts, replay=boom, tail_factory=_FakeTail)
    await feed.start()  # must not raise
    assert ts.scale == 1.0
    await feed.stop()
