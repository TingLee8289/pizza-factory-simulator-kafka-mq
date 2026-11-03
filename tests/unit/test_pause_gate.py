"""Unit tests for PauseGate (Phase 5 D-04)."""

import asyncio
import time

import pytest

from pizza.common.pause import GateAborted, PauseGate

TOL = 0.05


async def _timed(coro) -> float:
    t0 = time.monotonic()
    await coro
    return time.monotonic() - t0


@pytest.mark.asyncio
async def test_sleep_without_pause():
    elapsed = await _timed(PauseGate().sleep(0.3))
    assert 0.3 - TOL <= elapsed <= 0.3 + 0.1


@pytest.mark.asyncio
async def test_pause_preserves_remaining_time():
    gate = PauseGate()
    task = asyncio.create_task(_timed(gate.sleep(0.3)))
    await asyncio.sleep(0.1)
    gate.pause()
    await asyncio.sleep(0.4)
    gate.resume()
    elapsed = await task
    assert 0.7 - TOL <= elapsed <= 0.7 + 0.1


def test_reasons_combine_and_set_semantics():
    async def run():
        gate = PauseGate()
        gate.pause("run")
        gate.pause("breakdown")
        gate.resume("run")
        assert gate.is_paused
        gate.resume("breakdown")
        assert not gate.is_paused
        gate.pause()
        gate.pause()
        gate.resume()
        assert not gate.is_paused

    asyncio.run(run())


@pytest.mark.asyncio
async def test_wait_running_immediate_and_blocking():
    gate = PauseGate()
    assert await _timed(gate.wait_running()) < TOL
    gate.pause()
    task = asyncio.create_task(gate.wait_running())
    await asyncio.sleep(0.1)
    assert not task.done()
    gate.resume()
    await asyncio.wait_for(task, 1)


@pytest.mark.asyncio
async def test_abort_releases_sleep_and_wait_running():
    gate = PauseGate()
    sleeper = asyncio.create_task(gate.sleep(5))
    await asyncio.sleep(0.05)
    gate.abort()
    with pytest.raises(GateAborted):
        await asyncio.wait_for(sleeper, 1)

    gate.pause()
    waiter = asyncio.create_task(gate.wait_running())
    await asyncio.sleep(0.05)
    gate.abort()
    with pytest.raises(GateAborted):
        await asyncio.wait_for(waiter, 1)


@pytest.mark.asyncio
async def test_sleep_works_after_clear():
    gate = PauseGate()
    gate.pause()
    gate.clear()
    assert not gate.is_paused
    assert await _timed(gate.sleep(0.1)) >= 0.1 - TOL


@pytest.mark.asyncio
async def test_sleep_after_abort_works_in_new_epoch():
    gate = PauseGate()
    gate.abort()
    assert await _timed(gate.sleep(0.1)) >= 0.1 - TOL


def test_paused_seconds_with_fake_clock():
    async def run():
        now = [100.0]
        gate = PauseGate(clock=lambda: now[0])
        assert gate.paused_seconds() == 0.0
        gate.pause()
        now[0] += 2.0
        assert gate.paused_seconds() == 2.0
        gate.resume()
        now[0] += 5.0
        assert gate.paused_seconds() == 2.0
        gate.pause()
        now[0] += 1.5
        assert gate.paused_seconds() == 3.5

    asyncio.run(run())


# -- quick-et6: scaled sleeps ---------------------------------------------------
from pizza.common.timescale import TimeScale  # noqa: E402


@pytest.mark.asyncio
async def test_timescale_sleep_is_scaled():
    ts = TimeScale()
    ts.set(4)
    elapsed = await _timed(PauseGate(timescale=ts).sleep(0.8))
    assert 0.15 <= elapsed <= 0.4


@pytest.mark.asyncio
async def test_timescale_pause_preserves_remaining_sim_time():
    ts = TimeScale()
    ts.set(2)
    gate = PauseGate(timescale=ts)
    task = asyncio.create_task(_timed(gate.sleep(0.6)))  # 0.3 s wall at 2x
    await asyncio.sleep(0.1)  # 0.2 sim s consumed, 0.4 remain
    gate.pause()
    await asyncio.sleep(0.3)
    gate.resume()
    elapsed = await task
    assert 0.6 - TOL <= elapsed <= 0.6 + 0.15  # 0.1 + 0.3 paused + 0.2 remaining wall


@pytest.mark.asyncio
async def test_timescale_change_mid_sleep_rescales():
    ts = TimeScale()
    gate = PauseGate(timescale=ts)
    task = asyncio.create_task(_timed(gate.sleep(2.0)))
    await asyncio.sleep(0.2)
    ts.set(16)
    elapsed = await asyncio.wait_for(task, 2.0)
    assert 0.25 <= elapsed <= 0.8


@pytest.mark.asyncio
async def test_timescale_abort_raises():
    ts = TimeScale()
    gate = PauseGate(timescale=ts)
    task = asyncio.create_task(gate.sleep(5.0))
    await asyncio.sleep(0.05)
    gate.abort()
    with pytest.raises(GateAborted):
        await asyncio.wait_for(task, 1.0)


def test_paused_seconds_uses_sim_clock_with_timescale():
    clock = [0.0]
    ts = TimeScale(clock=lambda: clock[0])
    gate = PauseGate(timescale=ts)
    ts.set(8)
    gate.pause()
    clock[0] += 2.0
    assert gate.paused_seconds() == pytest.approx(16.0)
