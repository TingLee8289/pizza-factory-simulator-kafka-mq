"""Simulation time scale: sim clock, scaled sleep and the Kafka feed (quick-et6).

"Simulation seconds" are the unit of every duration in the system (recipe unit
times, MTBF/MTTR, repair, due lead, bank thresholds). Only the *real wait* is
divided by the scale. ``TimeScale.sleep`` wakes every sleeper when the scale
changes and recomputes the remaining simulated time from the elapsed wall time
at the previous scale, so a switch takes effect immediately.

Services learn the current scale from the ``sim.timescale`` Kafka topic through
``TimeScaleFeed`` (replay the last value, then tail). The topic is kept with
``cleanup.policy=delete`` so the KPI service can replay the whole scale history.
"""

import asyncio
import math
import time
from collections.abc import Callable
from typing import Any

import structlog

from pizza.common.envelope import TimeScaleChanged
from pizza.common.topology import TOPIC_TIMESCALE

_log = structlog.get_logger()

ALLOWED_SCALES: tuple[int, ...] = (1, 2, 4, 8)

# Tests flip this off so unit tests never open a real Kafka consumer.
FEED_ENABLED = True


class TimeScale:
    """Piecewise-integrated simulation clock with a live-changeable rate."""

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._scale = 1.0
        self._last = clock()
        self._sim = 0.0
        self._lag = 0.0
        self._changed = asyncio.Event()
        self._callbacks: list[Callable[[float], None]] = []

    @property
    def scale(self) -> float:
        return self._scale

    @property
    def changed_event(self) -> asyncio.Event:
        """Event that is set (and replaced) on every ``set``; grab it before waiting."""
        return self._changed

    def on_change(self, callback: Callable[[float], None]) -> None:
        self._callbacks.append(callback)

    def _advance(self) -> float:
        now = self._clock()
        dt = max(0.0, now - self._last)
        self._sim += dt * self._scale
        self._lag += dt * (1.0 - self._scale)
        self._last = now
        return now

    def set(self, scale: float) -> None:
        """Change the scale (call inside the event loop); wakes all sleepers."""
        if not math.isfinite(scale) or scale <= 0:
            raise ValueError(f"invalid time scale {scale!r}")
        self._advance()
        if scale == self._scale:
            return
        self._scale = float(scale)
        old, self._changed = self._changed, asyncio.Event()
        old.set()
        for callback in self._callbacks:
            callback(self._scale)

    def sim_now(self) -> float:
        """Simulated seconds since construction (monotonic, piecewise integral)."""
        self._advance()
        return self._sim

    def wall_lag(self) -> float:
        """Integral of (1 - scale) dt: wall elapsed minus sim elapsed; 0 at constant 1x."""
        self._advance()
        return self._lag

    async def sleep(self, sim_s: float) -> None:
        """Sleep ``sim_s`` simulated seconds, re-scaling when the scale changes."""
        remaining = float(sim_s)
        while remaining > 0:
            changed = self._changed
            scale = self._scale
            started = self._clock()
            try:
                await asyncio.wait_for(changed.wait(), timeout=remaining / scale)
            except TimeoutError:
                return
            remaining -= max(0.0, self._clock() - started) * scale


class TimeScaleFeed:
    """Keep a ``TimeScale`` in sync with the ``sim.timescale`` Kafka topic."""

    def __init__(
        self,
        bootstrap: str,
        service: str,
        timescale: TimeScale,
        *,
        replay_timeout_s: float = 15.0,
        replay: Callable[..., Any] | None = None,
        tail_factory: Callable[..., Any] | None = None,
    ) -> None:
        self._bootstrap = bootstrap
        self._service = service
        self._timescale = timescale
        self._replay_timeout_s = replay_timeout_s
        self._replay = replay
        self._tail_factory = tail_factory
        self._tail: Any = None

    async def _on_message(self, msg: Any) -> None:
        if isinstance(msg, TimeScaleChanged):
            self._timescale.set(msg.scale)

    async def start(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        """Replay the last scale then tail; never raises (falls back to 1x)."""
        if not FEED_ENABLED:
            return
        try:
            from pizza.scheduler.replay import KafkaTail, replay_topics

            replay = self._replay or replay_topics
            tail_factory = self._tail_factory or KafkaTail
            loop = loop or asyncio.get_running_loop()
            group = f"timescale-{self._service}"
            messages, end_offsets = await asyncio.to_thread(
                replay, self._bootstrap, [TOPIC_TIMESCALE], self._replay_timeout_s, group
            )
            for msg in messages:
                await self._on_message(msg)
            self._tail = tail_factory(
                self._bootstrap,
                group,
                end_offsets,
                TOPIC_TIMESCALE,
                self._on_message,
                loop,
                thread_name=f"timescale-{self._service}",
            )
            self._tail.start()
        except Exception:  # noqa: BLE001 - never block service start-up (T-et6-03)
            _log.warning("timescale_feed_start_failed", service=self._service, exc_info=True)
            self._tail = None

    async def stop(self) -> None:
        tail, self._tail = self._tail, None
        if tail is not None:
            await asyncio.to_thread(tail.stop)
