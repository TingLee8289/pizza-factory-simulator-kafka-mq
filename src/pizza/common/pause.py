"""Pausable timer gate shared by stations and the generator (Phase 5 D-04).

``PauseGate.sleep`` behaves like ``asyncio.sleep`` but freezes while any pause
reason is held and keeps the remaining time. ``abort`` releases every waiter
with ``GateAborted`` so a reset can never leave a job stuck. Durations use a
monotonic clock (project timing rule). stdlib only.
"""

import asyncio
import time
from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pizza.common.timescale import TimeScale


class GateAborted(Exception):
    """Raised in waiters when the gate is aborted (e.g. run reset)."""


class PauseGate:
    def __init__(
        self,
        clock: Callable[[], float] | None = None,
        timescale: "TimeScale | None" = None,
    ) -> None:
        # With a timescale, sleep durations are simulated seconds and the pause
        # accounting clock defaults to the simulated clock (quick-et6).
        if clock is None:
            clock = timescale.sim_now if timescale is not None else time.monotonic
        self._clock = clock
        self._timescale = timescale
        self._reasons: set[str] = set()
        self._running = asyncio.Event()
        self._running.set()
        # Set while paused (or aborted) so an in-progress sleep wakes up.
        self._interrupt = asyncio.Event()
        self._epoch = 0
        self._closed_paused = 0.0
        self._pause_started: float | None = None

    @property
    def is_paused(self) -> bool:
        return bool(self._reasons)

    def pause(self, reason: str = "run") -> None:
        if not self._reasons:
            self._pause_started = self._clock()
            self._running.clear()
            self._interrupt.set()
        self._reasons.add(reason)

    def resume(self, reason: str = "run") -> None:
        self._reasons.discard(reason)
        if not self._reasons:
            self._release()

    def clear(self) -> None:
        """Drop every reason and release without aborting waiters."""
        self._reasons.clear()
        self._release()

    def abort(self) -> None:
        """Wake every waiter with GateAborted and drop all reasons."""
        self._epoch += 1
        self._reasons.clear()
        self._release()
        self._interrupt.set()  # wake sleepers so they notice the epoch change

    def _release(self) -> None:
        if self._pause_started is not None:
            self._closed_paused += self._clock() - self._pause_started
            self._pause_started = None
        self._interrupt.clear()
        self._running.set()

    def paused_seconds(self) -> float:
        open_part = 0.0 if self._pause_started is None else self._clock() - self._pause_started
        return self._closed_paused + open_part

    async def wait_running(self) -> None:
        epoch = self._epoch
        await self._running.wait()
        if self._epoch != epoch:
            raise GateAborted

    async def sleep(self, seconds: float) -> None:
        epoch = self._epoch
        remaining = float(seconds)
        while True:
            await self._running.wait()
            if self._epoch != epoch:
                raise GateAborted
            if remaining <= 0:
                return
            started = time.monotonic()
            scale = 1.0
            if self._timescale is None:
                try:
                    await asyncio.wait_for(self._interrupt.wait(), timeout=remaining)
                except TimeoutError:
                    if self._epoch != epoch:
                        raise GateAborted from None
                    return
            else:
                scale = self._timescale.scale
                if not await self._wait_scaled(remaining / scale):
                    if self._epoch != epoch:
                        raise GateAborted
                    return
            if self._epoch != epoch:
                raise GateAborted
            remaining -= (time.monotonic() - started) * scale

    async def _wait_scaled(self, wall_s: float) -> bool:
        """Wait for a pause/abort or a scale change; False when the timeout elapsed."""
        assert self._timescale is not None
        tasks = {
            asyncio.ensure_future(self._interrupt.wait()),
            asyncio.ensure_future(self._timescale.changed_event.wait()),
        }
        try:
            done, _ = await asyncio.wait(tasks, timeout=wall_s, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        return bool(done)
