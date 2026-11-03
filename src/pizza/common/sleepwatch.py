"""Host sleep detection: wall clock jumps while the monotonic clock does not.

When the host sleeps, ``time.monotonic`` stops (on most platforms) but the wall
clock keeps going. The jump between two adjacent samples, ``dwall - dmono``, is
the slept time. Adjacent-sample differences (not a cumulative offset) keep slow
NTP drift from ever accumulating into a false positive; negative jumps are ignored.

Always uses the real monotonic clock, never the scaled sim clock.
"""

import time
from collections.abc import Callable
from datetime import datetime

from pizza.common.envelope import utc_now


class HostSleepDetector:
    """Report the slept seconds once per wall-vs-monotonic jump of >= threshold."""

    def __init__(
        self,
        threshold_s: float,
        mono: Callable[[], float] = time.monotonic,
        wall: Callable[[], datetime] = utc_now,
    ) -> None:
        self.threshold_s = threshold_s
        self._mono = mono
        self._wall = wall
        self._prev_mono = mono()
        self._prev_wall = wall()

    def check(self) -> float | None:
        """Sample both clocks; return the jump in seconds if it reaches the threshold."""
        mono, wall = self._mono(), self._wall()
        jump = (wall - self._prev_wall).total_seconds() - (mono - self._prev_mono)
        self._prev_mono, self._prev_wall = mono, wall
        if jump >= self.threshold_s:
            return round(jump, 3)
        return None
