from datetime import UTC, datetime, timedelta

from pizza.common.sleepwatch import HostSleepDetector

_T0 = datetime(2026, 1, 1, tzinfo=UTC)


class Clocks:
    def __init__(self) -> None:
        self.mono = 100.0
        self.wall = _T0

    def step(self, mono: float, wall: float) -> None:
        self.mono += mono
        self.wall += timedelta(seconds=wall)

    def detector(self, threshold: float = 5.0) -> HostSleepDetector:
        return HostSleepDetector(threshold, mono=lambda: self.mono, wall=lambda: self.wall)


def test_normal_ticks_do_not_trigger():
    c = Clocks()
    d = c.detector()
    for _ in range(5):
        c.step(1, 1)
        assert d.check() is None


def test_sleep_jump_triggers_exactly_once():
    c = Clocks()
    d = c.detector()
    c.step(1, 1)
    assert d.check() is None
    c.step(1, 1028)
    assert d.check() == 1027.0
    c.step(1, 1)
    assert d.check() is None


def test_threshold_boundary():
    c = Clocks()
    d = c.detector()
    c.step(0, 4.9)
    assert d.check() is None
    c.step(0, 5.0)
    assert d.check() == 5.0


def test_backward_wall_jump_ignored():
    c = Clocks()
    d = c.detector()
    c.step(1, -30)
    assert d.check() is None
    c.step(1, 1)
    assert d.check() is None
