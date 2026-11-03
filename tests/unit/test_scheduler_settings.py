import pytest
from pydantic import ValidationError

from pizza.scheduler.rework import DEFAULT_REWORK_LIMITS
from pizza.scheduler.settings import SchedulerSettings


def test_defaults():
    s = SchedulerSettings()
    assert s.station_queue_limit == 8
    assert s.bank_after_s == 60.0
    assert s.bank_wait_threshold is None
    assert s.heartbeat_interval_s == 5.0
    assert s.offline_factor == 3
    assert s.sweep_interval_s == 1.0
    assert s.rework_limits == DEFAULT_REWORK_LIMITS


def test_rework_env_merges(monkeypatch):
    monkeypatch.setenv("REWORK_LIMITS", '{"sauce":1}')
    s = SchedulerSettings()
    assert s.rework_limits["sauce"] == 1
    assert s.rework_limits["spin"] == 2


@pytest.mark.parametrize("raw", ['{"nope":1}', '{"sauce":-1}', '{"sauce":11}'])
def test_rework_rejects(monkeypatch, raw):
    monkeypatch.setenv("REWORK_LIMITS", raw)
    with pytest.raises(ValidationError):
        SchedulerSettings()


def test_threshold_consistency(monkeypatch):
    monkeypatch.setenv("STATION_QUEUE_LIMIT", "8")
    monkeypatch.setenv("BANK_WAIT_THRESHOLD", "4")
    with pytest.raises(ValidationError):
        SchedulerSettings()
    monkeypatch.setenv("BANK_WAIT_THRESHOLD", "8")
    assert SchedulerSettings().bank_wait_threshold == 8


def test_bounds(monkeypatch):
    monkeypatch.setenv("STATION_QUEUE_LIMIT", "0")
    with pytest.raises(ValidationError):
        SchedulerSettings()
    monkeypatch.delenv("STATION_QUEUE_LIMIT")
    monkeypatch.setenv("OFFLINE_FACTOR", "1")
    with pytest.raises(ValidationError):
        SchedulerSettings()


def test_rework_hold_grace_default_and_bounds(monkeypatch):
    """WR-01: grace defaults to 3 x RETRY_TTL_MS and must exceed the hold TTL."""
    from pizza.common.topology import RETRY_TTL_MS

    assert SchedulerSettings().rework_hold_grace_s == 3 * RETRY_TTL_MS / 1000
    monkeypatch.setenv("REWORK_HOLD_GRACE_S", "30")
    assert SchedulerSettings().rework_hold_grace_s == 30.0
    monkeypatch.setenv("REWORK_HOLD_GRACE_S", str(RETRY_TTL_MS / 1000))
    with pytest.raises(ValidationError):
        SchedulerSettings()
