"""Unit tests for StationSettings (STATION-01)."""

import pytest
from pydantic import ValidationError

from pizza.station.settings import StationSettings, get_station_settings


def test_valid_station_name():
    assert StationSettings(station_name="bake").station_name == "bake"


def test_recipes_dir_default_and_env(monkeypatch):
    assert StationSettings(station_name="bake", _env_file=None).recipes_dir is None
    monkeypatch.setenv("RECIPES_DIR", "/tmp/r")
    assert StationSettings(station_name="bake", _env_file=None).recipes_dir == "/tmp/r"


def test_env_invalid_name_fails(monkeypatch):
    monkeypatch.setenv("STATION_NAME", "nope")
    get_station_settings.cache_clear()
    with pytest.raises(ValidationError):
        get_station_settings()
    get_station_settings.cache_clear()


def test_missing_env_fails(monkeypatch):
    monkeypatch.delenv("STATION_NAME", raising=False)
    get_station_settings.cache_clear()
    with pytest.raises(ValidationError):
        StationSettings(_env_file=None)
    get_station_settings.cache_clear()


def test_failure_defaults(monkeypatch):
    for name in ("MTBF_S", "MTTR_S", "HEARTBEAT_INTERVAL_S"):
        monkeypatch.delenv(name, raising=False)
    cfg = StationSettings(station_name="bake", _env_file=None)
    assert cfg.mtbf_s is None
    assert cfg.mttr_s == 20.0
    assert cfg.heartbeat_interval_s == 5.0


def test_failure_env_parses_and_validates(monkeypatch):
    monkeypatch.setenv("MTBF_S", "300")
    assert StationSettings(station_name="bake", _env_file=None).mtbf_s == 300.0
    monkeypatch.setenv("MTBF_S", "0")
    with pytest.raises(ValidationError):
        StationSettings(station_name="bake", _env_file=None)
    monkeypatch.delenv("MTBF_S")
    monkeypatch.setenv("MTTR_S", "0")
    with pytest.raises(ValidationError):
        StationSettings(station_name="bake", _env_file=None)
    monkeypatch.delenv("MTTR_S")


@pytest.mark.parametrize(
    ("value", "ok"), [("0", True), ("0.05", True), ("0.01", False), ("301", False)]
)
def test_heartbeat_bounds(monkeypatch, value, ok):
    monkeypatch.setenv("HEARTBEAT_INTERVAL_S", value)
    if ok:
        assert StationSettings(station_name="bake", _env_file=None).heartbeat_interval_s == float(
            value
        )
    else:
        with pytest.raises(ValidationError):
            StationSettings(station_name="bake", _env_file=None)
