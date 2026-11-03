"""Station service settings (STATION-01)."""

from functools import lru_cache

from pydantic import field_validator

from pizza.common.config import Settings
from pizza.common.constants import STATIONS


class StationSettings(Settings):
    """Env: STATION_NAME (required, one of STATIONS), NACK_DELAY_S, MTBF_S, MTTR_S,
    HEARTBEAT_INTERVAL_S.

    Random breakdowns need both MTBF_S and the run's ``random_breakdowns`` flag (D-25).
    """

    station_name: str
    # Bounded pause before nack when Kafka emit fails (RESEARCH Pitfall 3).
    nack_delay_s: float = 1.0
    # Optional recipe directory override (RECIPES_DIR); None = packaged files.
    recipes_dir: str | None = None
    # Phase 6 failure model (D-01/D-09); MTBF unset = no random breakdowns.
    mtbf_s: float | None = None
    mttr_s: float = 20.0
    # 0 disables the heartbeat; lower bound prevents a Kafka flood (Pitfall 6).
    heartbeat_interval_s: float = 5.0

    @field_validator("mtbf_s")
    @classmethod
    def _mtbf_positive(cls, value: float | None) -> float | None:
        if value is not None and value <= 0:
            raise ValueError("MTBF_S must be > 0")
        return value

    @field_validator("mttr_s")
    @classmethod
    def _mttr_positive(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("MTTR_S must be > 0")
        return value

    @field_validator("heartbeat_interval_s")
    @classmethod
    def _heartbeat_bounds(cls, value: float) -> float:
        if value != 0 and not 0.05 <= value <= 300:
            raise ValueError("HEARTBEAT_INTERVAL_S must be 0 or within 0.05..300")
        return value

    @field_validator("station_name")
    @classmethod
    def _known_station(cls, value: str) -> str:
        if value not in STATIONS:
            raise ValueError(f"STATION_NAME {value!r} is not one of {list(STATIONS)}")
        return value


@lru_cache
def get_station_settings() -> StationSettings:
    """Return the cached station settings."""
    return StationSettings()
