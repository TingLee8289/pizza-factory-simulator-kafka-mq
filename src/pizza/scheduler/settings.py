"""Scheduler service settings."""

from functools import lru_cache

from pydantic import field_validator, model_validator

from pizza.common.config import Settings
from pizza.common.constants import STATIONS
from pizza.common.topology import RETRY_TTL_MS
from pizza.scheduler.rework import DEFAULT_REWORK_LIMITS


class SchedulerSettings(Settings):
    """Env: NACK_DELAY_S, DISPATCH_RETRY_S, INTAKE_DEFER_MAX, RESUME_ON_START,
    REWORK_LIMITS (JSON, merged over defaults), STATION_QUEUE_LIMIT, BANK_AFTER_S,
    BANK_WAIT_THRESHOLD, HEARTBEAT_INTERVAL_S, OFFLINE_FACTOR, SWEEP_INTERVAL_S, RECIPES_DIR,
    REWORK_HOLD_GRACE_S, ...
    """

    nack_delay_s: float = 1.0
    dispatch_retry_s: float = 1.0
    intake_defer_max: int = 10
    # False lets integration tests ignore stale runs left in the shared topics.
    resume_on_start: bool = True
    replay_timeout_s: float = 60.0
    kafka_group_id: str = "scheduler"
    rework_limits: dict[str, int] = dict(DEFAULT_REWORK_LIMITS)
    station_queue_limit: int = 8
    bank_after_s: float = 60.0
    bank_wait_threshold: int | None = None
    heartbeat_interval_s: float = 5.0
    offline_factor: int = 3
    sweep_interval_s: float = 1.0
    recipes_dir: str | None = None
    # Sweep fallback for a lost rework hold message (D-13); default 3 x RETRY_TTL_MS.
    rework_hold_grace_s: float = 3 * RETRY_TTL_MS / 1000

    @field_validator("nack_delay_s", "dispatch_retry_s", "replay_timeout_s")
    @classmethod
    def _non_negative(cls, value: float) -> float:
        if value < 0:
            raise ValueError("must be >= 0")
        return value

    @field_validator("intake_defer_max", "station_queue_limit")
    @classmethod
    def _positive(cls, value: int) -> int:
        if value < 1:
            raise ValueError("must be >= 1")
        return value

    @field_validator("rework_limits")
    @classmethod
    def _rework_limits(cls, value: dict[str, int]) -> dict[str, int]:
        for key, limit in value.items():
            if key not in STATIONS:
                raise ValueError(f"unknown station {key!r}")
            if not 0 <= limit <= 10:
                raise ValueError("rework limit must be within 0..10")
        return {**DEFAULT_REWORK_LIMITS, **value}

    @field_validator("bank_after_s", "sweep_interval_s")
    @classmethod
    def _gt_zero(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("must be > 0")
        return value

    @field_validator("rework_hold_grace_s")
    @classmethod
    def _grace_after_ttl(cls, value: float) -> float:
        # A grace at or below the hold-queue TTL would let the sweep beat the TTL hop.
        if value <= RETRY_TTL_MS / 1000:
            raise ValueError(f"must be > RETRY_TTL_MS ({RETRY_TTL_MS / 1000} s)")
        return value

    @field_validator("heartbeat_interval_s")
    @classmethod
    def _heartbeat(cls, value: float) -> float:
        if value != 0 and not 0.05 <= value <= 300:
            raise ValueError("must be 0 or within 0.05..300")
        return value

    @field_validator("offline_factor")
    @classmethod
    def _factor(cls, value: int) -> int:
        if value < 2:
            raise ValueError("must be >= 2")
        return value

    @model_validator(mode="after")
    def _threshold_consistency(self) -> "SchedulerSettings":
        if (
            self.bank_wait_threshold is not None
            and self.bank_wait_threshold < self.station_queue_limit
        ):
            raise ValueError("bank_wait_threshold must be >= station_queue_limit")
        return self


@lru_cache
def get_scheduler_settings() -> SchedulerSettings:
    """Return the cached scheduler settings."""
    return SchedulerSettings()
