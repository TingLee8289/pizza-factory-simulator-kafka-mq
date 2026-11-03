"""KPI service settings."""

from functools import lru_cache

from pydantic import field_validator

from pizza.common.config import Settings


class KpiSettings(Settings):
    """Env: WARMUP_ORDERS, LITTLE_TOLERANCE, BOTTLENECK_*, SNAPSHOT_INTERVAL_S, ..."""

    warmup_orders: int = 10
    little_tolerance: float = 0.10
    bottleneck_margin: float = 0.05
    bottleneck_confirm_n: int = 3
    bottleneck_min_util: float = 0.80
    snapshot_interval_s: float = 1.0
    kafka_group_id: str = "kpi"
    replay_timeout_s: float = 60.0
    nack_delay_s: float = 1.0
    resume_on_start: bool = True
    recipes_dir: str | None = None
    round_close_wait_s: float = 10.0

    @field_validator("warmup_orders")
    @classmethod
    def _warmup_non_negative(cls, value: int) -> int:
        if value < 0:
            raise ValueError("must be >= 0")
        return value

    @field_validator("bottleneck_confirm_n")
    @classmethod
    def _confirm_positive(cls, value: int) -> int:
        if value < 1:
            raise ValueError("must be >= 1")
        return value

    @field_validator("bottleneck_min_util")
    @classmethod
    def _unit_interval(cls, value: float) -> float:
        if not 0.0 <= value <= 1.0:
            raise ValueError("must be within [0, 1]")
        return value

    @field_validator(
        "little_tolerance",
        "bottleneck_margin",
        "snapshot_interval_s",
        "replay_timeout_s",
        "nack_delay_s",
    )
    @classmethod
    def _non_negative(cls, value: float) -> float:
        if value < 0:
            raise ValueError("must be >= 0")
        return value

    @field_validator("round_close_wait_s")
    @classmethod
    def _round_close_wait_bounded(cls, value: float) -> float:
        if not 0 <= value <= 60:
            raise ValueError("must be >= 0 and <= 60")
        return value


@lru_cache
def get_kpi_settings() -> KpiSettings:
    """Return the cached KPI settings."""
    return KpiSettings()
