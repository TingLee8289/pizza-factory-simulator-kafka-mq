"""Generator service settings."""

from functools import lru_cache

from pydantic import Field

from pizza.common.config import Settings
from pizza.common.constants import (
    DEFAULT_PRIORITY_HIGH_RATIO,
    DEFAULT_PRIORITY_HIGH_VALUE,
    PRIORITY_MAX,
    PRIORITY_MIN,
)


class GeneratorSettings(Settings):
    """Env: RECIPES_DIR, PUBLISH_RETRY_S, NACK_DELAY_S, DUE_K, ETA_TIMEOUT_S,
    RELEASE_LAG_WARN_S, SLEEP_DETECT_S, SLEEP_POLL_S, PRIORITY_HIGH_RATIO, PRIORITY_HIGH_VALUE."""

    recipes_dir: str | None = None
    publish_retry_s: float = Field(default=1.0, gt=0)
    nack_delay_s: float = 1.0
    # Default due-date multiplier for manual orders (mirrors dashboard DUE_K).
    due_k: float = Field(default=2.0, gt=0)
    # Bound on the scheduler ETA RPC at order release (Phase 7.1); failure falls back to theory due.
    eta_timeout_s: float = Field(default=1.5, gt=0, le=10)
    # Warn when an auto order's release lag jumps by more than this (wall seconds).
    release_lag_warn_s: float = Field(default=5.0, gt=0)
    # Host sleep detection: wall-vs-monotonic jump threshold and sampling period (real s).
    sleep_detect_s: float = Field(default=5.0, gt=0)
    sleep_poll_s: float = Field(default=1.0, gt=0)
    # Phase 07.2 D-08: share of auto orders given the high-urgency priority value.
    priority_high_ratio: float = Field(default=DEFAULT_PRIORITY_HIGH_RATIO, ge=0, le=1)
    priority_high_value: int = Field(
        default=DEFAULT_PRIORITY_HIGH_VALUE, ge=PRIORITY_MIN, le=PRIORITY_MAX
    )


@lru_cache
def get_generator_settings() -> GeneratorSettings:
    """Return the cached generator settings."""
    return GeneratorSettings()
