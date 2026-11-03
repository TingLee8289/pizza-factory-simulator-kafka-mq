"""Dashboard service settings."""

from functools import lru_cache

from pydantic import Field, SecretStr, field_validator

from pizza.common.config import Settings

# Mirrors pizza.scheduler.rework.DEFAULT_REWORK_LIMITS (D-12); duplicated so the dashboard
# does not depend on the scheduler package.
_DEFAULT_REWORK_LIMITS: dict[str, int] = {
    "spin": 2,
    "sauce": 0,
    "cheese": 0,
    "toppings": 0,
    "bake": 0,
    "slice": 0,
    "package": 2,
}


class DashboardSettings(Settings):
    """Env: DASHBOARD_PORT, RABBITMQ_MGMT_URL, PANEL_INTERVAL_S, ETA_TIMEOUT_S, ETA_POLL_S,
    ETA_POLL_ENABLED, ..."""

    dashboard_host: str = "0.0.0.0"  # noqa: S104 - container-bound localhost tool
    dashboard_port: int = 8000
    kafka_group_id: str = "dashboard"
    replay_timeout_s: float = 60.0
    rabbitmq_mgmt_url: str = "http://127.0.0.1:15672"
    rabbitmq_mgmt_user: str = "pizza"
    rabbitmq_mgmt_pass: SecretStr = SecretStr("pizza")
    mgmt_timeout_s: float = 2.0
    panel_interval_s: float = 2.0
    state_interval_s: float = 1.0
    max_arrival_per_min: float = 12.0
    due_k: float = 2.0
    # Wait for the generator's reply to a manual order (RPC), seconds.
    manual_order_timeout_s: float = 5.0
    # Phase 7.1: ETA estimate RPC timeout, background poll period and poll switch.
    eta_timeout_s: float = Field(default=1.5, gt=0, le=10)
    eta_poll_s: float = Field(default=1.0, gt=0)
    eta_poll_enabled: bool = True
    kpi_ring_size: int = Field(default=600, le=600)
    panel_ring_size: int = 30
    max_sse_clients: int = 20
    subscriber_queue_size: int = 64
    lag_groups: list[str] = Field(default_factory=lambda: ["scheduler", "kpi", "dashboard"])
    recipes_dir: str | None = None
    # Read-only failure/BANK tuning shared via the common compose env (UI copy only).
    mtbf_s: float | None = None
    mttr_s: float = 20.0
    rework_limits: dict[str, int] = Field(default_factory=lambda: dict(_DEFAULT_REWORK_LIMITS))
    station_queue_limit: int = 8
    bank_after_s: float = 60.0
    bank_wait_threshold: int | None = None
    heartbeat_interval_s: float = 5.0

    @field_validator("panel_interval_s")
    @classmethod
    def _panel_interval(cls, value: float) -> float:
        if not 1.0 <= value <= 2.0:
            raise ValueError("must be within [1.0, 2.0]")
        return value

    @field_validator("state_interval_s")
    @classmethod
    def _state_interval(cls, value: float) -> float:
        if not 0.5 <= value <= 1.0:
            raise ValueError("must be within [0.5, 1.0]")
        return value

    @field_validator("kpi_ring_size")
    @classmethod
    def _ring_size(cls, value: int) -> int:
        if not 1 <= value <= 600:
            raise ValueError("must be within [1, 600]")
        return value

    @field_validator(
        "replay_timeout_s",
        "mgmt_timeout_s",
        "max_arrival_per_min",
        "due_k",
        "manual_order_timeout_s",
    )
    @classmethod
    def _positive(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("must be > 0")
        return value


@lru_cache
def get_dashboard_settings() -> DashboardSettings:
    """Return the cached dashboard settings."""
    return DashboardSettings()
