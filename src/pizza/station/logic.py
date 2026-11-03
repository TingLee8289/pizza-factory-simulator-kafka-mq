"""Pure station helpers (no broker, no I/O)."""

from collections.abc import Mapping
from typing import Any

from pizza.common.envelope import MAX_JOB_TOTAL_S, JobCommand


class UnknownRecipe(ValueError):
    """The job names a recipe this station does not have (D-04)."""


def work_seconds(job: JobCommand, recipes: Mapping[str, float] | None = None) -> float:
    """Total processing time: one sleep of unit time * qty (D-01/D-13).

    ``job.unit_time_s`` is an override that wins over the recipe table (D-02).
    """
    if job.unit_time_s is not None:
        unit = job.unit_time_s
    elif recipes is None or job.recipe is None or job.recipe not in recipes:
        raise UnknownRecipe(f"unknown recipe {job.recipe!r}")
    else:
        unit = recipes[job.recipe]
    total = unit * job.qty
    if total > MAX_JOB_TOTAL_S:
        raise ValueError(f"resolved job time {total}s exceeds {MAX_JOB_TOTAL_S}s")
    return total


def attempt_from(redelivered: bool, headers: Mapping[str, Any] | None) -> int:
    """Delivery attempt number from RabbitMQ metadata.

    Informational only, never a dedupe key (D-09/D-10).
    """
    if not redelivered:
        return 1
    raw = (headers or {}).get("x-delivery-count")
    if isinstance(raw, bytes):
        raw = raw.decode(errors="replace")
    try:
        count = int(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        count = 1
    return count + 1


def is_for_station(job: JobCommand, station_name: str) -> bool:
    """True only when the job's step targets this station (D-07)."""
    return job.step == station_name


def should_fail(job: JobCommand) -> bool:
    """Test-injection failure predicate (D-10): poison, or the first fail_times visits."""
    return job.poison or job.visit <= (job.fail_times or 0)


def _text(value: Any) -> Any:
    return value.decode(errors="replace") if isinstance(value, bytes) else value


def delivery_limit_exceeded(headers: Mapping[str, Any] | None, jobs_queue: str) -> bool:
    """True when ``x-death`` records a delivery_limit dead-letter from ``jobs_queue``."""
    try:
        deaths = (headers or {}).get("x-death")
        if not isinstance(deaths, list | tuple):
            return False
        for entry in deaths:
            if not isinstance(entry, Mapping):
                continue
            if _text(entry.get("reason")) == "delivery_limit" and _text(entry.get("queue")) == (
                jobs_queue
            ):
                return True
    except Exception:  # noqa: BLE001 - malformed headers must never raise
        return False
    return False
