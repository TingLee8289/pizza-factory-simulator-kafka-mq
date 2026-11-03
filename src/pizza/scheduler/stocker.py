"""Stocker: classification of scheduler backlog, not a container.

Overflow and down-blocked lots stay WAIT and count in WIP; BANK does not.
Pure functions; time values are passed in, no clock inside.
"""

from collections.abc import Sequence
from datetime import datetime
from typing import Literal

StockKind = Literal["overflow", "station_down", "station_offline"]
Availability = Literal["ok", "down", "offline"]
BankReason = Literal["capacity", "customer", "schedule"]
BankSource = Literal["manual", "auto_down", "auto_wait"]


def classify_backlog(
    order_ids_fifo: Sequence[str],
    availability: Availability,
    queue_limit: int | None,
) -> dict[str, StockKind]:
    """Map backlog entries to a stocker kind; queue-slice entries are absent."""
    if availability == "offline":
        return {oid: "station_offline" for oid in order_ids_fifo}
    if availability == "down":
        return {oid: "station_down" for oid in order_ids_fifo}
    if queue_limit is None:
        return {}
    return {oid: "overflow" for oid in order_ids_fifo[queue_limit:]}


def due_for_down_bank(
    order_ids_fifo: Sequence[str],
    unavailable_since: datetime | None,
    now: datetime,
    bank_after_s: float,
) -> list[str]:
    """All ids (FIFO) once the station was unavailable for >= bank_after_s."""
    if unavailable_since is None:
        return []
    if (now - unavailable_since).total_seconds() >= bank_after_s:
        return list(order_ids_fifo)
    return []


def should_wait_bank(count: int, threshold: int | None) -> bool:
    """True when a newly arriving lot would push WAIT over the threshold."""
    if threshold is None:
        return False
    return count >= threshold


def wait_release_count(banked_count: int, wait_count: int, threshold: int | None) -> int:
    """How many banked lots may be released back to WAIT."""
    if threshold is None:
        return 0
    return min(banked_count, max(0, threshold - wait_count))
