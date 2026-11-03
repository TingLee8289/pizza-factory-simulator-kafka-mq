"""Heartbeat staleness: pure. Times (monotonic seconds) are passed in."""

from collections.abc import Mapping

OFFLINE_FACTOR = 3


def stale_stations(
    last_seen: Mapping[str, float],
    now: float,
    interval_s: float,
    factor: int = OFFLINE_FACTOR,
) -> set[str]:
    """Stations silent for more than interval_s * factor; empty if disabled."""
    if interval_s <= 0:
        return set()
    limit = interval_s * factor
    return {s for s, seen in last_seen.items() if now - seen > limit}
