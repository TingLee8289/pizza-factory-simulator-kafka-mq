"""In-process SSE fan-out hub with bounded rings (D-02, D-03, DASH-02).

asyncio only; no broker I/O. ``publish`` is synchronous and must be called from
the event loop thread only: Kafka consumer callbacks reach it through
``run_coroutine_threadsafe`` in the runtime (plan 05-08). A single ``seq``
counter is shared by both channels ("events" and "state").

Every collection is bounded (NFR-02, T-05-21/22): per-client queues drop their
oldest item when full, the client count is capped, and all rings are deques
with ``maxlen``.
"""

import asyncio
from collections import deque
from dataclasses import dataclass
from typing import Any, Literal

from pizza.common.envelope import KpiSnapshot

Channel = Literal["events", "state"]
CHANNELS: tuple[Channel, ...] = ("events", "state")


class TooManySubscribers(Exception):  # noqa: N818 - name fixed by the plan interface
    """Raised when ``max_clients`` subscriptions are already open."""


@dataclass(frozen=True)
class HubItem:
    seq: int
    kind: str
    payload: dict[str, Any]


class Subscription:
    """One client's bounded queue; a slow client loses its oldest items only."""

    def __init__(self, channel: Channel, queue_size: int) -> None:
        self.channel = channel
        self.queue: asyncio.Queue[HubItem] = asyncio.Queue(maxsize=queue_size)
        self.dropped = 0

    async def get(self) -> HubItem:
        return await self.queue.get()

    def put_nowait_drop_oldest(self, item: HubItem) -> None:
        try:
            self.queue.put_nowait(item)
        except asyncio.QueueFull:
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:  # pragma: no cover - defensive
                pass
            self.dropped += 1
            self.queue.put_nowait(item)


def _num(value: float | None, scale: float = 1.0) -> float | None:
    return None if value is None else value * scale


def kpi_point(snapshot: KpiSnapshot) -> dict[str, Any]:
    """Chart point for one KPI snapshot; missing values stay None (gaps)."""
    return {
        "t": snapshot.ts.timestamp(),
        "pizzas_per_min": _num(snapshot.pizzas_per_s, 60.0),
        "wip_now": snapshot.wip_now,
        "wip_pizzas": snapshot.wip_pizzas,
        "wip_avg": snapshot.wip_avg,
        "wip_avg_pizzas": snapshot.wip_avg_pizzas,
        "cycle_mean_s": snapshot.cycle_time.per_pizza_mean_s,
        "cycle_p95_s": snapshot.cycle_time.per_pizza_p95_s,
        "little_rel_error": snapshot.little.rel_error,
    }


class Hub:
    def __init__(
        self,
        max_clients: int = 20,
        queue_size: int = 64,
        kpi_ring_size: int = 600,
        panel_ring_size: int = 30,
    ) -> None:
        self.max_clients = max_clients
        self.queue_size = queue_size
        self.panel_ring_size = panel_ring_size
        self.seq = 0
        self._subs: dict[Channel, set[Subscription]] = {c: set() for c in CHANNELS}
        self.kpi_ring: deque[dict[str, Any]] = deque(maxlen=kpi_ring_size)
        self.panel_rings: dict[str, deque[float | None]] = {}

    # ---- fan-out ----------------------------------------------------------

    @property
    def client_count(self) -> int:
        return sum(len(s) for s in self._subs.values())

    def subscribe(self, channel: Channel) -> Subscription:
        if channel not in self._subs:
            raise ValueError(f"unknown channel: {channel}")
        if self.client_count >= self.max_clients:
            raise TooManySubscribers(f"max {self.max_clients} clients")
        sub = Subscription(channel, self.queue_size)
        self._subs[channel].add(sub)
        return sub

    def unsubscribe(self, sub: Subscription) -> None:
        self._subs[sub.channel].discard(sub)

    def publish(self, channel: Channel, kind: str, payload: dict[str, Any]) -> int:
        """Assign the next shared seq and fan out; never blocks or raises."""
        self.seq += 1
        item = HubItem(self.seq, kind, payload)
        for sub in tuple(self._subs[channel]):
            sub.put_nowait_drop_oldest(item)
        return self.seq

    # ---- rings ------------------------------------------------------------

    def add_kpi(self, snapshot: KpiSnapshot) -> dict[str, Any]:
        point = kpi_point(snapshot)
        self.kpi_ring.append(point)
        return point

    def add_panel(self, panel: dict[str, float | None]) -> None:
        """Append one point per series key (e.g. ``rabbit.<queue>.ready``)."""
        for key, value in panel.items():
            ring = self.panel_rings.get(key)
            if ring is None:
                ring = self.panel_rings[key] = deque(maxlen=self.panel_ring_size)
            ring.append(value)

    def clear_rings(self) -> None:
        """Empty kpi and panel rings (Start/Reset); seq is deliberately kept."""
        self.kpi_ring.clear()
        self.panel_rings.clear()
