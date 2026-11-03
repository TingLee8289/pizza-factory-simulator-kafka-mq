"""Kafka vs RabbitMQ panel: queue depths, DLQ and consumer lag (DASH-04, D-08).

Credentials and the management URL are never logged or returned (T-05-27).
"""

import asyncio
import copy
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from typing import Any

import httpx
import structlog

from pizza.common.topology import Q_DISPATCH_DLQ, QUEUES

_log = structlog.get_logger()

_KNOWN_QUEUES = frozenset(q.name for q in QUEUES)
_RESULT_TIMEOUT_S = 10


def parse_queues(rows: Any) -> dict[str, tuple[int, int]]:
    """Keep only topology queues; map to (ready, unacked)."""
    out: dict[str, tuple[int, int]] = {}
    if not isinstance(rows, list):
        return out
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = row.get("name")
        if name not in _KNOWN_QUEUES:
            continue
        ready = row.get("messages_ready", 0)
        unacked = row.get("messages_unacknowledged", 0)
        out[name] = (
            ready if isinstance(ready, int) else 0,
            unacked if isinstance(unacked, int) else 0,
        )
    return out


def summarize_rabbit(rows: Any) -> dict[str, Any]:
    parsed = parse_queues(rows)
    return {
        "queues": {n: {"ready": r, "unacked": u} for n, (r, u) in parsed.items()},
        "dlq": parsed.get(Q_DISPATCH_DLQ, (0, 0))[0],
    }


def compute_lag(
    committed: Mapping[tuple[str, int], int], ends: Mapping[tuple[str, int], int]
) -> int | None:
    """Sum of max(0, end - committed) over partitions with a committed offset."""
    lag = 0
    seen = False
    for key, offset in committed.items():
        if offset < 0:
            continue
        seen = True
        lag += max(0, ends.get(key, offset) - offset)
    return lag if seen else None


def _empty_panel() -> dict[str, Any]:
    return {
        "as_of": None,
        "error": False,
        "rabbit": {"queues": {}, "dlq": 0},
        "kafka": {"lag": {}},
    }


class PanelPoller:
    def __init__(self, settings: Any, http_client: Any = None, admin: Any = None) -> None:
        self.settings = settings
        self._http = http_client
        self._admin = admin
        self._last = _empty_panel()

    # -- lazy clients -------------------------------------------------------
    def _client(self) -> Any:
        if self._http is None:
            self._http = httpx.AsyncClient(
                base_url=self.settings.rabbitmq_mgmt_url,
                auth=(
                    self.settings.rabbitmq_mgmt_user,
                    self.settings.rabbitmq_mgmt_pass.get_secret_value(),
                ),
                timeout=self.settings.mgmt_timeout_s,
            )
        return self._http

    def _admin_client(self) -> Any:
        if self._admin is None:
            from confluent_kafka.admin import AdminClient

            self._admin = AdminClient({"bootstrap.servers": self.settings.kafka_bootstrap})
        return self._admin

    async def aclose(self) -> None:
        http, self._http = self._http, None
        if http is not None:
            await http.aclose()

    # -- polls --------------------------------------------------------------
    async def _poll_rabbit(self) -> dict[str, Any]:
        resp = await self._client().get(
            "/api/queues/%2F",
            params={"columns": "name,messages_ready,messages_unacknowledged"},
        )
        if resp.status_code != 200:
            raise RuntimeError("management api non-200")
        return summarize_rabbit(resp.json())

    def _group_lag_sync(self, group: str) -> int | None:
        from confluent_kafka import ConsumerGroupTopicPartitions
        from confluent_kafka.admin import OffsetSpec

        admin = self._admin_client()
        res = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group)])[
            group
        ].result(_RESULT_TIMEOUT_S)
        wanted = [tp for tp in res.topic_partitions if tp.offset >= 0]
        if not wanted:
            return None
        committed = {(tp.topic, tp.partition): tp.offset for tp in wanted}
        futures = admin.list_offsets({tp: OffsetSpec.latest() for tp in wanted})
        ends = {
            (tp.topic, tp.partition): fut.result(_RESULT_TIMEOUT_S).offset
            for tp, fut in futures.items()
        }
        return compute_lag(committed, ends)

    async def _poll_kafka(self) -> dict[str, int | None]:
        lag: dict[str, int | None] = {}
        for group in self.settings.lag_groups:
            lag[group] = await asyncio.to_thread(self._group_lag_sync, group)
        return lag

    async def poll_once(self) -> dict[str, Any]:
        panel = copy.deepcopy(self._last)
        error = False
        try:
            panel["rabbit"] = await self._poll_rabbit()
        except Exception:  # noqa: BLE001 - keep last good numbers
            error = True
            _log.warning("panel_poll_failed", source="rabbit")
        try:
            panel["kafka"] = {"lag": await self._poll_kafka()}
        except Exception:  # noqa: BLE001
            error = True
            panel["kafka"] = {"lag": {}}
            _log.warning("panel_poll_failed", source="kafka")
        panel["error"] = error
        panel["as_of"] = datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        self._last = panel
        return copy.deepcopy(panel)

    async def run(
        self,
        on_update: Callable[[dict[str, Any]], Awaitable[None] | None],
        stop_event: asyncio.Event,
    ) -> None:
        while not stop_event.is_set():
            try:
                panel = await self.poll_once()
                result = on_update(panel)
                if result is not None:
                    await result
            except Exception:  # noqa: BLE001 - the poller must never die
                _log.warning("panel_poll_failed", source="loop")
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.settings.panel_interval_s)
            except TimeoutError:
                pass
