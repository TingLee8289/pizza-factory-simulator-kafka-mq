"""Shared RabbitMQ RPC client (Phase 7.1).

One exclusive auto-delete reply queue, futures keyed by ``correlation_id`` (the command
event_id). Used by the dashboard (manual orders, ETA estimates), the generator and the
ETA poller. This module must not import from the dashboard or generator packages.
"""

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

from pizza.common.rabbit_io import publish_command

_log = structlog.get_logger()


class RpcTimeout(Exception):
    """No reply arrived within the timeout."""


class RpcBusy(Exception):
    """Too many requests in flight; nothing was published."""


class RpcClient:
    def __init__(
        self,
        get_channel: Callable[[], Any],
        get_control_exchange: Callable[[], Awaitable[Any]],
        *,
        max_inflight: int = 8,
    ) -> None:
        self._get_channel = get_channel
        self._get_control_exchange = get_control_exchange
        self._max_inflight = max_inflight
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._reply_queue: str | None = None
        self._lock = asyncio.Lock()

    async def _ensure(self) -> str:
        async with self._lock:
            if self._reply_queue is None:
                channel = self._get_channel()
                queue = await channel.declare_queue("", exclusive=True, auto_delete=True)
                await queue.consume(self._on_reply, no_ack=True)
                self._reply_queue = queue.name
            return self._reply_queue

    async def _on_reply(self, message: Any) -> None:
        try:
            body = json.loads(message.body)
            fut = self._pending.get(message.correlation_id)
            if fut is not None and not fut.done() and isinstance(body, dict):
                fut.set_result(body)
        except Exception:  # noqa: BLE001 - a bad reply must never break the consumer
            _log.warning("rpc_reply_ignored")

    async def request(self, command: Any, routing_key: str, timeout_s: float) -> dict[str, Any]:
        """Publish ``command`` and await its reply within one overall ``timeout_s`` deadline.

        The deadline covers reply-queue setup, exchange lookup and publish as well as the
        reply itself, so a silently stalled broker raises ``RpcTimeout`` instead of hanging
        the caller (generator release loop, dashboard request).
        """
        if len(self._pending) >= self._max_inflight:
            raise RpcBusy("too many pending rpc requests")
        key = str(command.event_id)
        fut: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[key] = fut
        stage = "setup"
        try:
            async with asyncio.timeout(timeout_s):
                reply_to = await self._ensure()
                exchange = await self._get_control_exchange()
                stage = "publish"
                await publish_command(
                    exchange,
                    routing_key,
                    command,
                    reply_to=reply_to,
                    correlation_id=key,
                )
                stage = "reply"
                return await fut
        except TimeoutError:
            if stage != "reply":
                _log.warning("rpc_stalled", stage=stage, routing_key=routing_key)
            raise RpcTimeout(f"no reply ({stage} timed out)") from None
        finally:
            self._pending.pop(key, None)
