"""Thin RabbitMQ helpers enforcing the envelope contract (D-10)."""

import asyncio
from collections.abc import Mapping
from typing import Any

import aio_pika
import structlog

from pizza.common.envelope import Command, encode
from pizza.common.topology import EX_DLX

_log = structlog.get_logger()


async def connect(url: str, retries: int = 30, delay: float = 2.0) -> Any:
    """Connect (robust) with a retry loop; never logs the URL (credentials)."""
    last: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return await aio_pika.connect_robust(url)
        except (ConnectionError, OSError, aio_pika.exceptions.AMQPError) as exc:
            last = exc
            _log.info("rabbitmq_connect_retry", attempt=attempt)
            await asyncio.sleep(delay)
    raise ConnectionError(f"RabbitMQ not reachable after {retries} attempts") from last


async def publish_command(
    exchange: Any,
    routing_key: str,
    command: Command,
    headers: Mapping[str, Any] | None = None,
    *,
    reply_to: str | None = None,
    correlation_id: str | None = None,
) -> None:
    """Publish a Command with a persistent message; refuse anything else."""
    if not isinstance(command, Command):
        raise TypeError(f"publish_command accepts only Command, got {type(command).__name__}")
    message = aio_pika.Message(
        body=encode(command),
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        content_type="application/json",
        message_id=str(command.event_id),
        headers=dict(headers) if headers else None,
        reply_to=reply_to,
        correlation_id=correlation_id,
    )
    await exchange.publish(message, routing_key=routing_key)


async def dead_letter_invalid(channel: Any, message: Any, reason: str, source_queue: str) -> None:
    """Route an invalid inbound message to the DLQ via ``dlx`` and ack the original.

    Resolves the D-10 / D-12 conflict: a plain reject(requeue=False) on
    ``station.<id>.jobs`` would dead-letter into dispatch.retry and loop
    forever, never reaching the DLQ. Instead the original body is published
    to ``dlx`` (bound to dispatch.dlq) with diagnostic headers, then acked.
    """
    dlx = await channel.get_exchange(EX_DLX)
    await dlx.publish(
        aio_pika.Message(
            body=message.body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            headers={"x-invalid-reason": reason[:500], "x-source-queue": source_queue},
        ),
        routing_key="",
    )
    await message.ack()
