"""Idempotent broker bootstrap and passive verification (INFRA-02, D-12/13/14).

This is the only module that applies topology. It never deletes or purges
anything: on drift it fails with an instruction to run ``docker compose down -v``.

Passive declare (``ensure=True``) checks existence only, not queue arguments.
"""

import asyncio
import sys
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

import structlog
from aio_pika import ExchangeType
from aio_pika.exceptions import ChannelClosed, ChannelNotFoundEntity, ChannelPreconditionFailed
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewTopic,
)

from pizza.common.config import get_settings
from pizza.common.rabbit_io import connect
from pizza.common.topology import EXCHANGES, KAFKA_TOPICS, QUEUES, TopicSpec

_log = structlog.get_logger()

RESET_HINT = "run `docker compose down -v` to reset broker state and restart"


class BootstrapError(RuntimeError):
    """Raised when topology cannot be applied or verified."""


@dataclass
class Report:
    created: list[str] = field(default_factory=list)
    existing: list[str] = field(default_factory=list)
    altered: list[str] = field(default_factory=list)


def _partition_counts(admin: AdminClient) -> dict[str, int]:
    md = admin.list_topics(timeout=10)
    return {name: len(t.partitions) for name, t in md.topics.items()}


def apply_kafka(admin: AdminClient, specs: tuple[TopicSpec, ...] = KAFKA_TOPICS) -> Report:
    """Create missing topics, verify partitions, and align drifted configs."""
    report = Report()
    futures = admin.create_topics(
        [
            NewTopic(
                s.name, num_partitions=s.partitions, replication_factor=1, config=dict(s.config)
            )
            for s in specs
        ]
    )
    for name, fut in futures.items():
        try:
            fut.result()
            report.created.append(name)
        except KafkaException as exc:
            if exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                report.existing.append(name)
            else:
                raise

    counts = _partition_counts(admin)
    for s in specs:
        actual = counts.get(s.name)
        if actual != s.partitions:
            raise BootstrapError(
                f"topic {s.name}: expected {s.partitions} partitions, found {actual}. "
                f"Partitions cannot be reduced; {RESET_HINT}."
            )

    resources = [ConfigResource(ConfigResource.Type.TOPIC, s.name) for s in specs]
    described = {r.name: f for r, f in admin.describe_configs(resources).items()}
    for s in specs:
        current = described[s.name].result()
        drift = [
            ConfigEntry(k, v, incremental_operation=AlterConfigOpType.SET)
            for k, v in s.config.items()
            if k not in current or current[k].value != v
        ]
        if drift:
            res = ConfigResource(ConfigResource.Type.TOPIC, s.name, incremental_configs=drift)
            for fut in admin.incremental_alter_configs([res]).values():
                fut.result()
            report.altered.append(s.name)
    return report


def verify_kafka(admin: AdminClient, specs: tuple[TopicSpec, ...] = KAFKA_TOPICS) -> None:
    """Passive check: every topic exists with the expected partition count."""
    counts = _partition_counts(admin)
    problems = [
        f"{s.name} (expected {s.partitions} partitions, found {counts.get(s.name)})"
        for s in specs
        if counts.get(s.name) != s.partitions
    ]
    if problems:
        raise BootstrapError("Kafka topics missing or mismatched: " + "; ".join(problems))


async def _exists(connection: Any, kind: str, name: str) -> bool:
    """Passive existence check on a throwaway channel."""
    channel = await connection.channel()
    try:
        if kind == "exchange":
            await channel.get_exchange(name, ensure=True)
        else:
            await channel.get_queue(name, ensure=True)
        return True
    except (ChannelNotFoundEntity, ChannelClosed):
        return False
    finally:
        if not channel.is_closed:
            await channel.close()


async def apply_rabbit(connection: Any, exchanges: Any = EXCHANGES, queues: Any = QUEUES) -> Report:
    """Declare exchanges, queues and bindings; collect drift into one error."""
    report = Report()
    drifted: list[str] = []

    for ex in exchanges:
        existed = await _exists(connection, "exchange", ex.name)
        channel = await connection.channel()  # fresh channel: 406 closes it
        try:
            await channel.declare_exchange(ex.name, ExchangeType(ex.type), durable=True)
            (report.existing if existed else report.created).append(ex.name)
        except (ChannelPreconditionFailed, ChannelClosed):
            drifted.append(f"exchange {ex.name}")
        finally:
            if not channel.is_closed:
                await channel.close()

    for q in queues:
        existed = await _exists(connection, "queue", q.name)
        channel = await connection.channel()
        try:
            queue = await channel.declare_queue(q.name, durable=True, arguments=dict(q.arguments))
            for exchange_name, routing_key in q.bindings:
                exchange = await channel.get_exchange(exchange_name, ensure=False)
                await queue.bind(exchange, routing_key=routing_key)
            (report.existing if existed else report.created).append(q.name)
        except (ChannelPreconditionFailed, ChannelClosed):
            drifted.append(f"queue {q.name}")
        finally:
            if not channel.is_closed:
                await channel.close()

    if drifted:
        raise BootstrapError(
            "RabbitMQ topology drift (PRECONDITION_FAILED) for: "
            + ", ".join(drifted)
            + f". Nothing was deleted; {RESET_HINT}."
        )
    return report


async def verify_rabbit(connection: Any, exchanges: Any = EXCHANGES, queues: Any = QUEUES) -> None:
    """Passive check: every exchange and queue exists (arguments are not checked)."""
    missing = [
        f"exchange {e.name}" for e in exchanges if not await _exists(connection, "exchange", e.name)
    ]
    missing += [f"queue {q.name}" for q in queues if not await _exists(connection, "queue", q.name)]
    if missing:
        raise BootstrapError("RabbitMQ entities missing: " + ", ".join(missing))


def _wait_for_kafka(bootstrap: str, retries: int = 30, delay: float = 2.0) -> AdminClient:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    for attempt in range(1, retries + 1):
        try:
            admin.list_topics(timeout=5)
            return admin
        except KafkaException:
            _log.info("kafka_connect_retry", attempt=attempt)
            time.sleep(delay)
    raise BootstrapError("Kafka not reachable")


async def _run_rabbit(url: str) -> Report:
    connection = await connect(url)
    try:
        report = await apply_rabbit(connection)
        await verify_rabbit(connection)
        return report
    finally:
        await connection.close()


def main() -> None:
    """Apply and verify all topology; exit 0 on success, 1 on BootstrapError."""
    settings = get_settings()
    # Never log the full URL (credentials); host only.
    rabbit_host = urlparse(settings.rabbitmq_url).hostname
    try:
        admin = _wait_for_kafka(settings.kafka_bootstrap)
        kafka_report = apply_kafka(admin)
        verify_kafka(admin)
        rabbit_report = asyncio.run(_run_rabbit(settings.rabbitmq_url))
    except (BootstrapError, ConnectionError) as exc:
        _log.error("bootstrap_failed", error=str(exc))
        sys.exit(1)
    _log.info(
        "bootstrap_done",
        rabbit_host=rabbit_host,
        kafka_created=len(kafka_report.created),
        kafka_existing=len(kafka_report.existing),
        kafka_altered=len(kafka_report.altered),
        rabbit_created=len(rabbit_report.created),
        rabbit_existing=len(rabbit_report.existing),
    )
    sys.exit(0)


if __name__ == "__main__":
    main()
