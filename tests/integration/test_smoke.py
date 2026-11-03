"""Envelope smoke tests against real Kafka and RabbitMQ (INFRA-03, D-10, D-17).

Smoke messages use run_id ``smoke-<uuid>`` and only existing D-12 topics/queues;
everything fetched from RabbitMQ is acked so reruns stay clean.
"""

import asyncio
import time
import uuid

import aio_pika
import pytest
from confluent_kafka import Consumer
from pydantic import ValidationError

from pizza.common.config import Settings
from pizza.common.envelope import (
    InvalidEnvelope,
    SmokeCommand,
    SmokeFact,
    encode,
    parse_message,
)
from pizza.common.kafka_io import make_producer, parse_or_skip, produce_fact
from pizza.common.rabbit_io import connect, dead_letter_invalid, publish_command

pytestmark = pytest.mark.integration


def _run_id() -> str:
    return f"smoke-{uuid.uuid4()}"


async def _get(
    queue: aio_pika.abc.AbstractQueue, timeout: float = 10.0
) -> aio_pika.abc.AbstractIncomingMessage:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        msg = await queue.get(fail=False)
        if msg is not None:
            return msg
        await asyncio.sleep(0.2)
    raise AssertionError(f"no message on {queue.name} within {timeout}s")


async def _get_matching(queue, predicate, timeout: float = 10.0):
    """Fetch until predicate matches; ack and discard non-matching smoke leftovers."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        msg = await _get(queue, max(deadline - time.monotonic(), 0.5))
        if predicate(msg):
            return msg
        await msg.ack()
    raise AssertionError("matching message not found")


def test_fact_roundtrip_through_kafka(brokers: Settings) -> None:
    run_id = _run_id()
    fact = SmokeFact.make(run_id=run_id, machine_id="bake")
    producer = make_producer(brokers.kafka_bootstrap)
    produce_fact(producer, "station.events", fact, key="bake")

    consumer = Consumer(
        {
            "bootstrap.servers": brokers.kafka_bootstrap,
            "group.id": f"smoke-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe(["station.events"])
    found = None
    deadline = time.monotonic() + 30
    try:
        while time.monotonic() < deadline and found is None:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            parsed = parse_or_skip(
                msg.value(), topic=msg.topic(), partition=msg.partition(), offset=msg.offset()
            )
            if parsed is not None and parsed.run_id == run_id:
                found = parsed
    finally:
        consumer.close()
    assert found == fact


async def test_command_roundtrip_through_rabbit(brokers: Settings) -> None:
    cmd = SmokeCommand.make(run_id=_run_id(), machine_id="intake")
    connection = await connect(brokers.rabbitmq_url)
    try:
        channel = await connection.channel()
        exchange = await channel.get_exchange("workorders")
        queue = await channel.get_queue("workorders.intake")
        await publish_command(exchange, "intake", cmd)
        msg = await _get_matching(queue, lambda m: m.message_id == str(cmd.event_id))
        assert parse_message(msg.body) == cmd
        await msg.ack()
    finally:
        await connection.close()


def test_producer_refuses_invalid(brokers: Settings) -> None:
    class CountingProducer:
        calls = 0

        def produce(self, *args: object, **kwargs: object) -> None:
            self.calls += 1

        def flush(self, *args: object) -> int:
            return 0

    with pytest.raises(ValidationError):
        data = SmokeFact.make(run_id="x", machine_id="bake").model_dump(mode="json")
        del data["run_id"]
        SmokeFact.model_validate(data)

    producer = CountingProducer()
    cmd = SmokeCommand.make(run_id=_run_id(), machine_id="intake")
    with pytest.raises(TypeError):
        produce_fact(producer, "station.events", cmd, key="x")  # type: ignore[arg-type]
    assert producer.calls == 0


async def test_invalid_inbound_goes_to_dlq(brokers: Settings) -> None:
    body = b'{"not":"an envelope", "marker":"%s"}' % uuid.uuid4().hex.encode()
    connection = await connect(brokers.rabbitmq_url)
    try:
        channel = await connection.channel()
        exchange = await channel.get_exchange("workorders")
        intake = await channel.get_queue("workorders.intake")
        dlq = await channel.get_queue("dispatch.dlq")

        await exchange.publish(aio_pika.Message(body=body), routing_key="intake")
        msg = await _get_matching(intake, lambda m: m.body == body)
        with pytest.raises(InvalidEnvelope) as exc_info:
            parse_message(msg.body)
        await dead_letter_invalid(channel, msg, str(exc_info.value), "workorders.intake")

        dead = await _get_matching(dlq, lambda m: m.body == body)
        assert dead.headers["x-invalid-reason"]
        assert dead.body == body
        await dead.ack()
    finally:
        await connection.close()


def test_parse_or_skip_garbage() -> None:
    assert parse_or_skip(b"\x00garbage", topic="station.events", partition=0, offset=0) is None
    assert encode(SmokeFact.make(run_id=_run_id(), machine_id="bake"))
