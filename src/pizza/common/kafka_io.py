"""Thin Kafka producer/consumer helpers enforcing the envelope contract (D-10).

Invalid inbound Kafka messages are logged with topic/partition/offset and
skipped (counted in ``INVALID_SKIPPED``). No "invalid" topic is created: that
would add topology beyond what D-12 defines.
"""

from typing import Any

import structlog
from confluent_kafka import Producer

from pizza.common.envelope import Fact, InvalidEnvelope, Message, encode, parse_message

INVALID_SKIPPED = 0

_log = structlog.get_logger()


def make_producer(bootstrap: str) -> Producer:
    """Create an idempotent, acks=all producer."""
    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,
            "acks": "all",
        }
    )


def produce_fact(producer: Any, topic: str, fact: Fact, key: str) -> None:
    """Produce a Fact synchronously; refuse anything else before any I/O."""
    if not isinstance(fact, Fact):
        raise TypeError(f"produce_fact accepts only Fact, got {type(fact).__name__}")
    errors: list[Any] = []

    def _on_delivery(err: Any, _msg: Any) -> None:
        if err is not None:
            errors.append(err)

    producer.produce(topic, value=encode(fact), key=key.encode(), on_delivery=_on_delivery)
    remaining = producer.flush(10)
    if errors:
        raise RuntimeError(f"Kafka delivery failed: {errors[0]}")
    if remaining:
        raise RuntimeError(f"{remaining} message(s) not delivered before flush timeout")


def parse_or_skip(raw: bytes, *, topic: str, partition: int, offset: int) -> Message | None:
    """Parse inbound bytes; on invalid input log a warning, count and return None."""
    global INVALID_SKIPPED
    try:
        return parse_message(raw)
    except InvalidEnvelope as exc:
        INVALID_SKIPPED += 1
        _log.warning(
            "invalid_message_skipped",
            topic=topic,
            partition=partition,
            offset=offset,
            error=str(exc),
        )
        return None
