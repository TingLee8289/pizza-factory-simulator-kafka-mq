"""Unit tests for producer/consumer guards (D-10); no brokers needed."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from structlog.testing import capture_logs

from pizza.common import kafka_io, rabbit_io
from pizza.common.envelope import SmokeCommand, SmokeFact, encode


class StubProducer:
    def __init__(self, fail: bool = False, pending: int = 0) -> None:
        self.calls: list[dict] = []
        self.flushed = 0
        self.fail = fail
        self.pending = pending

    def produce(self, topic, value=None, key=None, on_delivery=None):
        self.calls.append({"topic": topic, "value": value, "key": key})
        if on_delivery is not None:
            on_delivery("boom" if self.fail else None, MagicMock())

    def flush(self, timeout=None):
        self.flushed += 1
        return self.pending


def _fact() -> SmokeFact:
    return SmokeFact.make(run_id="r1", machine_id="m1")


def _command() -> SmokeCommand:
    return SmokeCommand.make(run_id="r1", machine_id="m1")


def test_produce_fact_rejects_command_without_io() -> None:
    p = StubProducer()
    with pytest.raises(TypeError):
        kafka_io.produce_fact(p, "t", _command(), "k")
    assert p.calls == []


def test_produce_fact_rejects_raw_dict() -> None:
    p = StubProducer()
    with pytest.raises(TypeError):
        kafka_io.produce_fact(p, "t", {"a": 1}, "k")
    assert p.calls == []


def test_produce_fact_valid() -> None:
    p = StubProducer()
    fact = _fact()
    kafka_io.produce_fact(p, "t", fact, "key1")
    assert len(p.calls) == 1
    assert p.calls[0]["value"] == encode(fact)
    assert p.calls[0]["key"] == b"key1"
    assert p.flushed == 1


def test_produce_fact_delivery_error_raises() -> None:
    p = StubProducer(fail=True)
    with pytest.raises(RuntimeError):
        kafka_io.produce_fact(p, "t", _fact(), "k")


def test_produce_fact_pending_raises() -> None:
    p = StubProducer(pending=1)
    with pytest.raises(RuntimeError):
        kafka_io.produce_fact(p, "t", _fact(), "k")


def test_parse_or_skip_valid() -> None:
    fact = _fact()
    assert kafka_io.parse_or_skip(encode(fact), topic="t", partition=0, offset=1) == fact


def test_parse_or_skip_invalid_logs_and_counts() -> None:
    before = kafka_io.INVALID_SKIPPED
    with capture_logs() as logs:
        out = kafka_io.parse_or_skip(b"not json", topic="t", partition=2, offset=7)
    assert out is None
    assert kafka_io.INVALID_SKIPPED == before + 1
    ev = logs[0]
    assert ev["log_level"] == "warning"
    assert ev["topic"] == "t" and ev["partition"] == 2 and ev["offset"] == 7
    assert ev["error"]


def test_publish_command_rejects_fact_before_channel_use() -> None:
    exchange = MagicMock()
    exchange.publish = AsyncMock()
    with pytest.raises(TypeError):
        asyncio.run(rabbit_io.publish_command(exchange, "rk", _fact()))
    exchange.publish.assert_not_called()


def test_publish_command_valid() -> None:
    exchange = MagicMock()
    exchange.publish = AsyncMock()
    cmd = _command()
    asyncio.run(rabbit_io.publish_command(exchange, "rk", cmd))
    exchange.publish.assert_awaited_once()
    msg = exchange.publish.await_args.args[0]
    assert msg.body == encode(cmd)


def test_dead_letter_invalid_publishes_to_dlx_then_acks() -> None:
    dlx = MagicMock()
    dlx.publish = AsyncMock()
    channel = MagicMock()
    channel.get_exchange = AsyncMock(return_value=dlx)
    message = MagicMock()
    message.body = b"garbage"
    message.ack = AsyncMock()
    message.reject = AsyncMock()
    asyncio.run(rabbit_io.dead_letter_invalid(channel, message, "x" * 900, "station.a.jobs"))
    channel.get_exchange.assert_awaited_once_with("dlx")
    sent = dlx.publish.await_args.args[0]
    assert sent.body == b"garbage"
    assert len(sent.headers["x-invalid-reason"]) == 500
    assert sent.headers["x-source-queue"] == "station.a.jobs"
    message.ack.assert_awaited_once()
    message.reject.assert_not_called()
