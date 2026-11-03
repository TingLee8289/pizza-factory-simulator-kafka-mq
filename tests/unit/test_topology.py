"""Self-consistency tests for the declarative topology spec."""

import dataclasses

import pytest

from pizza.common.constants import STATIONS
from pizza.common.topology import (
    EXCHANGES,
    KAFKA_TOPICS,
    QUEUES,
    RETRY_TTL_MS,
    ExchangeSpec,
    QueueSpec,
    TopicSpec,
)

Q = {q.name: q for q in QUEUES}
EX = {e.name: e for e in EXCHANGES}
T = {t.name: t for t in KAFKA_TOPICS}


def test_kafka_topics():
    assert set(T) == {
        "station.events",
        "order.lifecycle",
        "wip.snapshots",
        "kpi.snapshots",
        "sim.timescale",
    }
    assert [T[n].partitions for n in ("station.events", "order.lifecycle")] == [3, 3]
    assert T["wip.snapshots"].partitions == 1
    assert T["kpi.snapshots"].partitions == 1
    for n in ("station.events", "order.lifecycle"):
        assert T[n].config["cleanup.policy"] == "delete"
        assert T[n].config["retention.ms"] == "2592000000"
    for n in ("wip.snapshots", "kpi.snapshots"):
        assert T[n].config["cleanup.policy"] == "compact"
    # quick-et6: full scale history is replayed by KPI, so delete (not compact).
    assert T["sim.timescale"].partitions == 1
    assert T["sim.timescale"].config["cleanup.policy"] == "delete"
    assert len(KAFKA_TOPICS) == 5


def test_exchanges():
    assert {e.name: e.type for e in EXCHANGES} == {
        "workorders": "direct",
        "dispatch": "direct",
        "dispatch.retry": "direct",
        "dlx": "fanout",
        "control": "topic",
        "scheduler.rework": "direct",
    }


def test_queue_count_and_quorum():
    assert len(QUEUES) == 34
    assert len(Q) == 34
    for q in QUEUES:
        assert q.arguments["x-queue-type"] == "quorum"
        # No explicit x-delivery-limit: the effective limit is the RabbitMQ 4.x
        # default (20) enforced by the broker; the jobs queue DLX is dispatch.retry
        # and the station never rejects (it reports op_failed and acks).
        assert "x-delivery-limit" not in q.arguments


@pytest.mark.parametrize("s", STATIONS)
def test_station_queues(s):
    jobs = Q[f"station.{s}.jobs"]
    assert jobs.arguments["x-dead-letter-exchange"] == "dispatch.retry"
    assert jobs.arguments["x-dead-letter-routing-key"] == f"station.{s}"
    assert jobs.bindings == (("dispatch", f"station.{s}"),)
    retry = Q[f"station.{s}.retry"]
    assert retry.arguments["x-message-ttl"] == RETRY_TTL_MS == 5000
    assert retry.arguments["x-dead-letter-exchange"] == "dispatch"
    assert retry.arguments["x-dead-letter-routing-key"] == f"station.{s}"
    assert retry.bindings == (("dispatch.retry", f"station.{s}"),)
    assert Q[f"station.{s}.control"].bindings == (("control", f"station.{s}.#"),)


@pytest.mark.parametrize("s", STATIONS)
def test_rework_hold_queues(s):
    hold = Q[f"scheduler.rework.{s}.hold"]
    assert hold.arguments["x-queue-type"] == "quorum"
    assert hold.arguments["x-message-ttl"] == RETRY_TTL_MS
    assert hold.arguments["x-dead-letter-exchange"] == "scheduler.rework"
    assert hold.arguments["x-dead-letter-routing-key"] == "return"
    assert hold.bindings == (("scheduler.rework", f"hold.{s}"),)


def test_singleton_queues():
    ret = Q["scheduler.rework.return"]
    assert ret.arguments["x-queue-type"] == "quorum"
    assert "x-message-ttl" not in ret.arguments
    assert "x-dead-letter-exchange" not in ret.arguments
    assert ret.bindings == (("scheduler.rework", "return"),)
    assert Q["dispatch.dlq"].bindings == (("dlx", ""),)
    assert Q["scheduler.control"].bindings == (("control", "scheduler.#"),)
    assert Q["generator.control"].bindings == (("control", "generator.#"),)
    assert Q["kpi.control"].bindings == (("control", "kpi.#"),)
    intake = Q["workorders.intake"]
    assert intake.bindings == (("workorders", "intake"),)
    assert not any(k.startswith("x-dead-letter") for k in intake.arguments)


def test_references_exist():
    for q in QUEUES:
        for ex, _ in q.bindings:
            assert ex in EX
        dle = q.arguments.get("x-dead-letter-exchange")
        if dle is not None:
            assert dle in EX


def test_frozen():
    with pytest.raises(dataclasses.FrozenInstanceError):
        KAFKA_TOPICS[0].name = "x"  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        EXCHANGES[0].type = "x"  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        QUEUES[0].name = "x"  # type: ignore[misc]
    assert TopicSpec and ExchangeSpec and QueueSpec
