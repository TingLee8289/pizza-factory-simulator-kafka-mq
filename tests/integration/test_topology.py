"""Topology existence and bootstrap idempotency against real brokers (D-17)."""

import base64
import json
import urllib.request
from typing import Any
from urllib.parse import quote, urlparse

import pytest
from confluent_kafka.admin import AdminClient, ConfigResource

from pizza.common.bootstrap import apply_kafka, apply_rabbit
from pizza.common.config import Settings
from pizza.common.rabbit_io import connect
from pizza.common.topology import EXCHANGES, KAFKA_TOPICS, QUEUES

pytestmark = pytest.mark.integration

MGMT = "http://localhost:15672"


def _mgmt(settings: Settings, path: str) -> Any:
    parsed = urlparse(settings.rabbitmq_url)
    token = base64.b64encode(f"{parsed.username}:{parsed.password}".encode()).decode()
    req = urllib.request.Request(MGMT + path, headers={"Authorization": f"Basic {token}"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.load(resp)


def test_kafka_topics_exist(brokers: Settings) -> None:
    admin = AdminClient({"bootstrap.servers": brokers.kafka_bootstrap})
    md = admin.list_topics(timeout=10)
    for spec in KAFKA_TOPICS:
        assert spec.name in md.topics, spec.name
        assert len(md.topics[spec.name].partitions) == spec.partitions
    assert len(md.topics["station.events"].partitions) == 3
    assert len(md.topics["order.lifecycle"].partitions) == 3
    for name in ("wip.snapshots", "kpi.snapshots"):
        assert len(md.topics[name].partitions) == 1
        res = ConfigResource(ConfigResource.Type.TOPIC, name)
        cfg = next(iter(admin.describe_configs([res]).values())).result()
        assert cfg["cleanup.policy"].value == "compact"


def test_rabbit_entities_exist(brokers: Settings) -> None:
    vhost = quote("/", safe="")
    exchanges = {e["name"]: e["type"] for e in _mgmt(brokers, f"/api/exchanges/{vhost}")}
    for ex in EXCHANGES:
        assert exchanges.get(ex.name) == ex.type, ex.name

    queues = {q["name"]: q for q in _mgmt(brokers, f"/api/queues/{vhost}")}
    assert len(QUEUES) == 34
    for q in QUEUES:
        assert q.name in queues, q.name
        args = queues[q.name]["arguments"]
        assert args["x-queue-type"] == "quorum"
        if q.name.endswith((".retry", ".hold")):
            assert args["x-message-ttl"] == 5000
            if q.name.endswith(".hold"):
                assert args["x-dead-letter-exchange"] == "scheduler.rework"

    bindings = {
        (b["source"], b["destination"], b["routing_key"])
        for b in _mgmt(brokers, f"/api/bindings/{vhost}")
        if b["destination_type"] == "queue"
    }
    for q in QUEUES:
        for exchange_name, routing_key in q.bindings:
            assert (exchange_name, q.name, routing_key) in bindings, (q.name, routing_key)


async def test_bootstrap_idempotent(brokers: Settings) -> None:
    admin = AdminClient({"bootstrap.servers": brokers.kafka_bootstrap})
    for _ in range(2):
        kafka_report = apply_kafka(admin)
    assert kafka_report.created == []
    assert kafka_report.altered == []

    connection = await connect(brokers.rabbitmq_url)
    try:
        for _ in range(2):
            rabbit_report = await apply_rabbit(connection)
    finally:
        await connection.close()
    assert rabbit_report.created == []
    assert rabbit_report.altered == []
