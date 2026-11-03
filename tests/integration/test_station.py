"""Walking-skeleton integration tests against real Kafka and RabbitMQ (D-18).

Every test uses station ``sauce`` (never ``bake``): the compose ``station``
service runs as ``bake`` and must not steal messages. Do NOT run the compose
station as ``sauce`` while these tests run. Each test uses a fresh run_id and
order_id, and the sauce queues are purged before and after every test.
"""

import argparse
import asyncio
import os
import subprocess
import sys
import time
import uuid

import aio_pika
import pytest

from pizza.common.config import Settings
from pizza.common.dedupe import Deduper
from pizza.common.envelope import (
    JobCommand,
    OpCompleted,
    OpStarted,
    StationStatus,
    encode,
)
from pizza.common.kafka_io import make_producer
from pizza.common.rabbit_io import connect, publish_command
from pizza.common.topology import (
    EX_CONTROL,
    EX_DISPATCH,
    Q_DISPATCH_DLQ,
    jobs_queue_name,
)
from pizza.recorder.__main__ import record
from pizza.station.service import Station
from pizza.station.settings import StationSettings
from pizza.tools.send_job import build_job, build_run_started
from tests.conftest import collect_events, purge_station_queues, queue_counts

pytestmark = pytest.mark.integration

STATION = "sauce"
TIMEOUT = 30.0


@pytest.fixture(autouse=True)
async def _clean_queues(brokers: Settings):
    await purge_station_queues(brokers, STATION)
    yield
    await purge_station_queues(brokers, STATION)


def _ids() -> tuple[str, str]:
    return f"it-{uuid.uuid4().hex[:12]}", f"o-{uuid.uuid4().hex[:8]}"


def start_station_subprocess(settings: Settings, name: str = STATION) -> subprocess.Popen:
    env = {
        **os.environ,
        "STATION_NAME": name,
        "KAFKA_BOOTSTRAP": settings.kafka_bootstrap,
        "RABBITMQ_URL": settings.rabbitmq_url,
    }
    return subprocess.Popen(
        [sys.executable, "-m", "pizza.station"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _terminate(*procs: subprocess.Popen | None) -> None:
    for proc in procs:
        if proc is not None and proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)


def make_job(run_id: str, order_id: str, unit_time_s: float, visit: int = 1, event_id=None):
    return build_job(
        argparse.Namespace(
            station=STATION,
            run_id=run_id,
            order_id=order_id,
            unit_time_s=unit_time_s,
            visit=visit,
            qty=1,
            **({"event_id": event_id} if event_id else {}),
        )
    )


async def publish_job(settings: Settings, job: JobCommand) -> None:
    connection = await connect(settings.rabbitmq_url)
    try:
        channel = await connection.channel()
        exchange = await channel.get_exchange(EX_DISPATCH)
        await publish_command(exchange, f"station.{STATION}", job)
    finally:
        await connection.close()


async def publish_run_started(settings: Settings, run_id: str) -> None:
    connection = await connect(settings.rabbitmq_url)
    try:
        channel = await connection.channel()
        exchange = await channel.get_exchange(EX_CONTROL)
        await publish_command(
            exchange, f"station.{STATION}.run", build_run_started(STATION, run_id)
        )
    finally:
        await connection.close()


def _count(events, cls) -> int:
    return sum(isinstance(e, cls) for e in events)


def _has_idle(events) -> bool:
    return any(isinstance(e, StationStatus) and e.status == "idle" for e in events)


async def _collect(settings, run_id, until, timeout_s: float = TIMEOUT):
    return await asyncio.to_thread(collect_events, settings, run_id, until, timeout_s)


async def _wait_queue_empty(settings: Settings, timeout_s: float = 20.0) -> tuple[int, int | None]:
    """Poll until ready == 0 and unacked in (0, None); management stats lag a few s."""
    deadline = time.monotonic() + timeout_s
    counts: tuple[int, int | None] = (-1, -1)
    while time.monotonic() < deadline:
        counts = await queue_counts(settings, jobs_queue_name(STATION))
        if counts[0] == 0 and counts[1] in (0, None):
            return counts
        await asyncio.sleep(1.0)
    return counts


async def test_job_emits_events(brokers: Settings) -> None:
    run_id, order_id = _ids()
    proc = start_station_subprocess(brokers)
    try:
        await publish_run_started(brokers, run_id)
        # Idle must be visible first so a job-first delivery cannot skip it.
        await _collect(brokers, run_id, _has_idle)
        job = make_job(run_id, order_id, 0.2)
        await publish_job(brokers, job)
        events = await _collect(brokers, run_id, lambda ev: _count(ev, OpCompleted) >= 1)
        # Wait for the trailing idle as well.
        events = await _collect(
            brokers,
            run_id,
            lambda ev: (
                _count(ev, OpCompleted) >= 1
                and sum(isinstance(e, StationStatus) and e.status == "idle" for e in ev) >= 2
            ),
        )
    finally:
        _terminate(proc)

    seq = [f"{e.type}:{e.status}" if isinstance(e, StationStatus) else e.type for e in events]
    assert seq == [
        "station.status:idle",
        "station.status:busy",
        "op.started",
        "op.completed",
        "station.status:idle",
    ]
    started = next(e for e in events if isinstance(e, OpStarted))
    completed = next(e for e in events if isinstance(e, OpCompleted))
    for op in (started, completed):
        assert op.step == STATION
        assert op.visit == 1
        assert op.attempt == 1
        assert op.command_event_id == job.event_id
    assert (completed.ts - started.ts).total_seconds() >= 0.2


async def test_kill_redelivers(brokers: Settings) -> None:
    run_id, order_id = _ids()
    first = second = None
    try:
        first = start_station_subprocess(brokers)
        await publish_job(brokers, make_job(run_id, order_id, 3.0))
        await _collect(brokers, run_id, lambda ev: _count(ev, OpStarted) >= 1)
        first.kill()
        first.wait(timeout=10)

        second = start_station_subprocess(brokers)
        events = await _collect(
            brokers,
            run_id,
            lambda ev: _count(ev, OpCompleted) >= 1,
            timeout_s=TIMEOUT + 10,
        )
        counts = await _wait_queue_empty(brokers)
    finally:
        _terminate(first, second)

    starts = [e for e in events if isinstance(e, OpStarted)]
    assert len(starts) >= 2
    assert starts[-1].redelivered is True
    assert starts[-1].attempt >= 2
    assert _count(events, OpCompleted) >= 1
    results = record(events, Deduper(), run_id)
    assert [r.status for r in results].count("RECORDED") == 1
    assert counts[0] == 0
    assert counts[1] in (0, None)


async def test_prefetch_one(brokers: Settings) -> None:
    run_id, _ = _ids()
    proc = start_station_subprocess(brokers)
    try:
        await publish_job(brokers, make_job(run_id, f"o-{uuid.uuid4().hex[:8]}", 1.0))
        await publish_job(brokers, make_job(run_id, f"o-{uuid.uuid4().hex[:8]}", 1.0))
        events = await _collect(brokers, run_id, lambda ev: _count(ev, OpCompleted) >= 2)
    finally:
        _terminate(proc)

    ops = [e for e in events if isinstance(e, (OpStarted, OpCompleted))]
    assert [e.type for e in ops] == ["op.started", "op.completed", "op.started", "op.completed"]


async def test_invalid_job_dead_lettered(brokers: Settings) -> None:
    run_id, order_id = _ids()
    marker = uuid.uuid4().hex
    wrong_job = build_job(
        argparse.Namespace(station="cheese", run_id=run_id, order_id=order_id, unit_time_s=0.2)
    )
    garbage = b'{"not":"an envelope","marker":"%s"}' % marker.encode()

    proc = start_station_subprocess(brokers)
    connection = await connect(brokers.rabbitmq_url)
    try:
        channel = await connection.channel()
        exchange = await channel.get_exchange(EX_DISPATCH)
        # Routed to sauce's queue but for another station.
        await exchange.publish(
            aio_pika.Message(body=encode(wrong_job), message_id=str(wrong_job.event_id)),
            routing_key=f"station.{STATION}",
        )
        await exchange.publish(aio_pika.Message(body=garbage), routing_key=f"station.{STATION}")

        dlq = await channel.get_queue(Q_DISPATCH_DLQ)
        wanted = {encode(wrong_job), garbage}
        found: dict[bytes, aio_pika.abc.AbstractIncomingMessage] = {}
        deadline = time.monotonic() + TIMEOUT
        while len(found) < 2 and time.monotonic() < deadline:
            msg = await dlq.get(fail=False)
            if msg is None:
                await asyncio.sleep(0.3)
                continue
            if msg.body in wanted:
                found[msg.body] = msg
                assert msg.headers["x-invalid-reason"]
            await msg.ack()
        assert len(found) == 2, "both invalid jobs must reach dispatch.dlq"
    finally:
        _terminate(proc)
        await connection.close()

    counts = await _wait_queue_empty(brokers)
    assert counts[0] == 0


async def test_graceful_stop(brokers: Settings) -> None:
    run_id, order_id = _ids()
    settings = StationSettings(
        station_name=STATION,
        kafka_bootstrap=brokers.kafka_bootstrap,
        rabbitmq_url=brokers.rabbitmq_url,
    )
    producer = make_producer(brokers.kafka_bootstrap)
    connection = await connect(brokers.rabbitmq_url)
    station = Station(STATION, producer, settings)
    task = asyncio.create_task(station.run(connection))
    try:
        await publish_run_started(brokers, run_id)
        await publish_job(brokers, make_job(run_id, order_id, 1.0))
        await _collect(brokers, run_id, lambda ev: _count(ev, OpStarted) >= 1)
        station.stop()
        await asyncio.wait_for(task, timeout=TIMEOUT)
    finally:
        if not task.done():
            task.cancel()
        await connection.close()

    events = await _collect(
        brokers,
        run_id,
        lambda ev: any(isinstance(e, StationStatus) and e.status == "offline" for e in ev),
    )
    labels = [e.status if isinstance(e, StationStatus) else e.type for e in events]
    assert "op.completed" in labels
    last_idle = len(labels) - 1 - labels[::-1].index("idle")
    assert labels.index("op.completed") < last_idle < labels.index("offline")
    counts = await _wait_queue_empty(brokers)
    assert counts == (0, 0) or (counts[0] == 0 and counts[1] is None)


async def test_duplicate_recorded_once(brokers: Settings) -> None:
    run_id, order_id = _ids()
    event_id = uuid.uuid4()
    proc = start_station_subprocess(brokers)
    try:
        await publish_job(brokers, make_job(run_id, order_id, 0.2, event_id=event_id))
        await publish_job(brokers, make_job(run_id, order_id, 0.2, event_id=event_id))
        events = await _collect(brokers, run_id, lambda ev: _count(ev, OpCompleted) >= 2)
        assert _count(events, OpCompleted) == 2

        deduper = Deduper()
        statuses = [r.status for r in record(events, deduper, run_id)]
        assert statuses == ["RECORDED", "DUPLICATE"]

        # A re-visit (visit + 1) with a new event_id is a distinct completion (D-10).
        await publish_job(brokers, make_job(run_id, order_id, 0.2, visit=2))
        events = await _collect(brokers, run_id, lambda ev: _count(ev, OpCompleted) >= 3)
        visit_results = [r for r in record(events, deduper, run_id) if r.visit == 2]
        assert [r.status for r in visit_results] == ["RECORDED"]
    finally:
        _terminate(proc)
