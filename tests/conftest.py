"""Shared pytest fixtures. Unit tests must never import the `brokers` fixture."""

import aio_pika
import pytest
from confluent_kafka.admin import AdminClient

from pizza.common.config import Settings


@pytest.fixture(scope="session")
async def brokers() -> Settings:
    """Verify Kafka and RabbitMQ are reachable; used by integration tests only."""
    settings = Settings()
    try:
        AdminClient({"bootstrap.servers": settings.kafka_bootstrap}).list_topics(timeout=5)
        connection = await aio_pika.connect(settings.rabbitmq_url, timeout=5)
        await connection.close()
    except Exception as exc:
        pytest.fail(f"brokers unreachable: run `docker compose up -d --wait` first ({exc})")
    return settings


# ---------------------------------------------------------------------------
# Integration helpers (plain functions; unit tests never call them).
# ---------------------------------------------------------------------------
import asyncio  # noqa: E402
import base64  # noqa: E402
import json  # noqa: E402
import os  # noqa: E402
import subprocess  # noqa: E402
import sys  # noqa: E402
import time  # noqa: E402
import urllib.error  # noqa: E402
import urllib.request  # noqa: E402
import uuid  # noqa: E402
from collections.abc import Callable  # noqa: E402
from pathlib import Path  # noqa: E402
from urllib.parse import unquote, urlparse  # noqa: E402

from confluent_kafka import OFFSET_END, Consumer  # noqa: E402

from pizza.common.constants import STATIONS  # noqa: E402
from pizza.common.envelope import KpiSnapshot, Message  # noqa: E402
from pizza.common.kafka_io import parse_or_skip  # noqa: E402
from pizza.common.rabbit_io import connect  # noqa: E402
from pizza.common.recipes import load_all_recipes  # noqa: E402
from pizza.common.topology import (  # noqa: E402
    Q_GENERATOR_CONTROL,
    Q_KPI_CONTROL,
    Q_SCHEDULER_CONTROL,
    Q_WORKORDERS_INTAKE,
    TOPIC_KPI_SNAPSHOTS,
    TOPIC_STATION_EVENTS,
    control_queue_name,
    jobs_queue_name,
)


def collect_topics(
    settings: Settings,
    topics: list[str],
    run_id: str,
    until: Callable[[list[Message]], bool],
    timeout_s: float = 30.0,
    start_ms: int | None = None,
) -> list[Message]:
    """Read several topics in one consumer (fresh group, earliest) until `until` or timeout.

    `start_ms` (epoch ms) seeks every partition to the first offset at or after that
    timestamp, skipping the topic history; without it the read starts at earliest, which
    costs seconds once a live line has filled the topics.

    Returns messages of `run_id` merged in arrival order.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": f"test-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    events: list[Message] = []
    deadline = time.monotonic() + timeout_s

    def _seek_to_start(cons: Consumer, partitions: list) -> None:
        for tp in partitions:
            tp.offset = start_ms
        found = cons.offsets_for_times(partitions, timeout=10)
        for tp in found:
            if tp.offset < 0:  # no message at/after start_ms yet: read new ones only
                tp.offset = OFFSET_END
        cons.assign(found)

    try:
        if start_ms is None:
            consumer.subscribe(list(topics))
        else:
            consumer.subscribe(list(topics), on_assign=_seek_to_start)
        while time.monotonic() < deadline:
            msg = consumer.poll(0.5)
            if msg is None or msg.error():
                continue
            parsed = parse_or_skip(
                msg.value(), topic=msg.topic(), partition=msg.partition(), offset=msg.offset()
            )
            if parsed is not None and parsed.run_id == run_id:
                events.append(parsed)
                if until(events):
                    break
    finally:
        consumer.close()
    return events


def collect_topic(
    settings: Settings,
    topic: str,
    run_id: str,
    until: Callable[[list[Message]], bool],
    timeout_s: float = 30.0,
) -> list[Message]:
    """Read one topic (fresh group, earliest) until `until(events)` or timeout."""
    return collect_topics(settings, [topic], run_id, until, timeout_s)


def last_kpi_snapshot(
    settings: Settings,
    run_id: str,
    predicate: Callable[[KpiSnapshot], bool] = lambda s: True,
    timeout_s: float = 30.0,
) -> KpiSnapshot | None:
    """Read kpi.snapshots from earliest until a snapshot of `run_id` satisfies `predicate`.

    The topic is compacted, so a fresh consumer sees many snapshots of the run; the
    LAST one seen is returned (None if no snapshot satisfied the predicate).
    """

    def until(events: list[Message]) -> bool:
        last = events[-1]
        return isinstance(last, KpiSnapshot) and predicate(last)

    events = collect_topic(settings, TOPIC_KPI_SNAPSHOTS, run_id, until, timeout_s)
    snaps = [e for e in events if isinstance(e, KpiSnapshot)]
    if snaps and predicate(snaps[-1]):
        return snaps[-1]
    return None


def collect_events(
    settings: Settings,
    run_id: str,
    until: Callable[[list[Message]], bool],
    timeout_s: float = 30.0,
) -> list[Message]:
    """Read station.events for `run_id`; per-station order is preserved (key = machine_id)."""
    return collect_topic(settings, TOPIC_STATION_EVENTS, run_id, until, timeout_s)


async def purge_line_queues(settings: Settings) -> None:
    """Drain every jobs/control queue plus intake and scheduler/generator/kpi control.

    Quorum-safe: drains by get+ack, so queues are never deleted or re-declared.
    """
    connection = await connect(settings.rabbitmq_url)
    try:
        channel = await connection.channel()
        names = [
            Q_WORKORDERS_INTAKE,
            Q_SCHEDULER_CONTROL,
            Q_GENERATOR_CONTROL,
            Q_KPI_CONTROL,
        ]
        for station in STATIONS:
            names += [jobs_queue_name(station), control_queue_name(station)]
        for name in names:
            queue = await channel.get_queue(name)
            while True:
                msg = await queue.get(fail=False)
                if msg is None:
                    break
                await msg.ack()
    finally:
        await connection.close()


def write_scaled_recipes(dest: Path, scale: float) -> Path:
    """Write <dest>/<station>.toml with every unit time multiplied by `scale` (D-19)."""
    dest = Path(dest)
    dest.mkdir(parents=True, exist_ok=True)
    for station, table in load_all_recipes().items():
        lines = [f'station = "{station}"', "", "[unit_time_s]"]
        lines += [f"{product} = {value * scale!r}" for product, value in table.items()]
        (dest / f"{station}.toml").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return dest


def start_service(
    module: str,
    settings: Settings,
    extra_env: dict | None = None,
    station: str | None = None,
) -> subprocess.Popen:
    """Spawn `python -m <module>` with broker settings; output is discarded."""
    env = {
        **os.environ,
        "KAFKA_BOOTSTRAP": settings.kafka_bootstrap,
        "RABBITMQ_URL": settings.rabbitmq_url,
    }
    if station is not None:
        env["STATION_NAME"] = station
    env.update(extra_env or {})
    return subprocess.Popen(
        [sys.executable, "-m", module],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def terminate(proc: subprocess.Popen | None, kill: bool = False) -> None:
    """Stop a child: SIGKILL when kill=True, else terminate with a 20 s fallback to kill."""
    if proc is None or proc.poll() is not None:
        return
    if kill:
        proc.kill()
        proc.wait(timeout=10)
        return
    proc.terminate()
    try:
        proc.wait(timeout=20)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)


async def purge_station_queues(settings: Settings, station: str) -> None:
    """Drain the station's jobs and control queues by get/ack (quorum-safe)."""
    connection = await connect(settings.rabbitmq_url)
    try:
        channel = await connection.channel()
        for name in (jobs_queue_name(station), control_queue_name(station)):
            queue = await channel.get_queue(name)
            while True:
                msg = await queue.get(fail=False)
                if msg is None:
                    break
                await msg.ack()
    finally:
        await connection.close()


def _mgmt_queue(settings: Settings, queue_name: str) -> dict:
    """Read a queue through the management HTTP API. Credentials are parsed from
    RABBITMQ_URL at call time and are never printed or logged."""
    url = urlparse(settings.rabbitmq_url)
    token = base64.b64encode(
        f"{unquote(url.username or '')}:{unquote(url.password or '')}".encode()
    ).decode()
    vhost = unquote(url.path.lstrip("/")) or "/"
    req = urllib.request.Request(
        f"http://{url.hostname}:15672/api/queues/{vhost.replace('/', '%2F')}/{queue_name}",
        headers={"Authorization": f"Basic {token}"},
    )
    with urllib.request.urlopen(req, timeout=5) as resp:  # noqa: S310 - local dev broker
        return json.loads(resp.read())


async def queue_counts(settings: Settings, queue_name: str) -> tuple[int, int | None]:
    """Return (ready message_count, unacked or None).

    Ready comes from an AMQP passive declare. AMQP passive declare does not report
    unacked deliveries, so unacked is read from the management API (None if the
    API is unavailable).
    """
    connection = await connect(settings.rabbitmq_url)
    try:
        channel = await connection.channel()
        queue = await channel.declare_queue(queue_name, passive=True)
        ready = queue.declaration_result.message_count
    finally:
        await connection.close()
    try:
        info = await asyncio.to_thread(_mgmt_queue, settings, queue_name)
        unacked: int | None = int(info.get("messages_unacknowledged", 0))
    except Exception:  # noqa: BLE001 - management API is best effort
        unacked = None
    return ready, unacked


# ---------------------------------------------------------------------------
# Phase 6 helpers: running-line guard, generic queue purge, management API writes.
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parent.parent


def line_services_running() -> list[str]:
    """Names of running `line`-profile compose services ([] if docker is unavailable)."""
    try:
        result = subprocess.run(
            ["docker", "compose", "--profile", "line", "ps", "--status", "running", "--services"],
            capture_output=True,
            text=True,
            timeout=20,
            cwd=_REPO_ROOT,
        )
    except (OSError, subprocess.SubprocessError):
        return []
    if result.returncode != 0:
        return []
    # `ps --profile line` also lists profile-less infra services; those must stay up.
    infra = {"kafka", "rabbitmq", "init", "kafka-ui"}
    names = [name.strip() for name in result.stdout.splitlines() if name.strip()]
    return [name for name in names if name not in infra]


def require_line_down(allow: tuple[str, ...] = ()):
    """Autouse fixture factory: skip when line services (other than `allow`) are running.

    Usage in a test module: ``_line_guard = require_line_down()`` (Pitfall 12).
    """

    @pytest.fixture(autouse=True)
    def _guard():
        running = [name for name in line_services_running() if name not in allow]
        if running:
            pytest.skip(
                f"line profile is running ({', '.join(running)}); "
                "run `docker compose --profile line down` first (Pitfall 12)"
            )

    return _guard


async def purge_queues(settings: Settings, names: list[str]) -> None:
    """Drain arbitrary existing queues by get/ack (quorum-safe)."""
    connection = await connect(settings.rabbitmq_url)
    try:
        channel = await connection.channel()
        for name in names:
            queue = await channel.get_queue(name)
            while True:
                msg = await queue.get(fail=False)
                if msg is None:
                    break
                await msg.ack()
    finally:
        await connection.close()


def mgmt_delete(settings: Settings, kind: str, name: str) -> None:
    """DELETE /api/<queues|exchanges>/<vhost>/<name> through the management API.

    Credentials are parsed from RABBITMQ_URL at call time and never printed.
    """
    if kind not in ("queues", "exchanges"):
        raise ValueError(f"unsupported kind {kind!r}")
    url = urlparse(settings.rabbitmq_url)
    token = base64.b64encode(
        f"{unquote(url.username or '')}:{unquote(url.password or '')}".encode()
    ).decode()
    vhost = unquote(url.path.lstrip("/")) or "/"
    req = urllib.request.Request(
        f"http://{url.hostname}:15672/api/{kind}/{vhost.replace('/', '%2F')}/{name}",
        headers={"Authorization": f"Basic {token}"},
        method="DELETE",
    )
    try:
        with urllib.request.urlopen(req, timeout=5):  # noqa: S310 - local dev broker
            pass
    except urllib.error.HTTPError as exc:
        if exc.code != 404:
            raise
