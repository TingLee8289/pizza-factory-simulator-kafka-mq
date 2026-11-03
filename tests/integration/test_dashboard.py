"""Phase 5 dashboard backend integration tests on real Kafka and RabbitMQ (DASH-01..04, ORDER-03).

Prerequisites: `docker compose up -d --wait` with the `line` profile DOWN (containers
would steal jobs). The line services and the dashboard run as subprocesses with scaled
recipes (SCALE, not smaller than 0.25) so timing noise stays small.

Credentials come only from the `brokers` fixture settings; subprocess output is discarded
and no assertion message ever contains a broker URL (T-05-55).
"""

import asyncio
import json
import socket
import subprocess
import time
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import httpx
import pytest
from confluent_kafka import ConsumerGroupTopicPartitions
from confluent_kafka.admin import AdminClient, OffsetSpec

from pizza.common.config import Settings
from pizza.common.constants import STATIONS
from pizza.common.envelope import (
    KpiSnapshot,
    Message,
    OpCompleted,
    OrderLifecycle,
    PriorityChanged,
    RunPaused,
    RunResumed,
)
from pizza.common.rabbit_io import connect
from pizza.common.topology import (
    Q_DISPATCH_DLQ,
    Q_WORKORDERS_INTAKE,
    TOPIC_KPI_SNAPSHOTS,
    TOPIC_ORDER_LIFECYCLE,
    TOPIC_STATION_EVENTS,
    jobs_queue_name,
)
from pizza.dashboard.panel import compute_lag
from tests.conftest import (
    collect_topic,
    collect_topics,
    purge_line_queues,
    queue_counts,
    start_service,
    terminate,
    write_scaled_recipes,
)

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]
SCALE = 0.25
# Rate (orders/min) comfortably above bake capacity at SCALE 0.25 (max accepted is 12).
FAST_RATE = 12.0
SLOW_RATE = 2.0
HEALTH_DEADLINE_S = 150.0


@pytest.fixture(scope="module", autouse=True)
def _bootstrap_topology() -> None:
    """Rebuild the app image and idempotently declare topology."""
    for cmd in (["docker", "compose", "build"], ["docker", "compose", "run", "--rm", "init"]):
        result = subprocess.run(
            cmd, cwd=REPO_ROOT, capture_output=True, text=True, timeout=600, check=False
        )
        if result.returncode != 0:
            pytest.fail(
                f"`{' '.join(cmd)}` failed. If this is a queue argument mismatch, run "
                f"`docker compose down -v` and retry.\n{result.stderr[-1500:]}"
            )


async def _purge_dlq(settings: Settings) -> None:
    connection = await connect(settings.rabbitmq_url)
    try:
        channel = await connection.channel()
        queue = await channel.get_queue(Q_DISPATCH_DLQ)
        while True:
            msg = await queue.get(fail=False)
            if msg is None:
                break
            await msg.ack()
    finally:
        await connection.close()


@pytest.fixture(autouse=True)
async def _clean_queues(brokers: Settings):
    await purge_line_queues(brokers)
    await _purge_dlq(brokers)
    yield
    await purge_line_queues(brokers)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _mgmt_credentials(settings: Settings) -> tuple[str, str]:
    url = urlparse(settings.rabbitmq_url)
    return unquote(url.username or ""), unquote(url.password or "")


class Dash:
    """Handle to the running dashboard process plus the helper reads."""

    def __init__(self, settings: Settings, base_url: str, client: httpx.AsyncClient) -> None:
        self.settings = settings
        self.base_url = base_url
        self.client = client

    async def post(self, path: str, body: dict | None = None) -> httpx.Response:
        if body is None:
            return await self.client.post(path)
        return await self.client.post(path, json=body)

    async def state(self) -> dict[str, Any]:
        resp = await self.client.get("/api/state")
        assert resp.status_code == 200
        return resp.json()

    async def start_run(self, rate: float) -> str:
        resp = await self.post("/api/run/start", {"arrival_rate_per_min": rate})
        assert resp.status_code == 200, resp.status_code
        run_id = resp.json()["run_id"]
        assert run_id
        return run_id

    async def ticks(
        self, predicate: Callable[[str, dict], bool], deadline_s: float, what: str
    ) -> tuple[str, dict]:
        """Read /api/stream/state until `predicate(event, data)`; fail with diagnostics."""
        seen: list[str] = []
        try:
            async with asyncio.timeout(deadline_s):
                async with self.client.stream(
                    "GET", "/api/stream/state", timeout=httpx.Timeout(deadline_s + 5)
                ) as resp:
                    assert resp.status_code == 200, resp.status_code
                    event, data = "message", []
                    async for line in resp.aiter_lines():
                        if line.startswith("event:"):
                            event = line[6:].strip()
                        elif line.startswith("data:"):
                            data.append(line[5:].lstrip())
                        elif line == "" and data:
                            payload = json.loads("\n".join(data))
                            seen.append(event)
                            if predicate(event, payload):
                                return event, payload
                            event, data = "message", []
        except TimeoutError:
            pass
        pytest.fail(f"timeout ({deadline_s:.0f}s) waiting for: {what}; events seen: {seen[-5:]}")

    async def wait_busy(self, deadline_s: float = 60.0) -> dict:
        _, tick = await self.ticks(
            lambda e, d: e == "tick" and any(s["status"] == "busy" for s in d["stations"].values()),
            deadline_s,
            "a busy station",
        )
        return tick


@pytest.fixture
async def dash(brokers: Settings, tmp_path: Path) -> AsyncIterator[Dash]:
    """Scheduler, seven stations, kpi-service, generator and dashboard as subprocesses."""
    recipes_dir = write_scaled_recipes(tmp_path / "recipes", SCALE)
    env = {"RECIPES_DIR": str(recipes_dir)}
    user, password = _mgmt_credentials(brokers)
    port = _free_port()
    dash_env = {
        **env,
        "DASHBOARD_HOST": "127.0.0.1",
        "DASHBOARD_PORT": str(port),
        "RABBITMQ_MGMT_URL": "http://127.0.0.1:15672",
        "RABBITMQ_MGMT_USER": user,
        "RABBITMQ_MGMT_PASS": password,
        "PANEL_INTERVAL_S": "1.0",
    }
    kpi_env = {
        **env,
        "WARMUP_ORDERS": "5",
        "SNAPSHOT_INTERVAL_S": "0.5",
        "RESUME_ON_START": "false",
    }
    procs: list[subprocess.Popen] = []
    base_url = f"http://127.0.0.1:{port}"
    client = httpx.AsyncClient(base_url=base_url, timeout=30.0)
    try:
        for station in STATIONS:
            procs.append(start_service("pizza.station", brokers, env, station=station))
        procs.append(start_service("pizza.scheduler", brokers, {"RESUME_ON_START": "false"}))
        procs.append(start_service("pizza.generator", brokers, env))
        procs.append(start_service("pizza.kpi", brokers, kpi_env))
        procs.append(start_service("pizza.dashboard", brokers, dash_env))

        deadline = time.monotonic() + HEALTH_DEADLINE_S
        while True:
            try:
                if (await client.get("/healthz")).status_code == 200:
                    break
            except httpx.TransportError:
                pass
            if time.monotonic() > deadline:
                pytest.fail("dashboard /healthz did not become ready in time")
            await asyncio.sleep(1.0)

        handle = Dash(brokers, base_url, client)
        # The dashboard re-adopts the newest run from Kafka history; start from "none".
        if (await handle.state())["run"]["state"] != "none":
            assert (await handle.post("/api/run/reset")).status_code == 200
        await asyncio.sleep(5.0)  # service consumers attached
        yield handle
        try:
            if (await handle.state())["run"]["state"] != "none":
                await handle.post("/api/run/reset")
        except Exception:  # noqa: BLE001 - best effort cleanup
            pass
    finally:
        await client.aclose()
        for proc in procs:
            terminate(proc, kill=True)


# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------


async def read_facts(
    settings: Settings,
    run_id: str,
    until: Callable[[list[Message]], bool],
    timeout_s: float,
    topics: tuple[str, ...] = (TOPIC_STATION_EVENTS, TOPIC_ORDER_LIFECYCLE),
) -> list[Message]:
    return await asyncio.wait_for(
        asyncio.to_thread(collect_topics, settings, list(topics), run_id, until, timeout_s),
        timeout=timeout_s + 15,
    )


def _never(_events: list[Message]) -> bool:
    return False


def group_committed_and_lag(settings: Settings, group: str) -> tuple[int, int | None]:
    """(number of partitions with a committed offset, summed lag) for a consumer group."""
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    res = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group)])[group].result(10)
    wanted = [tp for tp in res.topic_partitions if tp.offset >= 0]
    if not wanted:
        return 0, None
    committed = {(tp.topic, tp.partition): tp.offset for tp in wanted}
    futures = admin.list_offsets({tp: OffsetSpec.latest() for tp in wanted})
    ends = {(tp.topic, tp.partition): f.result(10).offset for tp, f in futures.items()}
    return len(wanted), compute_lag(committed, ends)


# ---------------------------------------------------------------------------
# Task 1 tests: lag, panel, manual order, KPI relay
# ---------------------------------------------------------------------------


async def test_group_lag(dash: Dash) -> None:
    """After KafkaTail commits, AdminClient reports lag for the scheduler and kpi groups."""
    run_id = await dash.start_run(FAST_RATE)
    await dash.wait_busy()
    deadline = time.monotonic() + 60.0
    results: dict[str, tuple[int, int | None]] = {}
    while time.monotonic() < deadline:
        for group in ("scheduler", "kpi"):
            results[group] = await asyncio.to_thread(group_committed_and_lag, dash.settings, group)
        if all(n > 0 and lag is not None for n, lag in results.values()):
            break
        await asyncio.sleep(2.0)
    assert run_id
    for group in ("scheduler", "kpi"):
        n, lag = results[group]
        assert n > 0, f"no committed offsets for group {group}: {results}"
        assert lag is not None and lag >= 0, f"lag not computable for {group}: {results}"


async def test_panel_payload(dash: Dash) -> None:
    """Panel reports queue depths, DLQ count and lag keys; stats are fresh (A1)."""
    await dash.start_run(FAST_RATE)
    deadline = time.monotonic() + 60.0
    state: dict[str, Any] = {}
    while time.monotonic() < deadline:
        state = await dash.state()
        panel = state["panel"]
        if panel and panel.get("as_of") and panel["rabbit"]["queues"]:
            break
        await asyncio.sleep(1.0)
    panel = state["panel"]
    assert panel and panel.get("as_of"), "panel never populated"
    queues = panel["rabbit"]["queues"]
    assert Q_WORKORDERS_INTAKE in queues
    assert jobs_queue_name("bake") in queues
    assert panel["rabbit"]["dlq"] == 0
    assert set(panel["kafka"]["lag"]) >= {"scheduler", "kpi", "dashboard"}
    as_of = datetime.fromisoformat(panel["as_of"].replace("Z", "+00:00"))
    server_ts = datetime.fromisoformat(state["server_ts"].replace("Z", "+00:00"))
    age = (server_ts - as_of).total_seconds()
    print(f"panel age s={age:.2f} (A1 stats interval)")  # A1 calibration
    assert age < 6.0, age


async def test_manual_order(dash: Dash) -> None:
    """A manual order is released by the scheduler and completes through all seven stations."""
    before = await dash.post("/api/order", {"product": "margherita", "qty": 1, "priority": 10})
    assert before.status_code == 409  # no active run yet

    run_id = await dash.start_run(SLOW_RATE)
    resp = await dash.post("/api/order", {"product": "margherita", "qty": 1, "priority": 10})
    assert resp.status_code == 200, resp.status_code
    body = resp.json()
    order_id = body["order_id"]
    assert body["run_id"] == run_id
    assert body["priority"] == 10

    def done(events: list[Message]) -> bool:
        return any(
            isinstance(e, OrderLifecycle) and e.order_id == order_id and e.state == "completed"
            for e in events
        )

    events = await asyncio.wait_for(
        asyncio.to_thread(collect_topic, dash.settings, TOPIC_ORDER_LIFECYCLE, run_id, done, 90.0),
        timeout=105.0,
    )
    mine = [e for e in events if isinstance(e, OrderLifecycle) and e.order_id == order_id]
    states = [e.state for e in mine]
    assert "completed" in states, f"order {order_id} never completed; states={states}"
    created = next(e for e in mine if e.state == "created")
    assert created.source == "manual"
    assert created.priority == 10
    assert "released" in states

    ops = await read_facts(
        dash.settings,
        run_id,
        lambda ev: (
            len({e.step for e in ev if isinstance(e, OpCompleted) and e.order_id == order_id})
            >= len(STATIONS)
        ),
        30.0,
        topics=(TOPIC_STATION_EVENTS,),
    )
    steps = {e.step for e in ops if isinstance(e, OpCompleted) and e.order_id == order_id}
    assert len(steps) == len(STATIONS), steps


async def test_priority_change_emits_one_fact(dash: Dash) -> None:
    """POST a priority change on a live lot -> exactly one priority.changed on order.lifecycle."""
    run_id = await dash.start_run(SLOW_RATE)
    resp = await dash.post("/api/order", {"product": "margherita", "qty": 1, "priority": 50})
    assert resp.status_code == 200, resp.status_code
    order_id = resp.json()["order_id"]

    changed = None
    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:  # the lot may still be pre_release for a moment (409)
        changed = await dash.post(f"/api/lot/{order_id}/priority", {"priority": 5})
        if changed.status_code != 409:
            break
        await asyncio.sleep(0.5)
    assert changed is not None and changed.status_code == 200, changed
    bad = await dash.post(f"/api/lot/{order_id}/priority", {"priority": 100})
    assert bad.status_code == 422

    def seen(events: list[Message]) -> bool:
        return any(isinstance(e, PriorityChanged) and e.order_id == order_id for e in events)

    events = await asyncio.wait_for(
        asyncio.to_thread(collect_topic, dash.settings, TOPIC_ORDER_LIFECYCLE, run_id, seen, 30.0),
        timeout=45.0,
    )
    facts = [e for e in events if isinstance(e, PriorityChanged) and e.order_id == order_id]
    assert len(facts) == 1, facts
    assert facts[0].new_priority == 5 and facts[0].previous_priority == 50


async def test_kpi_relay_equals_topic(dash: Dash) -> None:
    """The kpi object in the state stream is a kpi.snapshots message verbatim (DASH-02)."""
    run_id = await dash.start_run(FAST_RATE)
    _, tick = await dash.ticks(
        lambda _e, d: d.get("kpi") is not None and d["kpi"]["orders_total"] >= 2,
        90.0,
        "a relayed kpi snapshot",
    )
    relayed = tick["kpi"]
    assert relayed["run_id"] == run_id

    events = await read_facts(dash.settings, run_id, _never, 12.0, topics=(TOPIC_KPI_SNAPSHOTS,))
    snaps = [e for e in events if isinstance(e, KpiSnapshot)]
    assert snaps, "no kpi.snapshots for the run"
    match = [s for s in snaps if str(s.event_id) == str(relayed["event_id"])]
    assert match, "relayed kpi is not a message of kpi.snapshots (recomputed?)"
    topic_kpi = match[0].model_dump(mode="json")
    for key in ("throughput_per_s", "wip_now", "cycle_time", "little"):
        assert relayed[key] == topic_kpi[key], key
    # Allow one tick of skew: the relayed snapshot is among the most recent few.
    assert snaps.index(match[0]) >= len(snaps) - 40


# ---------------------------------------------------------------------------
# Task 2 tests: stream, pause/resume freeze, reset
# ---------------------------------------------------------------------------


async def test_state_stream_shows_busy_and_queues(dash: Dash) -> None:
    await dash.start_run(FAST_RATE)
    _, tick = await dash.ticks(
        lambda e, d: (
            e == "tick"
            and any(s["status"] == "busy" for s in d["stations"].values())
            and any(s["queue"] > 0 for s in d["stations"].values())
        ),
        60.0,
        "a busy station and a non-zero queue",
    )
    assert set(tick["stations"]) == set(STATIONS)
    assert len(tick["stations"]) == 7
    state = await dash.state()
    assert set(state["line"]["stations"]) == set(STATIONS)


async def test_pause_freezes_line(dash: Dash) -> None:
    run_id = await dash.start_run(FAST_RATE)
    await dash.wait_busy()

    resp = await dash.post("/api/run/pause")
    assert resp.status_code == 200
    assert resp.json()["state"] == "paused"
    t_pause = datetime.now(UTC)
    await asyncio.sleep(3.0)  # let in-flight handling and the kpi snapshot settle
    _, tick_a = await dash.ticks(lambda _e, d: d.get("kpi") is not None, 30.0, "kpi tick (paused)")
    window_a = tick_a["kpi"]["window_s"] or 0.0
    assert tick_a["run"]["state"] == "paused"
    await asyncio.sleep(8.0)
    _, tick_b = await dash.ticks(lambda _e, d: d.get("kpi") is not None, 30.0, "kpi tick (later)")
    window_b = tick_b["kpi"]["window_s"] or 0.0
    assert tick_b["run"]["state"] == "paused"
    assert window_b - window_a < 1.0, (window_a, window_b)

    facts = await read_facts(dash.settings, run_id, _never, 12.0)
    cutoff = t_pause + timedelta(seconds=3.0)
    late = [e for e in facts if isinstance(e, OpCompleted) and e.ts > cutoff]
    assert late == [], f"{len(late)} op.completed while paused"
    assert any(isinstance(e, RunPaused) for e in facts), "no run.paused fact"

    resp = await dash.post("/api/run/resume")
    assert resp.status_code == 200
    assert resp.json()["state"] == "running"
    t_resume = datetime.now(UTC)
    after = await read_facts(
        dash.settings,
        run_id,
        lambda ev: any(isinstance(e, OpCompleted) and e.ts > t_resume for e in ev),
        60.0,
    )
    assert any(isinstance(e, OpCompleted) and e.ts > t_resume for e in after), (
        "no op.completed after resume"
    )
    assert any(isinstance(e, RunResumed) for e in after), "no run.resumed fact"


async def test_reset_new_run(dash: Dash) -> None:
    first = await dash.start_run(FAST_RATE)
    dup = await dash.post("/api/run/start", {"arrival_rate_per_min": FAST_RATE})
    assert dup.status_code == 409
    await dash.wait_busy()

    resp = await dash.post("/api/run/reset")
    assert resp.status_code == 200
    assert resp.json()["state"] == "none"
    assert (await dash.state())["run"]["state"] == "none"

    queues = [Q_WORKORDERS_INTAKE, *(jobs_queue_name(s) for s in STATIONS)]
    deadline = time.monotonic() + 20.0
    counts: dict[str, int] = {}
    while time.monotonic() < deadline:
        counts = {q: (await queue_counts(dash.settings, q))[0] for q in queues}
        if not any(counts.values()):
            break
        await asyncio.sleep(1.0)
    assert not any(counts.values()), f"queues not empty after reset: {counts}"

    second = await dash.start_run(FAST_RATE)
    assert second != first

    old = await read_facts(
        dash.settings,
        first,
        lambda ev: any(isinstance(e, OpCompleted) or isinstance(e, OrderLifecycle) for e in ev),
        30.0,
    )
    assert old, "first run facts vanished from Kafka (D-05)"
