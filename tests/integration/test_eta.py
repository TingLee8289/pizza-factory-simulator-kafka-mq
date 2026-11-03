"""Phase 7.1 ETA integration test on real Kafka and RabbitMQ (ORDER-06, ORDER-07, KPI-08).

Prerequisites: `docker compose up -d --wait` with the `line` profile DOWN (containers
would steal jobs). The line services and the dashboard run as subprocesses with scaled
recipes. The test starts its own run with a unique seed and resets it on teardown.
"""

import asyncio
import math
import subprocess
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import pytest

from pizza.common.config import Settings
from pizza.common.constants import STATIONS
from pizza.common.envelope import Message, OrderLifecycle
from pizza.common.topology import TOPIC_ORDER_LIFECYCLE
from pizza.kpi.eta import eta_error_s
from tests.conftest import (
    collect_topic,
    purge_line_queues,
    start_service,
    terminate,
    write_scaled_recipes,
)
from tests.integration.test_dashboard import (  # noqa: F401 - fixtures/helpers reused
    HEALTH_DEADLINE_S,
    SLOW_RATE,
    _bootstrap_topology,
    _clean_queues,
    _free_port,
    _mgmt_credentials,
)

pytestmark = pytest.mark.integration

SCALE = 0.25
UNIQUE_SEED = 7_100_171  # unique to this test: never collides with another test's run


@dataclass
class EtaStack:
    settings: Settings
    client: httpx.AsyncClient
    procs: dict[str, subprocess.Popen] = field(default_factory=dict)

    async def post(self, path: str, body: dict | None = None) -> httpx.Response:
        return await self.client.post(path, json=body) if body else await self.client.post(path)

    async def start_run(self, rate: float) -> str:
        resp = await self.post(
            "/api/run/start", {"arrival_rate_per_min": rate, "seed": UNIQUE_SEED}
        )
        assert resp.status_code == 200, resp.status_code
        return resp.json()["run_id"]

    async def lifecycle(self, run_id: str, until, timeout_s: float) -> list[Message]:
        return await asyncio.wait_for(
            asyncio.to_thread(
                collect_topic, self.settings, TOPIC_ORDER_LIFECYCLE, run_id, until, timeout_s
            ),
            timeout=timeout_s + 15,
        )


@pytest.fixture
async def eta_stack(brokers: Settings, tmp_path: Path) -> AsyncIterator[EtaStack]:
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
    client = httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", timeout=30.0)
    stack = EtaStack(brokers, client)
    try:
        for station in STATIONS:
            stack.procs[station] = start_service("pizza.station", brokers, env, station=station)
        stack.procs["scheduler"] = start_service(
            "pizza.scheduler", brokers, {**env, "RESUME_ON_START": "false"}
        )
        stack.procs["generator"] = start_service("pizza.generator", brokers, env)
        stack.procs["kpi"] = start_service("pizza.kpi", brokers, kpi_env)
        stack.procs["dashboard"] = start_service("pizza.dashboard", brokers, dash_env)

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
        state = (await client.get("/api/state")).json()
        if state["run"]["state"] != "none":
            assert (await client.post("/api/run/reset")).status_code == 200
        await asyncio.sleep(5.0)  # service consumers attached
        yield stack
        try:
            await client.post("/api/run/reset")  # run reset: leave no run behind
        except Exception:  # noqa: BLE001 - best effort cleanup
            pass
    finally:
        await client.aclose()
        for proc in stack.procs.values():
            terminate(proc, kill=True)


def _mine(events: list[Message], order_id: str) -> list[OrderLifecycle]:
    return [e for e in events if isinstance(e, OrderLifecycle) and e.order_id == order_id]


async def test_manual_order_eta_frozen_and_error_replayed(eta_stack: EtaStack) -> None:
    """A real manual order carries eta_ts/due_ts on created, and its ETA error is replayable."""
    run_id = await eta_stack.start_run(SLOW_RATE)
    resp = await eta_stack.post("/api/order", {"product": "margherita", "qty": 1, "buffer_k": 1.0})
    assert resp.status_code == 200, resp.status_code
    order_id = resp.json()["order_id"]

    def done(events: list[Message]) -> bool:
        return any(e.state == "completed" for e in _mine(events, order_id))

    events = await eta_stack.lifecycle(run_id, done, 90.0)
    mine = _mine(events, order_id)
    states = {e.state: e for e in mine}
    assert {"created", "released", "completed"} <= set(states), sorted(states)
    for state in ("created", "released"):
        fact = states[state]
        assert fact.eta_ts is not None, state
        assert fact.station_eta is not None and list(fact.station_eta) == list(STATIONS)
        times = list(fact.station_eta.values())
        assert all(a < b for a, b in zip(times, times[1:], strict=False)), times
        assert fact.due_ts >= fact.created_ts
    assert states["created"].eta_ts == states["released"].eta_ts

    # Replay with a fresh consumer group (collect_topic): the error derives from facts only.
    replay = await eta_stack.lifecycle(run_id, done, 90.0)
    replayed = {e.state: e for e in _mine(replay, order_id)}
    err = eta_error_s(replayed["completed"].ts, replayed["created"].eta_ts)
    assert err is not None and math.isfinite(err)


async def test_estimate_endpoint_is_read_only(eta_stack: EtaStack) -> None:
    """GET /api/order/estimate returns the ETA payload and creates no order fact."""
    run_id = await eta_stack.start_run(SLOW_RATE)

    def manual_created(events: list[Message]) -> list[str]:
        return [
            e.order_id
            for e in events
            if isinstance(e, OrderLifecycle) and e.state == "created" and e.source == "manual"
        ]

    before = manual_created(await eta_stack.lifecycle(run_id, lambda _e: False, 6.0))
    resp = await eta_stack.client.get(
        "/api/order/estimate", params={"product": "margherita", "qty": 1, "buffer_k": 1.0}
    )
    assert resp.status_code == 200, resp.status_code
    body: dict[str, Any] = resp.json()
    assert {"eta_ts", "station_eta", "due_ts", "lead_s", "buffer_k", "degraded"} <= set(body)
    assert isinstance(body["lead_s"], int | float) and body["lead_s"] > 0
    after = manual_created(await eta_stack.lifecycle(run_id, lambda _e: False, 6.0))
    assert after == before


async def test_scheduler_down_falls_back_to_theory_due(eta_stack: EtaStack) -> None:
    """Without the scheduler the estimate answers 422 and generated orders carry no ETA."""
    run_id = await eta_stack.start_run(12.0)
    terminate(eta_stack.procs["scheduler"], kill=True)
    resp = await eta_stack.client.get(
        "/api/order/estimate", params={"product": "margherita", "qty": 1, "buffer_k": 1.0}
    )
    assert resp.status_code == 422, resp.status_code

    def late_generated(events: list[Message]) -> bool:
        # A generated order created while the scheduler is down has no frozen ETA.
        return any(
            isinstance(e, OrderLifecycle)
            and e.state == "created"
            and e.source is None
            and e.eta_ts is None
            for e in events
        )

    events = await eta_stack.lifecycle(run_id, late_generated, 90.0)
    fallback = [
        e
        for e in events
        if isinstance(e, OrderLifecycle)
        and e.state == "created"
        and e.source is None
        and e.eta_ts is None
    ]
    assert fallback, "no generated order fell back to the theory due"
    assert all(e.station_eta is None and e.due_ts >= e.created_ts for e in fallback)
    await purge_line_queues(eta_stack.settings)
