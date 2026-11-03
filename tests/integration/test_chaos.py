"""Chaos test: docker-kill the bake station mid-job and assert redelivery (D-18, FAIL-04).

Run explicitly with `uv run pytest -m chaos tests/integration/test_chaos.py` while the
rest of the `line` profile is DOWN (only `station-bake` may be running). Uses the real
compose container, so the `pizza-app:dev` image must already be built.
"""

import asyncio
import subprocess
import sys
import time
import uuid

import pytest

from pizza.common.config import Settings
from pizza.common.constants import STATIONS
from pizza.common.dedupe import Deduper
from pizza.common.envelope import OpCompleted, OpStarted
from pizza.common.topology import jobs_queue_name
from tests.conftest import (
    _REPO_ROOT,
    collect_events,
    purge_station_queues,
    queue_counts,
    require_line_down,
)

pytestmark = [pytest.mark.integration, pytest.mark.chaos]

STATION = "bake"
SERVICE = f"station-{STATION}"
IMAGE = "pizza-app:dev"
_line_guard = require_line_down(allow=(SERVICE,))


def _docker(*args: str, timeout: float = 120.0) -> subprocess.CompletedProcess:
    """Run a docker CLI command from the repo root (argument list, never a shell)."""
    return subprocess.run(
        ["docker", *args],
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=_REPO_ROOT,
    )


def _compose(*args: str, timeout: float = 120.0) -> subprocess.CompletedProcess:
    return _docker("compose", "--profile", "line", *args, timeout=timeout)


def _running_services() -> list[str]:
    out = _compose("ps", "--status", "running", "--services", timeout=30.0).stdout
    return [line.strip() for line in out.splitlines() if line.strip()]


async def _send(*args: str) -> None:
    def _run() -> None:
        subprocess.run(
            [sys.executable, "-m", "pizza.tools.send_job", *args],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=60,
        )

    await asyncio.to_thread(_run)


async def _collect(settings: Settings, run_id: str, until, timeout_s: float):
    return await asyncio.wait_for(
        asyncio.to_thread(collect_events, settings, run_id, until, timeout_s),
        timeout=timeout_s + 15,
    )


async def test_chaos_kill_redelivery(brokers: Settings) -> None:
    assert STATION in STATIONS  # allowlist check before any docker call
    try:
        _docker("image", "inspect", IMAGE, timeout=30.0)
    except (subprocess.CalledProcessError, OSError):
        pytest.skip(f"image {IMAGE} not built; run `docker compose --profile line build` first")

    run_id, oid = f"it-{uuid.uuid4().hex[:12]}", f"o-{uuid.uuid4().hex[:8]}"
    await purge_station_queues(brokers, STATION)
    try:
        _compose("up", "-d", "--no-deps", SERVICE)
        await asyncio.sleep(5.0)
        await _send("start-run", "--station", STATION, "--run-id", run_id)

        # Wait until the container is alive on Kafka (heartbeat/status of the run).
        alive = await _collect(
            brokers, run_id, lambda ev: any(e.machine_id == STATION for e in ev), 60.0
        )
        assert any(e.machine_id == STATION for e in alive), "bake container never reported"

        await _send(
            "job",
            "--station",
            STATION,
            "--run-id",
            run_id,
            "--order-id",
            oid,
            "--unit-time-s",
            "8",
        )

        def busy(ev) -> bool:
            return any(isinstance(e, OpStarted) and e.order_id == oid for e in ev)

        events = await _collect(brokers, run_id, busy, 30.0)
        assert busy(events), "job never started (D-18: kill only when busy)"

        _compose("kill", SERVICE)
        await asyncio.sleep(3.0)
        _compose("start", SERVICE)  # idempotent: restart policy may have beaten us
        deadline = time.monotonic() + 30.0
        while SERVICE not in _running_services():
            assert time.monotonic() < deadline, f"{SERVICE} did not come back"
            await asyncio.sleep(1.0)

        def completed(ev) -> bool:
            return any(isinstance(e, OpCompleted) and e.order_id == oid for e in ev)

        events = await _collect(brokers, run_id, completed, 60.0)
        assert completed(events), "job was not redelivered and completed"
        # Let a duplicate completion (if any) land before counting.
        await asyncio.sleep(2.0)
        events = await _collect(brokers, run_id, lambda ev: False, 5.0)

        deduper = Deduper()
        recorded = [
            e
            for e in events
            if isinstance(e, OpCompleted)
            and e.order_id == oid
            and e.step == STATION
            and deduper.first_time_op(e)
        ]
        assert len(recorded) == 1, f"expected exactly one op.completed, got {len(recorded)}"
        started = [e for e in events if isinstance(e, OpStarted) and e.order_id == oid]
        assert any(e.redelivered is True and e.attempt >= 2 for e in started), [
            (e.redelivered, e.attempt) for e in started
        ]

        ready, unacked = 1, 1
        for _ in range(10):
            ready, unacked = await queue_counts(brokers, jobs_queue_name(STATION))
            if ready == 0 and unacked == 0:
                break
            await asyncio.sleep(1.0)
        assert ready == 0 and unacked == 0, (ready, unacked)
    finally:
        try:
            _compose("stop", SERVICE)
        finally:
            await purge_station_queues(brokers, STATION)
