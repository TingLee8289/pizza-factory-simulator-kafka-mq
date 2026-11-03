"""Full-line integration tests against real Kafka and RabbitMQ (D-19).

Prerequisites: infra-only compose must be up (`docker compose up -d --wait`) and
the `line` profile must be DOWN, otherwise containers steal jobs. Recipes are
scaled by 0.05 so a whole run takes seconds. Each test uses a fresh run_id, and
all line queues are purged before and after every test.
"""

import asyncio
import subprocess
import sys
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pytest

from pizza.common.config import Settings
from pizza.common.constants import STATIONS
from pizza.common.dedupe import Deduper
from pizza.common.envelope import (
    OpCompleted,
    OpDispatched,
    OrderLifecycle,
    RunParams,
)
from pizza.common.recipes import load_all_recipes
from pizza.common.topology import TOPIC_ORDER_LIFECYCLE, TOPIC_STATION_EVENTS
from pizza.generator.plan import plan_orders
from tests.conftest import (
    collect_topics,
    purge_line_queues,
    start_service,
    terminate,
    write_scaled_recipes,
)

pytestmark = pytest.mark.integration

SCALE = 0.05
TIMEOUT = 90.0
TOPICS = [TOPIC_ORDER_LIFECYCLE, TOPIC_STATION_EVENTS]


@pytest.fixture(autouse=True)
async def _clean_queues(brokers: Settings):
    await purge_line_queues(brokers)
    yield
    await purge_line_queues(brokers)


@dataclass
class Line:
    settings: Settings
    recipes_dir: Path
    scheduler: subprocess.Popen
    procs: list[subprocess.Popen] = field(default_factory=list)

    async def start_run(self, run_id: str, seed: int, rate: float, max_orders: int) -> None:
        def _run() -> None:
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pizza.tools.send_job",
                    "start-run",
                    "--run-id",
                    run_id,
                    "--seed",
                    str(seed),
                    "--arrival-rate",
                    str(rate),
                    "--max-orders",
                    str(max_orders),
                ],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=60,
            )

        await asyncio.to_thread(_run)


@pytest.fixture
async def line(brokers: Settings, tmp_path: Path):
    recipes_dir = write_scaled_recipes(tmp_path / "recipes", SCALE)
    env = {"RECIPES_DIR": str(recipes_dir)}
    procs: list[subprocess.Popen] = []
    try:
        for station in STATIONS:
            procs.append(start_service("pizza.station", brokers, env, station=station))
        scheduler = start_service("pizza.scheduler", brokers, {"RESUME_ON_START": "false"})
        procs.append(scheduler)
        procs.append(start_service("pizza.generator", brokers, env))
        await asyncio.sleep(5.0)  # consumers attached
        yield Line(brokers, recipes_dir, scheduler, procs)
    finally:
        for proc in procs:
            terminate(proc, kill=True)


def _new_run_id() -> str:
    return f"it-{uuid.uuid4().hex[:12]}"


def _completed_orders(events, kind=OrderLifecycle, state="completed") -> set[str]:
    return {e.order_id for e in events if isinstance(e, kind) and e.state == state}


async def _collect(line_or_settings, run_id: str, until, timeout_s: float = TIMEOUT):
    settings = getattr(line_or_settings, "settings", line_or_settings)
    return await asyncio.wait_for(
        asyncio.to_thread(collect_topics, settings, TOPICS, run_id, until, timeout_s),
        timeout=timeout_s + 15,
    )


async def _run_to_completion(
    line: Line, run_id: str, seed: int, rate: float, n: int, timeout_s: float = TIMEOUT
):
    await line.start_run(run_id, seed, rate, n)
    events = await _collect(line, run_id, lambda ev: len(_completed_orders(ev)) >= n, timeout_s)
    assert len(_completed_orders(events)) == n, "run did not complete in time"
    # Let trailing op facts of the last step land.
    return events


def _lifecycle_by_order(events) -> dict[str, list[OrderLifecycle]]:
    out: dict[str, list[OrderLifecycle]] = defaultdict(list)
    for e in events:
        if isinstance(e, OrderLifecycle):
            out[e.order_id].append(e)
    for facts in out.values():
        facts.sort(key=lambda f: f.ts)
    return out


def _recorded_ops(events, run_id: str) -> list[OpCompleted]:
    deduper = Deduper()
    return [
        e
        for e in events
        if isinstance(e, OpCompleted) and e.run_id == run_id and deduper.first_time_op(e)
    ]


def _expected_orders(seed: int, rate: float, n: int, recipes_dir: Path, t0: datetime):
    params = RunParams(seed=seed, arrival_rate=rate, max_orders=n)
    return list(plan_orders(params, t0, load_all_recipes(str(recipes_dir))))


async def test_end_to_end(line: Line) -> None:
    run_id, seed, n = _new_run_id(), 7, 5
    expected = _expected_orders(seed, 2.0, n, line.recipes_dir, datetime.now().astimezone())
    assert len({o.product for o in expected}) >= 2, "choose a seed with a product mix"

    events = await _run_to_completion(line, run_id, seed, 2.0, n)
    ops = _recorded_ops(events, run_id)
    scaled = load_all_recipes(str(line.recipes_dir))

    by_order: dict[str, list[OpCompleted]] = defaultdict(list)
    for op in ops:
        by_order[op.order_id].append(op)
    assert len(by_order) == n
    products = set()
    for order_id, order_ops in by_order.items():
        assert [o.step for o in sorted(order_ops, key=lambda o: o.ts)] == list(STATIONS), order_id
        for op in order_ops:
            assert op.product is not None
            products.add(op.product)
            # 50 ms slack: Windows timer granularity wakes sleeps slightly early.
            assert op.duration_s >= scaled[op.step][op.product] * op.qty - 0.05
    assert len(products) >= 2


async def test_lifecycle(line: Line) -> None:
    run_id, n = _new_run_id(), 5
    events = await _run_to_completion(line, run_id, 7, 2.0, n)
    by_order = _lifecycle_by_order(events)
    assert len(by_order) == n
    for order_id, facts in by_order.items():
        # Kafka key is order_id, so per-order ordering is guaranteed; dedupe replays.
        states: list[str] = []
        for fact in facts:
            if fact.state not in states:
                states.append(fact.state)
        assert states == ["created", "released", "in_process", "completed"], order_id
        first = {}
        for fact in facts:
            first.setdefault(fact.state, fact.ts)
        assert first["released"] <= first["in_process"] <= first["completed"]


async def test_seed_reproducibility(line: Line) -> None:
    n, seed, rate = 4, 11, 3.0
    runs = []
    for _ in range(2):
        run_id = _new_run_id()
        events = await _run_to_completion(line, run_id, seed, rate, n)
        created = sorted(
            (e for e in events if isinstance(e, OrderLifecycle) and e.state == "created"),
            key=lambda e: e.created_ts,
        )
        seen, seq = set(), []
        for e in created:
            if e.order_id not in seen:
                seen.add(e.order_id)
                seq.append((e.order_id, e.product, e.qty))
        runs.append(seq)
        await asyncio.sleep(3.0)  # let the line go idle before the next run
    assert len(runs[0]) == n
    assert runs[0] == runs[1]
    planned = _expected_orders(seed, rate, n, line.recipes_dir, datetime.now().astimezone())
    assert runs[0] == [(o.order_id, o.product, o.qty) for o in planned]


async def test_one_outstanding(line: Line) -> None:
    run_id, n = _new_run_id(), 6
    events = await _run_to_completion(line, run_id, 5, 3.0, n)
    completed_ts: dict[uuid.UUID, datetime] = {}
    for e in events:
        if isinstance(e, OpCompleted):
            completed_ts.setdefault(e.command_event_id, e.ts)
    for station in STATIONS:
        seen, dispatched = set(), []
        for e in sorted((e for e in events if isinstance(e, OpDispatched)), key=lambda e: e.ts):
            if e.step == station and e.command_event_id not in seen:
                seen.add(e.command_event_id)
                dispatched.append(e)
        assert len(dispatched) == n, station
        for prev, nxt in zip(dispatched, dispatched[1:], strict=False):
            assert prev.command_event_id in completed_ts, (station, "no completion")
            assert nxt.ts >= completed_ts[prev.command_event_id], (station, "overlap")


async def test_bake_bottleneck(line: Line) -> None:
    run_id, n = _new_run_id(), 10
    events = await _run_to_completion(line, run_id, 3, 1.0, n, timeout_s=120.0)
    waits: dict[str, list[float]] = defaultdict(list)
    intervals: list[tuple[datetime, datetime]] = []
    seen = set()
    for e in events:
        if not isinstance(e, OpDispatched) or e.command_event_id in seen:
            continue
        seen.add(e.command_event_id)
        wait = max(0.0, (e.ts - e.ready_ts).total_seconds())
        waits[e.step].append(wait)
        if e.step == "bake":
            intervals.append((e.ready_ts, e.ts))
    assert all(len(waits[s]) == n for s in STATIONS), "every order visits every station"
    mean = {s: sum(v) / len(v) for s, v in waits.items()}
    for station in STATIONS:
        if station != "bake":
            assert mean["bake"] > mean[station], (station, mean)

    points = []
    for start, end in intervals:
        if end > start:
            points += [(start, 1), (end, -1)]
    points.sort(key=lambda p: (p[0], p[1]))
    peak = cur = 0
    for _, delta in points:
        cur += delta
        peak = max(peak, cur)
    assert peak >= 2, f"bake backlog peak {peak}"
    assert len(_completed_orders(events)) == n


async def test_scheduler_restart(line: Line) -> None:
    """Kill -9 the scheduler mid-run; a restarted one rebuilds from Kafka (D-14).

    Valid only because this test starts its own run last, so the newest-run
    adoption heuristic (research A2) picks it in order.lifecycle.
    """
    run_id, n = _new_run_id(), 8
    await line.start_run(run_id, 9, 1.5, n)

    def spin_done(ev) -> bool:
        spun = sum(isinstance(e, OpCompleted) and e.step == "spin" for e in ev)
        return spun >= 2

    events = await _collect(line, run_id, spin_done, 60.0)
    assert len(_completed_orders(events)) < n, "run finished before the kill"

    terminate(line.scheduler, kill=True)
    line.procs.remove(line.scheduler)
    restarted = start_service("pizza.scheduler", line.settings, {"RESUME_ON_START": "true"})
    line.procs.append(restarted)

    events = await _collect(line, run_id, lambda ev: len(_completed_orders(ev)) >= n, 120.0)
    done = _completed_orders(events)
    assert len(done) == n
    released = _completed_orders(events, state="released")
    assert released == done

    ops = _recorded_ops(events, run_id)
    per_order: dict[str, list[str]] = defaultdict(list)
    for op in ops:
        per_order[op.order_id].append(op.step)
    assert set(per_order) == done
    for order_id, steps in per_order.items():
        assert sorted(steps) == sorted(STATIONS), (order_id, steps)
