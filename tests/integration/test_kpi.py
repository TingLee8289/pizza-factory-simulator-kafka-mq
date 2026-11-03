"""Phase 4 KPI service integration tests on real Kafka and RabbitMQ (KPI-01..KPI-05).

Prerequisites: `docker compose up -d --wait` with the `line` profile DOWN (containers
would steal jobs). Recipes are scaled by SCALE (not smaller than 0.25, Pitfall 7) so
the timing noise stays small relative to the processing times.
"""

import asyncio
import subprocess
import sys
import uuid
from collections.abc import Callable
from pathlib import Path

import pytest

from pizza.common.config import Settings
from pizza.common.constants import PRODUCTS, STATIONS
from pizza.common.envelope import KpiSnapshot, OrderLifecycle
from pizza.common.recipes import load_all_recipes
from pizza.common.topology import TOPIC_KPI_SNAPSHOTS, TOPIC_ORDER_LIFECYCLE
from pizza.kpi.theory import compute_theory, expected_qty
from tests.conftest import (
    collect_topic,
    last_kpi_snapshot,
    purge_line_queues,
    start_service,
    terminate,
    write_scaled_recipes,
)

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]
SCALE = 0.25
WARMUP = 5
OVERLOAD_FACTOR = 1.3
STABLE_FACTOR = 0.6
# Calibrated per research A1: measured throughput / theoretical capacity after >= 25
# completions from an overloaded line. Cumulative throughput from T0 is biased low by
# pipeline fill, so the lower bound is loose. The executor records the observed value
# in 04-07-SUMMARY.md.
CAPACITY_RATIO_MIN = 0.75
CAPACITY_RATIO_MAX = 1.15
MIN_COMPLETED_FOR_RATIO = 25


@pytest.fixture(scope="module", autouse=True)
def _bootstrap_topology() -> None:
    """Rebuild the app image and idempotently declare topology (incl. kpi.control)."""
    for cmd in (["docker", "compose", "build"], ["docker", "compose", "run", "--rm", "init"]):
        result = subprocess.run(
            cmd, cwd=REPO_ROOT, capture_output=True, text=True, timeout=600, check=False
        )
        if result.returncode != 0:
            pytest.fail(
                f"`{' '.join(cmd)}` failed. If this is a queue argument mismatch, run "
                f"`docker compose down -v` and retry.\n{result.stderr[-1500:]}"
            )


@pytest.fixture(autouse=True)
async def _clean_queues(brokers: Settings):
    await purge_line_queues(brokers)
    yield
    await purge_line_queues(brokers)


class KpiLine:
    def __init__(self, settings: Settings, recipes_dir: Path) -> None:
        self.settings = settings
        self.recipes_dir = recipes_dir
        recipes = load_all_recipes(str(recipes_dir))
        self.theory = compute_theory(recipes, dict.fromkeys(PRODUCTS, 1.0), expected_qty())
        self.bake_time_s = self.theory.expected_station_time_s["bake"]

    def rate(self, factor: float) -> float:
        return factor / self.bake_time_s

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

    async def snapshot(
        self, run_id: str, predicate: Callable[[KpiSnapshot], bool], timeout_s: float
    ) -> KpiSnapshot | None:
        return await asyncio.wait_for(
            asyncio.to_thread(last_kpi_snapshot, self.settings, run_id, predicate, timeout_s),
            timeout=timeout_s + 15,
        )


@pytest.fixture
async def kpi_line(brokers: Settings, tmp_path: Path):
    recipes_dir = write_scaled_recipes(tmp_path / "recipes", SCALE)
    env = {"RECIPES_DIR": str(recipes_dir)}
    kpi_env = {
        **env,
        "WARMUP_ORDERS": str(WARMUP),
        "SNAPSHOT_INTERVAL_S": "0.5",
        "KAFKA_GROUP_ID": f"kpi-it-{uuid.uuid4().hex[:8]}",
        "RESUME_ON_START": "false",
    }
    procs: list[subprocess.Popen] = []
    try:
        for station in STATIONS:
            procs.append(start_service("pizza.station", brokers, env, station=station))
        procs.append(start_service("pizza.scheduler", brokers, {"RESUME_ON_START": "false"}))
        procs.append(start_service("pizza.generator", brokers, env))
        # kpi service must be consuming kpi.control BEFORE start-run is sent.
        procs.append(start_service("pizza.kpi", brokers, kpi_env))
        await asyncio.sleep(8.0)  # consumers attached
        yield KpiLine(brokers, recipes_dir)
    finally:
        for proc in procs:
            terminate(proc, kill=True)


def _run_id() -> str:
    return f"kpi-{uuid.uuid4().hex[:12]}"


def _created_ids(events) -> set[str]:
    return {e.order_id for e in events if isinstance(e, OrderLifecycle) and e.state == "created"}


async def test_overload_reports_bake_bottleneck(kpi_line: KpiLine) -> None:
    run_id = _run_id()
    max_orders = WARMUP + 40
    assert kpi_line.theory.bottleneck_station == "bake"
    await kpi_line.start_run(run_id, 3, kpi_line.rate(OVERLOAD_FACTOR), max_orders)

    timeout_s = max_orders / kpi_line.theory.capacity_per_s * 2 + 120

    def ready(s: KpiSnapshot) -> bool:
        return s.orders_completed >= MIN_COMPLETED_FOR_RATIO and s.bottleneck is not None

    events = await asyncio.wait_for(
        asyncio.to_thread(
            collect_topic,
            kpi_line.settings,
            TOPIC_KPI_SNAPSHOTS,
            run_id,
            lambda ev: isinstance(ev[-1], KpiSnapshot) and ready(ev[-1]),
            timeout_s,
        ),
        timeout=timeout_s + 15,
    )
    snaps = [e for e in events if isinstance(e, KpiSnapshot)]
    assert snaps and ready(snaps[-1]), "no snapshot with >= 25 completions and a bottleneck"
    snap = snaps[-1]

    assert snap.bottleneck == "bake"
    assert snap.theory.bottleneck_station == "bake"
    assert snap.capacity_ratio is not None
    print(f"observed capacity_ratio={snap.capacity_ratio:.3f}")  # calibration (A1)
    assert CAPACITY_RATIO_MIN <= snap.capacity_ratio <= CAPACITY_RATIO_MAX, snap.capacity_ratio

    utils = {s: snap.stations[s].utilization for s in STATIONS}
    assert all(u is not None for u in utils.values()), utils
    assert utils["bake"] == max(utils.values()), utils
    assert snap.cycle_time.p50_s is not None and snap.cycle_time.p95_s is not None
    assert snap.theory.capacity_per_s is not None
    assert snap.theory.raw_process_time_s is not None
    assert snap.theory.critical_wip is not None
    assert snap.theory.weights_source in ("run_started", "observed", "uniform")

    # Non-flapping: once adopted, the reported bottleneck never changes.
    adopted = [s.bottleneck for s in snaps if s.bottleneck is not None]
    assert set(adopted) == {"bake"}, adopted


async def test_drained_run_little_passes(kpi_line: KpiLine) -> None:
    # Little's check is not expected to pass under sustained overload (WIP grows
    # without bound), so it is asserted only on a stable, fully drained finite run.
    run_id = _run_id()
    max_orders = WARMUP + 20
    await kpi_line.start_run(run_id, 5, kpi_line.rate(STABLE_FACTOR), max_orders)

    timeout_s = max_orders / kpi_line.theory.capacity_per_s * 2 + 120
    snap = await kpi_line.snapshot(
        run_id,
        lambda s: (
            s.orders_total == max_orders and s.wip_now == 0 and s.orders_completed == s.orders_total
        ),
        timeout_s,
    )
    assert snap is not None, "run did not drain in time"
    assert snap.little.status == "pass", snap.little
    assert snap.little.rel_error is not None
    assert snap.little.rel_error <= snap.little.tolerance


async def test_new_run_resets(kpi_line: KpiLine) -> None:
    run_a, run_b = _run_id(), _run_id()
    n = 3
    rate = kpi_line.rate(STABLE_FACTOR)
    timeout_s = n / kpi_line.theory.capacity_per_s * 2 + 120

    await kpi_line.start_run(run_a, 1, rate, n)
    snap_a = await kpi_line.snapshot(
        run_a, lambda s: s.orders_total == n and s.orders_completed == n, timeout_s
    )
    assert snap_a is not None, "run a did not finish"

    await kpi_line.start_run(run_b, 2, rate, n)
    snap_b = await kpi_line.snapshot(
        run_b, lambda s: s.orders_total == n and s.orders_completed == n, timeout_s
    )
    assert snap_b is not None, "no snapshot for the new run_id"
    assert snap_b.run_id == run_b
    assert snap_b.warming_up is True  # n < WARMUP
    assert snap_b.orders_total == n  # nothing carried over from run a

    lifecycle = await asyncio.to_thread(
        collect_topic,
        kpi_line.settings,
        TOPIC_ORDER_LIFECYCLE,
        run_b,
        lambda ev: len(_created_ids(ev)) >= n,
        30.0,
    )
    assert snap_b.orders_total == len(_created_ids(lifecycle))


def test_spec_has_no_warmup_s() -> None:
    """D-02 follow-up: the spec uses WARMUP_ORDERS; no WARMUP_S remains."""
    spec = REPO_ROOT / "docs" / "spec"
    hits = [
        str(p)
        for p in spec.rglob("*")
        if p.is_file() and "WARMUP_S" in p.read_text(encoding="utf-8", errors="ignore")
    ]
    assert hits == []
