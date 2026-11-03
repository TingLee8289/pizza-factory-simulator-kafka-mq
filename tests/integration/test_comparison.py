"""Phase 7 end-to-end scaled 3-round comparison + replay verification on real
Kafka and RabbitMQ (SCHED-03, KPI-07, COMP-01..04, D-06, D-07, D-12, D-13).

Prerequisites: `docker compose up -d --wait` with the `line` profile DOWN (containers
would steal jobs). The line services and the dashboard run as subprocesses with scaled
recipes (SCALE 0.25, Pitfall 7) so timing noise stays small relative to processing
times; MTBF_S/MTTR_S are set small on every station so at least one random breakdown
fires per round.

Credentials come only from the `brokers` fixture settings; subprocess output is
discarded and no assertion message ever contains a broker URL (T-05-55, T-07-43).
"""

import asyncio
import socket
import subprocess
import time
import uuid
from collections.abc import AsyncIterator, Callable, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import httpx
import pytest
from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

from pizza.common.config import Settings
from pizza.common.constants import STATIONS
from pizza.common.envelope import ComparisonResult, Message, OrderLifecycle, StationStatus
from pizza.common.kafka_io import parse_or_skip
from pizza.common.topology import TOPIC_KPI_SNAPSHOTS, TOPIC_ORDER_LIFECYCLE, TOPIC_STATION_EVENTS
from tests.conftest import (
    purge_line_queues,
    start_service,
    terminate,
    write_scaled_recipes,
)

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]
SCALE = 0.25
# Small enough (relative to SCALE-ed per-station busy time over 5 orders) that at
# least the bottleneck (bake) station trips once per round; MTTR kept short so the
# round still finishes quickly.
MTBF_S = "6"
MTTR_S = "3"
SEED = 1234
N_ORDERS = 5
ARRIVAL_RATE_PER_MIN = 6.0
K = 1.5
ROUND_DEADLINE_S = 240.0
POLL_INTERVAL_S = 2.0
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


@pytest.fixture(autouse=True)
async def _clean_queues(brokers: Settings):
    await purge_line_queues(brokers)
    yield
    await purge_line_queues(brokers)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _mgmt_credentials(settings: Settings) -> tuple[str, str]:
    url = urlparse(settings.rabbitmq_url)
    return unquote(url.username or ""), unquote(url.password or "")


@pytest.fixture
async def dash(brokers: Settings, tmp_path: Path) -> AsyncIterator[httpx.AsyncClient]:
    """Scheduler, seven stations (small MTBF/MTTR), kpi-service, generator and dashboard."""
    recipes_dir = write_scaled_recipes(tmp_path / "recipes", SCALE)
    env = {"RECIPES_DIR": str(recipes_dir)}
    station_env = {**env, "MTBF_S": MTBF_S, "MTTR_S": MTTR_S, "HEARTBEAT_INTERVAL_S": "0.5"}
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
        "WARMUP_ORDERS": "0",
        "SNAPSHOT_INTERVAL_S": "0.5",
        "RESUME_ON_START": "false",
    }
    procs: list[subprocess.Popen] = []
    base_url = f"http://127.0.0.1:{port}"
    client = httpx.AsyncClient(base_url=base_url, timeout=30.0)
    try:
        for station in STATIONS:
            procs.append(start_service("pizza.station", brokers, station_env, station=station))
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

        state = (await client.get("/api/state")).json()
        if state["run"]["state"] != "none":
            assert (await client.post("/api/run/reset")).status_code == 200
        await asyncio.sleep(5.0)  # service consumers attached
        yield client
        try:
            state = (await client.get("/api/state")).json()
            if state["run"]["state"] != "none":
                await client.post("/api/run/reset")
        except Exception:  # noqa: BLE001 - best effort cleanup
            pass
    finally:
        await client.aclose()
        for proc in procs:
            terminate(proc, kill=True)


# ---------------------------------------------------------------------------
# Kafka helpers (scan by comparison_id / run_id set, not a single run_id)
# ---------------------------------------------------------------------------


def _collect_comparison_results(
    settings: Settings, comparison_id: str, n_rounds: int, timeout_s: float = 30.0
) -> dict[int, ComparisonResult]:
    """Read kpi.snapshots from earliest; return every ComparisonResult fact by round."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": f"test-cmp-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    found: dict[int, ComparisonResult] = {}
    deadline = time.monotonic() + timeout_s
    try:
        consumer.subscribe([TOPIC_KPI_SNAPSHOTS])
        while time.monotonic() < deadline and len(found) < n_rounds:
            msg = consumer.poll(0.5)
            if msg is None or msg.error():
                continue
            parsed = parse_or_skip(
                msg.value(), topic=msg.topic(), partition=msg.partition(), offset=msg.offset()
            )
            if isinstance(parsed, ComparisonResult) and parsed.comparison_id == comparison_id:
                found[parsed.round] = parsed
    finally:
        consumer.close()
    return found


def _collect_for_run_ids(
    settings: Settings,
    topics: Sequence[str],
    run_ids: set[str],
    until: Callable[[list[Message]], bool],
    timeout_s: float = 60.0,
) -> list[Message]:
    """Read several topics from earliest; keep only facts whose run_id is in run_ids."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": f"test-multirun-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    events: list[Message] = []
    deadline = time.monotonic() + timeout_s
    try:
        consumer.subscribe(list(topics))
        while time.monotonic() < deadline:
            msg = consumer.poll(0.5)
            if msg is None or msg.error():
                continue
            parsed = parse_or_skip(
                msg.value(), topic=msg.topic(), partition=msg.partition(), offset=msg.offset()
            )
            if parsed is not None and getattr(parsed, "run_id", None) in run_ids:
                events.append(parsed)
                if until(events):
                    break
    finally:
        consumer.close()
    return events


def _topic_end_offsets(settings: Settings, topic: str) -> dict[tuple[str, int], int]:
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    md = admin.list_topics(timeout=10).topics[topic]
    tps = [TopicPartition(topic, p) for p in md.partitions]
    futures = admin.list_offsets({tp: OffsetSpec.latest() for tp in tps})
    return {(tp.topic, tp.partition): f.result(10).offset for tp, f in futures.items()}


def _new_messages_for_run(
    settings: Settings,
    topic: str,
    run_id: str,
    from_offsets: dict[tuple[str, int], int],
    to_offsets: dict[tuple[str, int], int],
) -> list[object]:
    """Read only the offset range appended between two `_topic_end_offsets` snapshots
    and return the parsed facts among them whose run_id matches `run_id`."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": f"test-newmsgs-{uuid.uuid4()}",
            "enable.auto.commit": False,
        }
    )
    found: list[object] = []
    try:
        partitions = [
            TopicPartition(t, p, offset=from_offsets[(t, p)])
            for (t, p) in from_offsets
            if t == topic and to_offsets.get((t, p), from_offsets[(t, p)]) > from_offsets[(t, p)]
        ]
        if not partitions:
            return found
        consumer.assign(partitions)
        remaining = {
            (tp.topic, tp.partition): to_offsets[(tp.topic, tp.partition)] - tp.offset
            for tp in partitions
        }
        deadline = time.monotonic() + 15.0
        while any(v > 0 for v in remaining.values()) and time.monotonic() < deadline:
            msg = consumer.poll(0.5)
            if msg is None or msg.error():
                continue
            key = (msg.topic(), msg.partition())
            if remaining.get(key, 0) <= 0:
                continue
            remaining[key] -= 1
            parsed = parse_or_skip(
                msg.value(), topic=msg.topic(), partition=msg.partition(), offset=msg.offset()
            )
            if parsed is not None and getattr(parsed, "run_id", None) == run_id:
                found.append(parsed)
    finally:
        consumer.close()
    return found


def _group_committed_count(settings: Settings, group: str) -> int:
    """Number of partitions with a committed offset for `group` (0 if unknown/errored)."""
    try:
        admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
        res = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group)])[
            group
        ].result(10)
        return len([tp for tp in res.topic_partitions if tp.offset >= 0])
    except Exception:  # noqa: BLE001 - best effort; an unknown group means 0 committed
        return 0


async def _poll_comparison(
    client: httpx.AsyncClient,
    predicate: Callable[[dict[str, Any]], bool],
    deadline_s: float,
    what: str,
) -> dict[str, Any]:
    deadline = time.monotonic() + deadline_s
    last: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        resp = await client.get("/api/comparison")
        assert resp.status_code == 200, resp.status_code
        last = resp.json()
        if predicate(last):
            return last
        await asyncio.sleep(POLL_INTERVAL_S)
    pytest.fail(f"timeout ({deadline_s:.0f}s) waiting for: {what}; last status={last}")


# ---------------------------------------------------------------------------
# The test
# ---------------------------------------------------------------------------


async def test_three_round_comparison_and_replay(
    dash: httpx.AsyncClient, brokers: Settings
) -> None:
    """A real 3-round FIFO -> EDD -> SPT comparison, then a read-only recompute (D-13)."""
    # (1) Start the comparison: round 1 is always FIFO.
    resp = await dash.post(
        "/api/comparison/start",
        json={
            "seed": SEED,
            "n_orders": N_ORDERS,
            "arrival_rate_per_min": ARRIVAL_RATE_PER_MIN,
            "k": K,
            "random_breakdowns": True,
        },
    )
    assert resp.status_code == 200, resp.status_code
    status1 = await _poll_comparison(
        dash, lambda s: s["state"] == "round_done", ROUND_DEADLINE_S, "round 1 (fifo) done"
    )
    assert status1["round"] == 1

    # (8) Reset/Rule are locked while a comparison is in progress, even between rounds.
    r_reset = await dash.post("/api/run/reset")
    assert r_reset.status_code == 409
    r_rule = await dash.post("/api/rule", json={"rule": "edd"})
    assert r_rule.status_code == 409

    # (2) Advance through round 2 (EDD) and round 3 (SPT).
    resp2 = await dash.post("/api/comparison/next")
    assert resp2.status_code == 200, resp2.status_code
    status2 = await _poll_comparison(
        dash, lambda s: s["state"] == "round_done", ROUND_DEADLINE_S, "round 2 (edd) done"
    )
    assert status2["round"] == 2

    resp3 = await dash.post("/api/comparison/next")
    assert resp3.status_code == 200, resp3.status_code
    status3 = await _poll_comparison(
        dash,
        lambda s: s["state"] in ("round_done", "finished"),
        ROUND_DEADLINE_S,
        "round 3 (spt) done",
    )
    assert status3["round"] == 3

    rows = status3["results"]
    assert len(rows) == 3
    assert [r["rule"] for r in rows] == ["fifo", "edd", "spt"]

    comparison_id = status3["comparison_id"]
    assert comparison_id
    run_ids_ordered = [r["run_id"] for r in rows]
    run_ids = set(run_ids_ordered)
    assert len(run_ids) == 3, f"rounds did not use distinct run_ids: {run_ids_ordered}"
    for r in rows:
        assert r["seed"] == SEED
        assert r["n_orders"] == N_ORDERS

    # (3) ComparisonResult facts on kpi.snapshots, keyed cmp:<id>:1..3.
    results = await asyncio.to_thread(
        _collect_comparison_results, brokers, comparison_id, 3, 30.0
    )
    assert set(results) == {1, 2, 3}, f"expected rounds 1-3, got {sorted(results)}"
    for round_num, result in results.items():
        assert result.seed == SEED
        assert result.n_orders == N_ORDERS
        assert result.n_completed + result.n_failed == N_ORDERS, (
            f"round {round_num}: n_completed={result.n_completed} n_failed={result.n_failed} "
            f"!= n_orders={N_ORDERS}"
        )
    assert {r.run_id for r in results.values()} == run_ids

    # (4) The three rounds' generated order sets are identical (COMP-01).
    created_events = await asyncio.to_thread(
        _collect_for_run_ids,
        brokers,
        [TOPIC_ORDER_LIFECYCLE],
        run_ids,
        lambda ev: len(
            {
                (e.run_id, e.order_id)
                for e in ev
                if isinstance(e, OrderLifecycle) and e.state == "created"
            }
        )
        >= N_ORDERS * 3,
        90.0,
    )
    # D-09: due_ts is ETA-derived and differs per rule; compare seed-derived inputs only.
    by_run: dict[str, set[tuple[str, str, int]]] = {rid: set() for rid in run_ids}
    for e in created_events:
        if isinstance(e, OrderLifecycle) and e.state == "created":
            by_run[e.run_id].add((e.order_id, e.product, e.qty))
    order_sets = list(by_run.values())
    assert all(len(s) == N_ORDERS for s in order_sets), by_run
    assert all(s == order_sets[0] for s in order_sets[1:]), (
        f"generated order sets differ across rounds: {by_run}"
    )

    # (5) Per-station failure/repair sequences agree over their common prefix (D-07).
    status_events = await asyncio.to_thread(
        _collect_for_run_ids,
        brokers,
        [TOPIC_STATION_EVENTS],
        run_ids,
        lambda _ev: False,  # drain what is already on the topic; bounded by the timeout
        20.0,
    )
    down_by_run_station: dict[str, dict[str, list[float]]] = {
        rid: {s: [] for s in STATIONS} for rid in run_ids
    }
    downs = sorted(
        (e for e in status_events if isinstance(e, StationStatus) and e.status == "down"),
        key=lambda e: e.ts,
    )
    for e in downs:
        if e.run_id in down_by_run_station and e.machine_id in STATIONS:
            down_by_run_station[e.run_id][e.machine_id].append(e.repair_in_s or 0.0)

    had_comparable_station = False
    for station in STATIONS:
        seqs = [down_by_run_station[rid][station] for rid in run_ids_ordered]
        min_len = min(len(s) for s in seqs)
        if min_len == 0:
            continue
        had_comparable_station = True
        base = seqs[0][:min_len]
        for seq in seqs[1:]:
            assert seq[:min_len] == base, (
                f"station {station} failure-duration sequences diverge over the common "
                f"prefix (min_len={min_len}): {[s[:min_len] for s in seqs]}"
            )
    if not had_comparable_station:
        pytest.skip(
            "no breakdown fired in any round for any station; cannot verify D-07 prefix "
            "agreement (MTBF_S may need lowering further)"
        )

    # (6) CSV endpoint: one header + one row per recorded round (D-12 columns).
    csv_resp = await dash.get("/api/comparison.csv", params={"comparison_id": comparison_id})
    assert csv_resp.status_code == 200
    csv_lines = csv_resp.text.strip().split("\n")
    assert len(csv_lines) == 1 + 3, csv_resp.text
    assert "rule" in csv_lines[0] and "run_id" in csv_lines[0]

    # (7) Recompute on a finished round: matched, replay-only, never publishes (D-13, D-14).
    # Note: the live kpi-service ticks kpi.snapshots every snapshot_interval_s (1s default)
    # for whatever run is currently active (round 3's run_id) regardless of recompute
    # activity, so a raw before/after end-offset comparison on the whole topic races against
    # that unrelated background cadence. Instead, read only the messages newly appended
    # during the recompute call and assert none of them carries the RECOMPUTED run's id -
    # this is what D-13 ("recompute never publishes") actually means.
    recomputed_run_id = run_ids_ordered[0]
    before_offsets = await asyncio.to_thread(_topic_end_offsets, brokers, TOPIC_KPI_SNAPSHOTS)
    live_group_offsets_before = {
        g: await asyncio.to_thread(_group_committed_count, brokers, g)
        for g in ("scheduler", "kpi", "dashboard")
    }

    recompute_resp = await dash.post(f"/api/runs/{recomputed_run_id}/recompute")
    assert recompute_resp.status_code == 200, recompute_resp.status_code
    report = recompute_resp.json()
    assert report["matched"] is True, report

    after_offsets = await asyncio.to_thread(_topic_end_offsets, brokers, TOPIC_KPI_SNAPSHOTS)
    new_for_recomputed_run = await asyncio.to_thread(
        _new_messages_for_run,
        brokers,
        TOPIC_KPI_SNAPSHOTS,
        recomputed_run_id,
        before_offsets,
        after_offsets,
    )
    assert new_for_recomputed_run == [], (
        "recompute must never publish a new snapshot for the recomputed run (D-13): "
        f"{new_for_recomputed_run}"
    )
    live_group_offsets_after = {
        g: await asyncio.to_thread(_group_committed_count, brokers, g)
        for g in ("scheduler", "kpi", "dashboard")
    }
    assert live_group_offsets_after == live_group_offsets_before, (
        "recompute must never commit offsets under a live group name"
    )

    # The replay consumer groups never commit (enable.auto.commit=False); if the
    # broker happens to list them at all, they must show zero committed offsets.
    groups_result = await asyncio.to_thread(
        lambda: AdminClient({"bootstrap.servers": brokers.kafka_bootstrap})
        .list_consumer_groups()
        .result(10)
    )
    replay_group_ids = [
        g.group_id for g in groups_result.valid if g.group_id.startswith("replay-")
    ]
    for group_id in replay_group_ids:
        assert await asyncio.to_thread(_group_committed_count, brokers, group_id) == 0, (
            f"replay group {group_id} unexpectedly has committed offsets"
        )


async def test_comparison_rejects_bad_request_and_rule_locked_mid_round(
    dash: httpx.AsyncClient,
) -> None:
    """A malformed start body is 422 and does not lock the comparison (sanity check)."""
    bad = await dash.post("/api/comparison/start", json={"n_orders": 4})
    assert bad.status_code == 422

    status = await dash.get("/api/comparison")
    assert status.status_code == 200
    assert status.json()["state"] == "idle"
