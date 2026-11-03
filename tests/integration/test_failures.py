"""Failure-path integration tests against real Kafka and RabbitMQ (Phase 6, D-24).

Prerequisites: infra-only compose is up (`docker compose up -d --wait`) and the `line`
profile is DOWN. Every test skips with an explicit message when line services are
running (Pitfall 12). Recipes are scaled by 0.05; each test uses a fresh run_id and
unique order ids, and every spawned process is terminated in `finally`.
"""

import asyncio
import subprocess
import sys
import time
import uuid
from datetime import timedelta
from pathlib import Path

import aio_pika
import pytest

from pizza.common.config import Settings
from pizza.common.constants import STATIONS
from pizza.common.envelope import (
    LotBanked,
    LotReleased,
    LotStocked,
    OpCompleted,
    OpDispatched,
    OpFailed,
    OpStarted,
    OrderLifecycle,
    StationStatus,
)
from pizza.common.kafka_io import make_producer, produce_fact
from pizza.common.rabbit_io import connect
from pizza.common.topology import (
    Q_DISPATCH_DLQ,
    Q_REWORK_RETURN,
    TOPIC_ORDER_LIFECYCLE,
    TOPIC_STATION_EVENTS,
    jobs_queue_name,
    retry_queue_name,
    rework_hold_queue_name,
)
from tests.conftest import (
    collect_topics,
    mgmt_delete,
    purge_line_queues,
    purge_queues,
    queue_counts,
    require_line_down,
    start_service,
    terminate,
    write_scaled_recipes,
)

pytestmark = pytest.mark.integration

SCALE = 0.05
TOPICS = [TOPIC_ORDER_LIFECYCLE, TOPIC_STATION_EVENTS]
_line_guard = require_line_down()
# Epoch ms at which the current test started; `_collect` reads from here instead of the
# whole topic history (a live line leaves hundreds of thousands of facts behind, and an
# earliest read then lags seconds behind the station, wrecking every timing window).
_since_ms: int | None = None


def _extra_queues() -> list[str]:
    return [
        Q_DISPATCH_DLQ,
        Q_REWORK_RETURN,
        *(retry_queue_name(s) for s in STATIONS),
        *(rework_hold_queue_name(s) for s in STATIONS),
    ]


@pytest.fixture(autouse=True)
async def _clean_queues(brokers: Settings, _line_guard):
    global _since_ms
    _since_ms = int(time.time() * 1000) - 5_000  # 5 s slack for clock jitter
    await purge_line_queues(brokers)
    await purge_queues(brokers, _extra_queues())
    yield
    await purge_line_queues(brokers)
    await purge_queues(brokers, _extra_queues())


def _ids() -> tuple[str, str]:
    return f"it-{uuid.uuid4().hex[:12]}", f"o-{uuid.uuid4().hex[:8]}"


async def _cli(*args: str) -> None:
    """Run `python -m pizza.tools.send_job <args>` (argument list, no shell)."""

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
        asyncio.to_thread(collect_topics, settings, TOPICS, run_id, until, timeout_s, _since_ms),
        timeout=timeout_s + 15,
    )


def _statuses(events, machine: str, status: str | None = None) -> list[StationStatus]:
    out = [e for e in events if isinstance(e, StationStatus) and e.machine_id == machine]
    if status is not None:
        out = [e for e in out if e.status == status]
    return sorted(out, key=lambda e: e.ts)


# ---------------------------------------------------------------------------
# 1. Delivery-limit semantics on a private probe queue (never touches line queues)
# ---------------------------------------------------------------------------
async def test_delivery_limit_semantics(brokers: Settings) -> None:
    tag = uuid.uuid4().hex[:10]
    probe_q, probe_dlq, probe_dlx = f"probe.{tag}.jobs", f"probe.{tag}.dlq", f"probe.{tag}.dlx"
    connection = await connect(brokers.rabbitmq_url)
    try:
        channel = await connection.channel()
        dlx = await channel.declare_exchange(probe_dlx, aio_pika.ExchangeType.FANOUT)
        dlq = await channel.declare_queue(
            probe_dlq, durable=True, arguments={"x-queue-type": "quorum"}
        )
        await dlq.bind(dlx)
        # Same shape as station.<id>.jobs but WITHOUT x-delivery-limit (broker default 20).
        queue = await channel.declare_queue(
            probe_q,
            durable=True,
            arguments={"x-queue-type": "quorum", "x-dead-letter-exchange": probe_dlx},
        )
        await channel.default_exchange.publish(
            aio_pika.Message(b"probe", delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key=probe_q,
        )

        deliveries = 0
        dead = None
        for _ in range(80):
            msg = await queue.get(fail=False, timeout=5)
            if msg is not None:
                deliveries += 1
                await msg.nack(requeue=True)
            dead = await dlq.get(fail=False, timeout=5)
            if dead is not None:
                break
            await asyncio.sleep(0.05)
        assert dead is not None, f"message never dead-lettered after {deliveries} deliveries"
        assert deliveries == 21
        x_death = dead.headers["x-death"]
        assert x_death[0]["reason"] == "delivery_limit"
        await dead.ack()
    finally:
        await connection.close()
        for kind, name in (("queues", probe_q), ("queues", probe_dlq), ("exchanges", probe_dlx)):
            try:
                await asyncio.to_thread(mgmt_delete, brokers, kind, name)
            except Exception:  # noqa: BLE001 - best-effort cleanup
                pass


# ---------------------------------------------------------------------------
# 2. Poison order -> TTL retry -> dispatch.dlq with failed lifecycle
# ---------------------------------------------------------------------------
async def test_poison_to_dlq(brokers: Settings, tmp_path: Path) -> None:
    run_id, oid = _ids()
    recipes = write_scaled_recipes(tmp_path / "recipes", SCALE)
    procs: list[subprocess.Popen] = []
    try:
        procs.append(
            start_service(
                "pizza.scheduler",
                brokers,
                {
                    "REWORK_LIMITS": '{"spin":1}',
                    "HEARTBEAT_INTERVAL_S": "0.5",
                    "SWEEP_INTERVAL_S": "0.2",
                    "RESUME_ON_START": "true",
                },
            )
        )
        procs.append(
            start_service(
                "pizza.station",
                brokers,
                {"RECIPES_DIR": str(recipes), "HEARTBEAT_INTERVAL_S": "0.5"},
                station="spin",
            )
        )
        await asyncio.sleep(5.0)  # consumers attached
        await _cli("start-run", "--station", "scheduler", "--run-id", run_id)
        await _cli("start-run", "--station", "spin", "--run-id", run_id)
        await _cli(
            "order", "--run-id", run_id, "--order-id", oid, "--fail-step", "spin", "--poison"
        )

        def done(ev) -> bool:
            return any(
                isinstance(e, OrderLifecycle) and e.order_id == oid and e.state == "failed"
                for e in ev
            )

        events = await _collect(brokers, run_id, done, 45.0)
        assert done(events), "order never reached lifecycle failed"

        failed = [e for e in events if isinstance(e, OpFailed) and e.order_id == oid]
        assert {e.visit for e in failed} >= {1, 2}, [e.visit for e in failed]
        dispatched2 = [
            e for e in events if isinstance(e, OpDispatched) and e.order_id == oid and e.visit == 2
        ]
        assert dispatched2, "no rework dispatch (visit 2)"
        assert not [e for e in events if isinstance(e, OpCompleted) and e.order_id == oid]

        # Rework went through the 5 s scheduler.rework.<station>.hold TTL hop (D-14):
        # visit-2 dispatch happens >= 4 s after the visit-1 failure.
        failed1 = min((e for e in failed if e.visit == 1), key=lambda e: e.ts)
        hop = (min(e.ts for e in dispatched2) - failed1.ts).total_seconds()
        assert hop >= 4.0, f"rework skipped the retry TTL (hop {hop:.2f}s)"

        ready, _ = await queue_counts(brokers, Q_DISPATCH_DLQ)
        assert ready >= 1, "exhausted order did not land in dispatch.dlq"
    finally:
        for proc in procs:
            terminate(proc, kill=True)


# ---------------------------------------------------------------------------
# 3. Manual break / repair, job held while down (D-08)
# ---------------------------------------------------------------------------
async def test_break_repair(brokers: Settings, tmp_path: Path) -> None:
    run_id, oid = _ids()
    recipes = write_scaled_recipes(tmp_path / "recipes", SCALE)
    proc = None
    try:
        proc = start_service(
            "pizza.station",
            brokers,
            {"RECIPES_DIR": str(recipes), "HEARTBEAT_INTERVAL_S": "0.5", "MTTR_S": "30"},
            station="spin",
        )
        await asyncio.sleep(4.0)
        await _cli("start-run", "--station", "spin", "--run-id", run_id)
        # 5 s outage: observing the down (~1 s) plus the next CLI spawn (~1 s) must still
        # land inside it, or the second break below opens a new outage instead.
        await _cli("break", "--station", "spin", "--run-id", run_id, "--duration-s", "5")

        events = await _collect(
            brokers, run_id, lambda ev: bool(_statuses(ev, "spin", "down")), 20.0
        )
        downs = _statuses(events, "spin", "down")
        assert downs and downs[0].cause == "manual"

        # A second break while down is ignored; a job published while down is held.
        await _cli("break", "--station", "spin", "--run-id", run_id, "--duration-s", "2")
        await _cli(
            "job", "--station", "spin", "--run-id", run_id, "--order-id", oid,
            "--unit-time-s", "0.3",
        )  # fmt: skip

        def recovered(ev) -> bool:
            return any(isinstance(e, OpCompleted) and e.order_id == oid for e in ev)

        events = await _collect(brokers, run_id, recovered, 30.0)
        assert recovered(events)
        downs = _statuses(events, "spin", "down")
        assert len(downs) == 1, "break while down must not emit a second down"
        idles = [e for e in _statuses(events, "spin", "idle") if e.ts > downs[0].ts]
        assert idles, "no idle after the outage"
        started = [e for e in events if isinstance(e, OpStarted) and e.order_id == oid]
        assert started, "held job never started"
        # 50 ms slack: Windows timer granularity.
        assert min(e.ts for e in started) >= idles[0].ts - _ms(50), "job started while down"
        assert (idles[0].ts - downs[0].ts).total_seconds() >= 1.5

        # Early repair (no duration -> sampled MTTR=30 s) ends the outage quickly.
        await _cli("break", "--station", "spin", "--run-id", run_id)
        mark = idles[0].ts

        def second_down(ev) -> bool:
            return any(e.ts > mark for e in _statuses(ev, "spin", "down"))

        events = await _collect(brokers, run_id, second_down, 20.0)
        down2 = next(e for e in _statuses(events, "spin", "down") if e.ts > mark)
        await _cli("repair", "--station", "spin", "--run-id", run_id)

        def second_idle(ev) -> bool:
            return any(e.ts > down2.ts for e in _statuses(ev, "spin", "idle"))

        events = await _collect(brokers, run_id, second_idle, 15.0)
        idle2 = next(e for e in _statuses(events, "spin", "idle") if e.ts > down2.ts)
        assert (idle2.ts - down2.ts).total_seconds() < 10.0, "repair did not end the outage early"
    finally:
        terminate(proc, kill=True)


def _ms(value: int):
    return timedelta(milliseconds=value)


# ---------------------------------------------------------------------------
# 4. Heartbeat silence -> offline, no dispatch until the station returns
# ---------------------------------------------------------------------------
async def test_heartbeat_offline_no_dispatch(brokers: Settings, tmp_path: Path) -> None:
    run_id, oid = _ids()
    recipes = write_scaled_recipes(tmp_path / "recipes", SCALE)
    procs: list[subprocess.Popen] = []
    try:
        procs.append(
            start_service(
                "pizza.scheduler",
                brokers,
                {
                    "HEARTBEAT_INTERVAL_S": "0.4",
                    "SWEEP_INTERVAL_S": "0.2",
                    "RESUME_ON_START": "false",
                },
            )
        )
        await asyncio.sleep(4.0)
        await _cli("start-run", "--station", "scheduler", "--run-id", run_id)

        def offline(ev) -> bool:
            return bool(_statuses(ev, "spin", "offline"))

        # Wait for the offline declaration before placing the order, so the check does
        # not depend on whether the sweep ran before the order arrived.
        events = await _collect(brokers, run_id, offline, 8.0)
        assert offline(events), "scheduler never declared spin offline"
        await _cli("order", "--run-id", run_id, "--order-id", oid)
        await asyncio.sleep(2.0)  # window in which a wrong dispatch would appear
        ready, _ = await queue_counts(brokers, jobs_queue_name("spin"))
        assert ready == 0, "work was dispatched to an offline station"

        procs.append(
            start_service(
                "pizza.station",
                brokers,
                {"RECIPES_DIR": str(recipes), "HEARTBEAT_INTERVAL_S": "0.4"},
                station="spin",
            )
        )
        await asyncio.sleep(3.0)
        await _cli("start-run", "--station", "spin", "--run-id", run_id)

        def spin_done(ev) -> bool:
            return any(
                isinstance(e, OpCompleted) and e.order_id == oid and e.step == "spin" for e in ev
            )

        events = await _collect(brokers, run_id, spin_done, 20.0)
        assert spin_done(events), "dispatch did not resume after the station returned"
    finally:
        for proc in procs:
            terminate(proc, kill=True)


# ---------------------------------------------------------------------------
# 5. Stocker overflow plus manual BANK / release (facts only; WIP is unit-tested)
# ---------------------------------------------------------------------------
async def test_stocker_and_bank(brokers: Settings) -> None:
    run_id, _ = _ids()
    oids = [f"o-{uuid.uuid4().hex[:8]}" for _ in range(4)]
    scheduler = None
    try:
        scheduler = start_service(
            "pizza.scheduler",
            brokers,
            {
                "HEARTBEAT_INTERVAL_S": "0",
                "STATION_QUEUE_LIMIT": "1",
                "RESUME_ON_START": "false",
            },
        )
        await asyncio.sleep(4.0)
        await _cli("start-run", "--station", "scheduler", "--run-id", run_id)
        # No station process runs here, so report `spin` idle explicitly: a station that
        # has never reported is not dispatchable, and the overflow path needs a free station.
        idle = StationStatus.make(run_id=run_id, machine_id="spin", status="idle")
        produce_fact(make_producer(brokers.kafka_bootstrap), "station.events", idle, key="spin")
        await asyncio.sleep(2.0)  # let the scheduler consume the status before the orders
        for oid in oids:
            await _cli("order", "--run-id", run_id, "--order-id", oid)

        def overflowed(ev) -> bool:
            return len([e for e in ev if isinstance(e, LotStocked) and e.kind == "overflow"]) >= 2

        events = await _collect(brokers, run_id, overflowed, 30.0)
        stocked = sorted(
            (e for e in events if isinstance(e, LotStocked) and e.kind == "overflow"),
            key=lambda e: e.ts,
        )
        seen: list[str] = []
        for fact in stocked:
            if fact.order_id not in seen:
                seen.append(fact.order_id)
        assert seen[:2] == [oids[2], oids[3]], seen

        await _cli("bank", "--run-id", run_id, "--order-id", oids[1], "--reason", "customer")

        def banked(ev) -> bool:
            return any(
                isinstance(e, LotBanked) and e.order_id == oids[1] and e.source == "manual"
                for e in ev
            )

        events = await _collect(brokers, run_id, banked, 20.0)
        bank = next(e for e in events if isinstance(e, LotBanked) and e.order_id == oids[1])
        assert bank.reason == "customer" and bank.source == "manual"

        await _cli("release", "--run-id", run_id, "--order-id", oids[1])

        def released(ev) -> bool:
            return any(
                isinstance(e, LotReleased) and e.order_id == oids[1] and e.by == "manual"
                for e in ev
            )

        events = await _collect(brokers, run_id, released, 20.0)
        assert released(events)
    finally:
        terminate(scheduler, kill=True)


# ---------------------------------------------------------------------------
# 4. Phase 06.1: breakdown mid-job -> op.failed(cause=breakdown) -> rework -> scrap
# ---------------------------------------------------------------------------
# Spin work time 4.0 s * 2.0 = 8 s: observing op.started (~1 s), the 0.5 s settle and the
# break CLI spawn (~1 s) must all fit inside the work window, or the break hits an idle
# station and no op.failed(cause=breakdown) is ever produced.
BREAKDOWN_SCALE = 2.0


def _scheduler_env(limits: str) -> dict:
    return {
        "REWORK_LIMITS": limits,
        "HEARTBEAT_INTERVAL_S": "0.5",
        "SWEEP_INTERVAL_S": "0.2",
        "RESUME_ON_START": "true",
    }


def _started(events, oid: str, visit: int) -> list[OpStarted]:
    return [
        e for e in events if isinstance(e, OpStarted) and e.order_id == oid and e.visit == visit
    ]


def _failed(events, oid: str, visit: int) -> list[OpFailed]:
    return [e for e in events if isinstance(e, OpFailed) and e.order_id == oid and e.visit == visit]


async def _break_when_started(brokers: Settings, run_id: str, oid: str, visit: int) -> None:
    """Wait until `visit` of the order is processing on spin, let it run a moment, then Break."""
    events = await _collect(brokers, run_id, lambda ev: bool(_started(ev, oid, visit)), 40.0)
    assert _started(events, oid, visit), f"visit {visit} never started"
    await asyncio.sleep(0.5)  # mid-job (spin takes ~8 s at BREAKDOWN_SCALE)
    await _cli("break", "--station", "spin", "--run-id", run_id, "--duration-s", "1")


async def test_breakdown_mid_job_reworks_then_scraps(brokers: Settings, tmp_path: Path) -> None:
    run_id, oid = _ids()
    recipes = write_scaled_recipes(tmp_path / "recipes", BREAKDOWN_SCALE)
    procs: list[subprocess.Popen] = []
    try:
        procs.append(start_service("pizza.scheduler", brokers, _scheduler_env('{"spin":1}')))
        procs.append(
            start_service(
                "pizza.station",
                brokers,
                {"RECIPES_DIR": str(recipes), "HEARTBEAT_INTERVAL_S": "0.5", "MTTR_S": "30"},
                station="spin",
            )
        )
        await asyncio.sleep(5.0)  # consumers attached
        await _cli("start-run", "--station", "scheduler", "--run-id", run_id)
        await _cli("start-run", "--station", "spin", "--run-id", run_id)
        await _cli("order", "--run-id", run_id, "--order-id", oid)

        # Visit 1: breakdown mid-job -> one op.failed(cause=breakdown), no completion.
        await _break_when_started(brokers, run_id, oid, 1)
        events = await _collect(brokers, run_id, lambda ev: bool(_failed(ev, oid, 1)), 30.0)
        failed1 = _failed(events, oid, 1)
        assert len(failed1) == 1, "expected exactly one op.failed for visit 1"
        assert failed1[0].cause == "breakdown"
        assert not [e for e in events if isinstance(e, OpCompleted) and e.order_id == oid]

        # Rework: visit 2 starts after the ~5 s retry TTL.
        events = await _collect(brokers, run_id, lambda ev: bool(_started(ev, oid, 2)), 40.0)
        started2 = _started(events, oid, 2)
        assert started2, "no rework (visit 2 never started after breakdown)"
        gap = (min(e.ts for e in started2) - failed1[0].ts).total_seconds()
        assert 3.5 <= gap <= 20.0, f"visit 2 start not ~retry-TTL after failure (gap {gap:.2f}s)"

        # Visit 2 breaks down too -> exceeds spin limit 1 -> dispatch.dlq + lifecycle failed.
        await _break_when_started(brokers, run_id, oid, 2)

        def scrapped(ev) -> bool:
            return any(
                isinstance(e, OrderLifecycle) and e.order_id == oid and e.state == "failed"
                for e in ev
            )

        events = await _collect(brokers, run_id, scrapped, 45.0)
        assert scrapped(events), "order never reached lifecycle failed"
        assert all(e.cause == "breakdown" for e in _failed(events, oid, 2))
        ready, _ = await queue_counts(brokers, Q_DISPATCH_DLQ)
        assert ready >= 1, "scrapped order did not land in dispatch.dlq"
    finally:
        for proc in procs:
            terminate(proc, kill=True)


async def test_breakdown_zero_limit_station_scraps_first_failure(
    brokers: Settings, tmp_path: Path
) -> None:
    """Limit-0 station (spin forced to 0 here so the test needs no full line): the first
    breakdown failure scraps the lot; there is no visit 2."""
    run_id, oid = _ids()
    recipes = write_scaled_recipes(tmp_path / "recipes", BREAKDOWN_SCALE)
    procs: list[subprocess.Popen] = []
    try:
        procs.append(start_service("pizza.scheduler", brokers, _scheduler_env('{"spin":0}')))
        procs.append(
            start_service(
                "pizza.station",
                brokers,
                {"RECIPES_DIR": str(recipes), "HEARTBEAT_INTERVAL_S": "0.5", "MTTR_S": "30"},
                station="spin",
            )
        )
        await asyncio.sleep(5.0)
        await _cli("start-run", "--station", "scheduler", "--run-id", run_id)
        await _cli("start-run", "--station", "spin", "--run-id", run_id)
        await _cli("order", "--run-id", run_id, "--order-id", oid)

        await _break_when_started(brokers, run_id, oid, 1)

        def scrapped(ev) -> bool:
            return any(
                isinstance(e, OrderLifecycle) and e.order_id == oid and e.state == "failed"
                for e in ev
            )

        events = await _collect(brokers, run_id, scrapped, 45.0)
        assert scrapped(events), "order never reached lifecycle failed"
        failed1 = _failed(events, oid, 1)
        assert failed1 and failed1[0].cause == "breakdown"
        await asyncio.sleep(7.0)  # longer than the retry TTL: still no visit 2
        events = await _collect(brokers, run_id, lambda ev: False, 1.0)
        assert not _started(events, oid, 2), "limit-0 station was reworked"
        ready, _ = await queue_counts(brokers, Q_DISPATCH_DLQ)
        assert ready >= 1, "scrapped order did not land in dispatch.dlq"
    finally:
        for proc in procs:
            terminate(proc, kill=True)


async def test_failing_lot_does_not_block_station(brokers: Settings, tmp_path: Path) -> None:
    """D-02: while the poison lot cools in scheduler.rework.spin.hold, the next lot starts."""
    run_id, poison = _ids()
    normal = f"o-{uuid.uuid4().hex[:8]}"
    recipes = write_scaled_recipes(tmp_path / "recipes", SCALE)
    procs: list[subprocess.Popen] = []
    try:
        procs.append(start_service("pizza.scheduler", brokers, _scheduler_env('{"spin":1}')))
        procs.append(
            start_service(
                "pizza.station",
                brokers,
                {"RECIPES_DIR": str(recipes), "HEARTBEAT_INTERVAL_S": "0.5"},
                station="spin",
            )
        )
        await asyncio.sleep(5.0)  # consumers attached
        await _cli("start-run", "--station", "scheduler", "--run-id", run_id)
        await _cli("start-run", "--station", "spin", "--run-id", run_id)
        await _cli(
            "order", "--run-id", run_id, "--order-id", poison, "--fail-step", "spin", "--poison"
        )
        await _cli("order", "--run-id", run_id, "--order-id", normal)

        def both(ev) -> bool:
            return bool(_started(ev, normal, 1)) and bool(_started(ev, poison, 2))

        events = await _collect(brokers, run_id, both, 45.0)
        assert _started(events, normal, 1), "second lot never started"
        assert _started(events, poison, 2), "poison lot never reworked (visit 2)"
        normal_start = min(e.ts for e in _started(events, normal, 1))
        poison_v2_start = min(e.ts for e in _started(events, poison, 2))
        assert normal_start < poison_v2_start, "failing lot blocked its station while cooling"
    finally:
        for proc in procs:
            terminate(proc, kill=True)
