"""Live-broker proof of the Phase 8 recipe facts (D-08, D-09, D-10).

Prerequisites (same as the other integration modules):
- Infra only: ``docker compose up -d --wait`` (kafka, rabbitmq, init).
- The ``line`` compose profile must be DOWN so no other station, scheduler or
  generator contaminates ``station.events``.
- Recipes are written scaled by ``SCALE`` into a temp dir and passed via
  ``RECIPES_DIR``; the real packaged recipes are never modified.
- The station under test is ``sauce`` (never ``bake``, which the compose
  ``station`` service runs as).

Every test uses a fresh ``run_id`` and purges the line queues before and after.
Assertions filter by the test's own ``run_id``.
"""

import asyncio
import uuid

import pytest

from pizza.common.config import Settings
from pizza.common.constants import STATIONS
from pizza.common.envelope import RecipeChanged, RecipeSnapshot, SetRecipe, StationStatus
from pizza.common.rabbit_io import connect, publish_command
from pizza.common.recipes import load_all_recipes
from pizza.common.topology import EX_CONTROL
from pizza.tools.send_job import build_run_started
from tests.conftest import (
    collect_events,
    purge_line_queues,
    start_service,
    terminate,
    write_scaled_recipes,
)

pytestmark = pytest.mark.integration

STATION = "sauce"
SCALE = 0.05
TIMEOUT = 30.0


@pytest.fixture(autouse=True)
async def _clean_queues(brokers: Settings):
    await purge_line_queues(brokers)
    yield
    await purge_line_queues(brokers)


def _new_run_id() -> str:
    return f"it-r-{uuid.uuid4().hex[:10]}"


async def _publish_control(settings: Settings, routing_key: str, command) -> None:
    connection = await connect(settings.rabbitmq_url)
    try:
        channel = await connection.channel()
        exchange = await channel.get_exchange(EX_CONTROL)
        await publish_command(exchange, routing_key, command)
    finally:
        await connection.close()


async def publish_run_started(settings: Settings, routing_key: str, machine_id: str, run_id: str):
    await _publish_control(settings, routing_key, build_run_started(machine_id, run_id))


async def publish_set_recipe(
    settings: Settings, station: str, product: str, unit_time_s: float, run_id: str
) -> None:
    cmd = SetRecipe.make(
        run_id=run_id,
        machine_id=station,
        station=station,
        product=product,
        unit_time_s=unit_time_s,
    )
    await _publish_control(settings, f"station.{station}.recipe", cmd)


def _has_idle(events) -> bool:
    return any(isinstance(e, StationStatus) and e.status == "idle" for e in events)


async def _collect(settings, run_id, until, timeout_s: float = TIMEOUT):
    return await asyncio.to_thread(collect_events, settings, run_id, until, timeout_s)


async def test_station_emits_recipe_changed(brokers: Settings, tmp_path) -> None:
    run_id = _new_run_id()
    recipes_dir = write_scaled_recipes(tmp_path / "recipes", SCALE)
    base = load_all_recipes(str(recipes_dir))[STATION]["margherita"]
    new_value = round(base * 2, 3)

    proc = start_service(
        "pizza.station", brokers, {"RECIPES_DIR": str(recipes_dir)}, station=STATION
    )
    try:
        await publish_run_started(brokers, f"station.{STATION}.run", STATION, run_id)
        # Wait for idle so the station is subscribed before the edit is sent.
        await _collect(brokers, run_id, _has_idle)
        await publish_set_recipe(brokers, STATION, "margherita", new_value, run_id)
        events = await _collect(
            brokers,
            run_id,
            lambda ev: any(isinstance(e, RecipeChanged) for e in ev),
        )
    finally:
        terminate(proc, kill=True)

    changed = [e for e in events if isinstance(e, RecipeChanged)]
    assert len(changed) == 1
    fact = changed[0]
    assert fact.machine_id == STATION
    assert fact.station == STATION
    assert fact.product == "margherita"
    assert fact.new_unit_time_s == pytest.approx(new_value)
    assert fact.old_unit_time_s == pytest.approx(base)
    assert fact.run_id == run_id


async def test_scheduler_emits_recipe_snapshot(brokers: Settings, tmp_path) -> None:
    run_id = _new_run_id()
    recipes_dir = write_scaled_recipes(tmp_path / "recipes", SCALE)
    expected = load_all_recipes(str(recipes_dir))

    proc = start_service(
        "pizza.scheduler",
        brokers,
        {"RESUME_ON_START": "false", "RECIPES_DIR": str(recipes_dir)},
    )
    try:
        await publish_run_started(brokers, "scheduler.run", "scheduler", run_id)
        events = await _collect(
            brokers,
            run_id,
            lambda ev: any(isinstance(e, RecipeSnapshot) for e in ev),
        )
    finally:
        terminate(proc, kill=True)

    snapshots = [e for e in events if isinstance(e, RecipeSnapshot)]
    assert len(snapshots) == 1
    fact = snapshots[0]
    assert fact.machine_id == "scheduler"
    assert fact.run_id == run_id
    # No snapshot for any other run id may be attributed to this run.
    assert all(s.run_id == run_id for s in snapshots)
    assert set(fact.recipes) == set(STATIONS)
    for station in STATIONS:
        assert set(fact.recipes[station]) == set(expected[station])
        for product in expected[station]:
            assert fact.recipes[station][product] == pytest.approx(expected[station][product])
