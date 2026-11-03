"""Pure unit tests for the event-derived recipe table (Phase 8, STATION-06).

Covers ``pizza.kpi.recompute.recipes_from_events``: snapshot selection, change
replay windowed by ``run_id`` and ``ts``, order independence, the disk fallback
with provenance, and the guarantee that the caller's fallback mapping is never
mutated. All timestamps are explicit (``at(offset)``), never wall clock.
"""

import copy
import random

from pizza.common.constants import PRODUCTS, STATIONS
from pizza.common.envelope import RecipeChanged, RecipeSnapshot
from pizza.common.recipes import load_all_recipes
from pizza.kpi.recompute import recipes_from_events
from tests.unit.kpi_fixtures import RUN, at

FULL_TABLE = load_all_recipes()


def _table() -> dict[str, dict[str, float]]:
    return copy.deepcopy(FULL_TABLE)


def _snap(offset_s: float, recipes=None, run_id: str = RUN) -> RecipeSnapshot:
    return RecipeSnapshot.make(
        run_id=run_id,
        machine_id="scheduler",
        ts=at(offset_s),
        recipes=recipes if recipes is not None else _table(),
    )


def _change(
    offset_s: float,
    station: str,
    product: str,
    new: float,
    run_id: str = RUN,
    old: float | None = None,
) -> RecipeChanged:
    return RecipeChanged.make(
        run_id=run_id,
        machine_id=station,
        station=station,
        product=product,
        old_unit_time_s=old,
        new_unit_time_s=new,
        ts=at(offset_s),
    )


def test_snapshot_only_returns_snapshot_values_from_events():
    table = _table()
    table["bake"]["margherita"] = 99.0
    snap = _snap(0, recipes=table)

    result, source, count = recipes_from_events([snap], RUN, at(10), FULL_TABLE)

    assert result == table
    assert source == "events"
    assert count == 0


def test_snapshot_plus_one_change_updates_only_that_product():
    snap = _snap(0)
    change = _change(5, "bake", "margherita", 42.0, old=FULL_TABLE["bake"]["margherita"])

    result, source, count = recipes_from_events([snap, change], RUN, at(10), FULL_TABLE)

    expected = _table()
    expected["bake"]["margherita"] = 42.0
    assert result == expected
    assert source == "events"
    assert count == 1


def test_change_after_as_of_is_ignored():
    snap = _snap(0)
    late = _change(20, "bake", "margherita", 42.0)

    result, source, count = recipes_from_events([snap, late], RUN, at(10), FULL_TABLE)

    assert result == _table()
    assert source == "events"
    assert count == 0


def test_change_for_other_run_is_ignored():
    snap = _snap(0)
    other = _change(5, "bake", "margherita", 42.0, run_id="r-20260101-000000-0000")

    result, _, count = recipes_from_events([snap, other], RUN, at(10), FULL_TABLE)

    assert result == _table()
    assert count == 0


def test_change_before_snapshot_is_ignored():
    early = _change(1, "bake", "margherita", 42.0)
    snap = _snap(5)

    result, source, count = recipes_from_events([early, snap], RUN, at(10), FULL_TABLE)

    assert result == _table()
    assert source == "events"
    assert count == 0


def test_no_snapshot_returns_fallback_from_disk():
    change = _change(5, "bake", "margherita", 42.0)

    result, source, count = recipes_from_events([change], RUN, at(10), FULL_TABLE)

    assert result == FULL_TABLE
    assert source == "disk"
    assert count == 0


def test_snapshot_missing_station_falls_back_to_disk():
    bad = _table()
    bad.pop("spin")
    snap = _snap(0).model_copy(update={"recipes": bad})

    result, source, count = recipes_from_events([snap], RUN, at(10), FULL_TABLE)

    assert result == FULL_TABLE
    assert source == "disk"
    assert count == 0


def test_snapshot_station_missing_product_falls_back_to_disk():
    bad = _table()
    bad["bake"].pop("margherita")
    snap = _snap(0).model_copy(update={"recipes": bad})

    result, source, count = recipes_from_events([snap], RUN, at(10), FULL_TABLE)

    assert result == FULL_TABLE
    assert source == "disk"
    assert count == 0


def test_shuffled_event_order_yields_identical_table():
    events = [
        _snap(0),
        _change(2, "bake", "margherita", 30.0),
        _change(3, "bake", "margherita", 31.0),
        _change(4, "spin", "pepperoni", 12.0),
    ]
    baseline = recipes_from_events(events, RUN, at(10), FULL_TABLE)

    shuffled = list(events)
    random.Random(7).shuffle(shuffled)
    result = recipes_from_events(shuffled, RUN, at(10), FULL_TABLE)

    assert result == baseline
    assert result[1] == "events"


def test_fallback_is_never_mutated_on_disk_path():
    before = copy.deepcopy(FULL_TABLE)

    result, source, _ = recipes_from_events([], RUN, at(10), FULL_TABLE)
    result["bake"]["margherita"] = -1.0

    assert source == "disk"
    assert FULL_TABLE == before


def test_fallback_is_never_mutated_on_events_path():
    before = copy.deepcopy(FULL_TABLE)
    snap = _snap(0)
    change = _change(5, "bake", "margherita", 42.0)

    result, source, _ = recipes_from_events([snap, change], RUN, at(10), FULL_TABLE)
    result["bake"]["margherita"] = -1.0

    assert source == "events"
    assert FULL_TABLE == before
    assert set(STATIONS) == set(FULL_TABLE)
    assert all(set(FULL_TABLE[s]) == set(PRODUCTS) for s in STATIONS)
