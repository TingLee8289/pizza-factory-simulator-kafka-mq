"""Unit tests for the recipe loader (STATION-02, D-03, D-05)."""

import copy

import pytest

from pizza.common.constants import PRODUCTS, STATIONS
from pizza.common.recipes import (
    RecipeError,
    load_all_recipes,
    load_station_recipes,
    theoretical_total_s,
    validate_recipes,
)


def _write_set(path, tables=None):
    base = load_all_recipes()
    for station, table in (tables or {}).items():
        base[station] = table
    for station, table in base.items():
        body = "\n".join(f"{k} = {v!r}" for k, v in table.items())
        (path / f"{station}.toml").write_text(f'station = "{station}"\n[unit_time_s]\n{body}\n')


def test_bake_values():
    assert load_station_recipes("bake") == {"margherita": 12.0, "pepperoni": 13.0, "veggie": 14.0}


def test_empty_recipes_dir_falls_back_to_packaged_files():
    """Compose sets RECIPES_DIR="" when unset; that must behave like None."""
    assert load_station_recipes("bake", "") == load_station_recipes("bake")


def test_all_recipes_load_and_validate():
    allr = load_all_recipes()
    assert tuple(allr) == STATIONS
    for table in allr.values():
        assert set(table) == set(PRODUCTS)
    validate_recipes(allr)


def test_bake_strictly_slowest_per_product():
    allr = load_all_recipes()
    for product in PRODUCTS:
        others = [allr[s][product] for s in STATIONS if s != "bake"]
        assert allr["bake"][product] > max(others)


def test_validate_rejects_non_bottleneck_bake():
    allr = copy.deepcopy(load_all_recipes())
    allr["slice"]["veggie"] = 20.0
    with pytest.raises(RecipeError, match="veggie"):
        validate_recipes(allr)


@pytest.mark.parametrize("name", ["../etc", "bake.toml", "", "nope"])
def test_unknown_station_rejected(name):
    with pytest.raises(ValueError):
        load_station_recipes(name)


@pytest.mark.parametrize("bad", ["0.0", "-1.0", "nan", "inf", '"x"'])
def test_bad_time_rejected(tmp_path, bad):
    _write_set(tmp_path)
    (tmp_path / "spin.toml").write_text(
        f'station = "spin"\n[unit_time_s]\nmargherita = {bad}\npepperoni = 1.0\nveggie = 1.0\n'
    )
    with pytest.raises(RecipeError):
        load_station_recipes("spin", tmp_path)


def test_station_key_mismatch_rejected(tmp_path):
    _write_set(tmp_path)
    (tmp_path / "spin.toml").write_text(
        'station = "sauce"\n[unit_time_s]\nmargherita = 1.0\n'
    )
    with pytest.raises(RecipeError):
        load_station_recipes("spin", tmp_path)


def test_bad_slug_rejected(tmp_path):
    (tmp_path / "spin.toml").write_text('station = "spin"\n[unit_time_s]\n"Bad Key" = 1.0\n')
    with pytest.raises(RecipeError):
        load_station_recipes("spin", tmp_path)


def test_missing_product_rejected_by_validate():
    allr = copy.deepcopy(load_all_recipes())
    del allr["spin"]["veggie"]
    with pytest.raises(RecipeError):
        validate_recipes(allr)


def test_recipes_dir_override(tmp_path):
    _write_set(tmp_path, {"spin": {"margherita": 0.2, "pepperoni": 0.2, "veggie": 0.2}})
    assert load_station_recipes("spin", tmp_path)["veggie"] == 0.2


def test_missing_file_rejected(tmp_path):
    with pytest.raises(RecipeError):
        load_station_recipes("spin", tmp_path)


def test_theoretical_total():
    allr = load_all_recipes()
    assert theoretical_total_s(allr, "veggie", 2) == 2 * sum(allr[s]["veggie"] for s in STATIONS)
