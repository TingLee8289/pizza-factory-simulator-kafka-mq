"""Per-station recipe tables (D-01..D-06). No broker imports.

Recipes are TOML files, one per station, shipped as package data in
``pizza.recipes``. File names come only from the STATIONS allow-list (T-03-02).
"""

import math
import os
import re
import tomllib
from importlib import resources
from pathlib import Path

from pizza.common.constants import PRODUCTS, SLUG_PATTERN, STATIONS


class RecipeError(ValueError):
    """Raised when a recipe file or table is invalid."""


def load_station_recipes(
    station: str, recipes_dir: str | os.PathLike[str] | None = None
) -> dict[str, float]:
    """Load unit times (seconds per pizza) for one station, keyed by product."""
    if station not in STATIONS:
        raise ValueError(f"unknown station {station!r}")
    filename = f"{station}.toml"
    try:
        if recipes_dir:
            text = (Path(recipes_dir) / filename).read_text(encoding="utf-8")
        else:
            text = resources.files("pizza.recipes").joinpath(filename).read_text(encoding="utf-8")
        raw = tomllib.loads(text)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise RecipeError(f"cannot read recipe file {filename}: {exc}") from exc
    if raw.get("station") != station:
        raise RecipeError(f"{filename}: station key mismatch ({raw.get('station')!r})")
    table = raw.get("unit_time_s")
    if not isinstance(table, dict) or not table:
        raise RecipeError(f"{filename}: missing [unit_time_s] table")
    out: dict[str, float] = {}
    for product, value in table.items():
        if not re.match(SLUG_PATTERN, product):
            raise RecipeError(f"{filename}: invalid product slug {product!r}")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise RecipeError(f"{filename}: {product} time is not a number")
        if not math.isfinite(value) or value <= 0:
            raise RecipeError(f"{filename}: {product} time must be finite and > 0")
        out[product] = float(value)
    return out


def load_all_recipes(
    recipes_dir: str | os.PathLike[str] | None = None,
) -> dict[str, dict[str, float]]:
    """Load all seven station tables in STATIONS order."""
    return {s: load_station_recipes(s, recipes_dir) for s in STATIONS}


def validate_recipes(all_recipes: dict[str, dict[str, float]]) -> None:
    """Check product sets match PRODUCTS and bake is strictly slowest per product."""
    expected = set(PRODUCTS)
    for station in STATIONS:
        table = all_recipes.get(station)
        if table is None or set(table) != expected:
            raise RecipeError(
                f"station {station!r} must define exactly products {sorted(expected)}"
            )
    for product in PRODUCTS:
        bake = all_recipes["bake"][product]
        for station in STATIONS:
            if station != "bake" and all_recipes[station][product] >= bake:
                raise RecipeError(
                    f"bake must be strictly slowest for {product!r}; {station!r} is not faster"
                )


def theoretical_total_s(
    all_recipes: dict[str, dict[str, float]], product: str, qty: int
) -> float:
    """Sum of unit_time * qty across all stations (no queueing)."""
    return sum(all_recipes[s][product] * qty for s in STATIONS)
