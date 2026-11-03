"""Live-stack proof that a recipe Apply is reflected by GET /api/recipes (PORTFOLIO-04).

Prerequisites: the full compose stack is up and the dashboard answers on
http://127.0.0.1:8000. The test skips cleanly otherwise and always restores the
original Bake pepperoni unit time (T-09-09).
"""

import importlib.util
from pathlib import Path

import httpx
import pytest

pytestmark = pytest.mark.integration

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "record_showcase.py"


@pytest.fixture(scope="module")
def showcase():
    spec = importlib.util.spec_from_file_location("record_showcase", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    try:
        httpx.get(mod.BASE_URL + "/api/state", timeout=3.0).raise_for_status()
    except httpx.HTTPError:
        pytest.skip("dashboard not reachable at " + mod.BASE_URL)
    return mod


def _bake_pepperoni(showcase) -> float:
    return showcase.api("GET", "/api/recipes")["recipes"]["bake"]["pepperoni"]


def test_recipe_apply_changes_and_restores(showcase) -> None:
    state = showcase.api("GET", "/api/state")
    if state["run"]["state"] not in ("running", "paused"):
        showcase.api("POST", "/api/run/start", {"arrival_rate_per_min": 0.1})
    original = _bake_pepperoni(showcase)
    target = 6.0 if original != 6.0 else 7.0
    try:
        showcase.api(
            "POST", "/api/station/bake/recipe", {"product": "pepperoni", "unit_time_s": target}
        )
        assert _bake_pepperoni(showcase) == pytest.approx(target)
    finally:
        showcase.api(
            "POST", "/api/station/bake/recipe", {"product": "pepperoni", "unit_time_s": original}
        )
    assert _bake_pepperoni(showcase) == pytest.approx(original)


def test_restore_helper_runs_on_exception(showcase) -> None:
    original = _bake_pepperoni(showcase)
    with pytest.raises(RuntimeError):
        with showcase.restored_recipe("bake", "pepperoni"):
            showcase.api(
                "POST", "/api/station/bake/recipe", {"product": "pepperoni", "unit_time_s": 5.0}
            )
            raise RuntimeError("boom")
    assert _bake_pepperoni(showcase) == pytest.approx(original)
