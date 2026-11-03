"""Live-stack proof that a P1 VIP overtakes a P50 backlog at Bake (PORTFOLIO-03).

Prerequisites: the full compose stack (including the `line` profile) is up and the
dashboard answers on http://127.0.0.1:8000. The test skips cleanly otherwise.
It shares its helpers with scripts/record_showcase.py so the recorder and the test
assert the same preconditions. Time scale is restored afterwards.
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


def test_vip_overtake_ok_pure(showcase) -> None:
    vip = {"stations": {"bake": {"in": "2026-01-01T00:00:10Z", "out": "2026-01-01T00:00:20Z"}}}
    later = {"stations": {"bake": {"in": "2026-01-01T00:00:30Z", "out": "2026-01-01T00:00:40Z"}}}
    earlier = {"stations": {"bake": {"in": "2026-01-01T00:00:00Z", "out": "2026-01-01T00:00:09Z"}}}
    assert showcase.vip_overtake_ok(vip, [later, later])
    assert not showcase.vip_overtake_ok(vip, [later, earlier])
    assert not showcase.vip_overtake_ok(vip, [])


def test_vip_overtakes_p50_backlog(showcase) -> None:
    state = showcase.api("GET", "/api/state")
    prior_scale = int(state.get("time_scale") or 1)
    if state["run"]["state"] not in ("running", "paused"):
        showcase.api("POST", "/api/run/start", {"arrival_rate_per_min": 0.1})
    elif state["run"]["state"] == "paused":
        showcase.api("POST", "/api/run/resume")
    showcase.api("POST", "/api/time_scale", {"scale": 4})
    try:
        seeded = showcase.seed_vip_backlog()
        queued = showcase.queued_at_bake(seeded)
        assert len(queued) >= 3
        vip = showcase.api("POST", "/api/order", {"product": "pepperoni", "qty": 1, "priority": 1})[
            "order_id"
        ]
        vip_eta = showcase.api("GET", f"/api/lot/{vip}/eta")
        backlog_eta = [
            showcase.api("GET", f"/api/lot/{oid}/eta") for oid in showcase.queued_at_bake(seeded)
        ]
        assert showcase.vip_overtake_ok(vip_eta, backlog_eta), (vip_eta, backlog_eta)
    finally:
        showcase.api("POST", "/api/time_scale", {"scale": prior_scale})
