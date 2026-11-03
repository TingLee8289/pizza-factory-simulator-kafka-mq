"""Routes, SSE generators and security middleware with a fake runtime (no brokers)."""

from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from pizza.dashboard.app import create_app
from pizza.dashboard.control import ConflictError, RunController
from pizza.dashboard.hub import Hub
from pizza.dashboard.routes import event_stream, state_stream
from pizza.dashboard.settings import DashboardSettings


class FakeRuntime:
    def __init__(self, max_clients: int = 20) -> None:
        self.hub = Hub(max_clients=max_clients)
        exchange = MagicMock()
        exchange.publish = AsyncMock()

        async def get_exchange():
            return exchange

        async def purge():
            return {}

        async def place(controller, request):
            if controller.state == "none":
                raise ConflictError("cannot place order: no active run")
            return {"order_id": "m-1", "run_id": "r", "due_ts": "x", "priority": request.priority}

        self.lots: dict[str, str] = {}
        self.exchange = exchange
        self.controller = RunController(
            None, get_exchange, purge, place_order_fn=place, lot_state_fn=self.lots.get
        )
        self.fail = False
        self.settings = DashboardSettings()

    def snapshot_payload(self):
        if self.fail:
            raise RuntimeError("amqp://user:secret@broker boom")
        return {"seq": self.hub.seq, "replaying": False, "marker": "snap"}

    def lot_detail(self, order_id):
        if order_id != "L-1":
            return None
        return {"order_id": "L-1", "visits": [], "steps": {}, "station_eta": None}

    def recipe_tables(self):
        return {"bake": {"margherita": 12.0, "pepperoni": 13.0, "veggie": 14.0}}

    async def set_recipe(self, name, request):
        return await self.controller.set_recipe(name, request)


@pytest.fixture
def runtime():
    return FakeRuntime()


@pytest.fixture
async def client(runtime):
    app = create_app(runtime)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


JSON = {"content-type": "application/json"}
START = {"arrival_rate_per_min": 2.0}


# ---- SSE generators ----------------------------------------------------------


async def test_state_stream_snapshot_then_tick_and_release(runtime):
    gen = state_stream(runtime)
    first = await gen.__anext__()
    assert first.event == "snapshot" and first.data["marker"] == "snap"
    assert first.retry == 2000 and first.id == str(runtime.hub.seq)
    assert runtime.hub.client_count == 1
    runtime.hub.publish("state", "tick", {"n": 1})
    runtime.hub.publish("events", "fact", {"n": 2})  # other channel: not delivered
    tick = await gen.__anext__()
    assert tick.event == "tick" and tick.data == {"n": 1}
    assert tick.id == str(runtime.hub.seq - 1)
    await gen.aclose()
    assert runtime.hub.client_count == 0


async def test_event_stream_only_facts(runtime):
    gen = event_stream(runtime)
    runtime.hub.publish("state", "tick", {"n": 0})  # not subscribed yet: lost

    import asyncio

    task = asyncio.create_task(gen.__anext__())
    await asyncio.sleep(0)
    runtime.hub.publish("state", "tick", {"n": 1})
    runtime.hub.publish("events", "fact", {"n": 2, "seq": 99})
    task_item = await task
    assert task_item.event == "fact" and task_item.data["n"] == 2
    await gen.aclose()
    assert runtime.hub.client_count == 0


# ---- HTTP -------------------------------------------------------------------


async def test_healthz_and_state(client, runtime):
    r = await client.get("/healthz")
    assert r.status_code == 200 and r.json() == {"status": "ok"}
    r = await client.get("/api/state")
    assert r.json() == runtime.snapshot_payload()


async def test_stream_cap_returns_503(runtime):
    runtime.hub.max_clients = 1
    runtime.hub.subscribe("state")
    app = create_app(runtime)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        r = await c.get("/api/stream/state")
        assert r.status_code == 503 and "detail" in r.json()
        r = await c.get("/api/stream/events")
        assert r.status_code == 503


async def test_runtime_not_ready_is_503():
    app = create_app(None)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        assert (await c.get("/healthz")).status_code == 200
        assert (await c.get("/api/state")).status_code == 503


async def test_start_twice_409_and_pause_resume_flow(client):
    r = await client.post("/api/run/pause", headers=JSON)
    assert r.status_code == 409  # no run
    r = await client.post("/api/run/start", json=START)
    assert r.status_code == 200 and r.json()["state"] == "running"
    r = await client.post("/api/run/start", json=START)
    assert r.status_code == 409 and "detail" in r.json()
    assert (await client.post("/api/run/pause")).json()["state"] == "paused"
    assert (await client.post("/api/run/resume", json={})).json()["state"] == "running"
    assert (await client.post("/api/run/reset")).json()["state"] == "none"


async def test_order_rate_rule_mapping(client):
    assert (
        await client.post("/api/order", json={"product": "margherita", "qty": 1})
    ).status_code == 409
    assert (await client.post("/api/rate", json={"arrival_rate_per_min": 2})).status_code == 409
    await client.post("/api/run/start", json=START)
    r = await client.post("/api/order", json={"product": "margherita", "qty": 0})
    assert r.status_code == 422
    r = await client.post("/api/order", json={"product": "margherita", "qty": 2, "priority": 10})
    assert r.status_code == 200 and r.json()["order_id"] == "m-1"
    assert (await client.post("/api/rate", json={"arrival_rate_per_min": 99})).status_code == 422
    assert (await client.post("/api/rate", json={"arrival_rate_per_min": 3})).status_code == 200
    # Phase 7 (D-10/SCHED-03): edd/spt are unlocked, not just fifo.
    r = await client.post("/api/rule", json={"rule": "edd"})
    assert r.status_code == 200 and r.json() == {"rule": "edd"}
    r = await client.post("/api/rule", json={"rule": "fifo"})
    assert r.status_code == 200 and r.json() == {"rule": "fifo"}
    assert (await client.post("/api/rule", json={"rule": "bogus"})).status_code == 422


async def test_non_json_post_is_415_and_cross_origin_403(client):
    r = await client.post("/api/run/start", content="a=1", headers={"content-type": "text/plain"})
    assert r.status_code == 415
    r = await client.post(
        "/api/run/start",
        content="a=1",
        headers={"content-type": "application/x-www-form-urlencoded"},
    )
    assert r.status_code == 415
    r = await client.post("/api/run/pause", headers={"origin": "http://evil.example"})
    assert r.status_code == 403
    r = await client.post("/api/run/start", json=START, headers={"origin": "http://test"})
    assert r.status_code == 200


async def test_unknown_route_404(client):
    r = await client.get("/nope")
    assert r.status_code == 404


async def test_index_without_file_is_json_404_or_html(client):
    r = await client.get("/")
    assert r.status_code in (200, 404)


async def test_security_headers_everywhere(client):
    for r in (
        await client.get("/healthz"),
        await client.get("/nope"),
        await client.post("/api/run/pause"),
    ):
        csp = r.headers["content-security-policy"]
        assert "default-src 'self'" in csp
        assert "script-src 'self' 'unsafe-inline'" in csp
        assert "style-src 'self' 'unsafe-inline'" in csp
        assert "connect-src 'self'" in csp and "img-src 'self' data:" in csp
        assert r.headers["x-content-type-options"] == "nosniff"
        assert r.headers["referrer-policy"] == "no-referrer"
    assert "access-control-allow-origin" not in (await client.get("/healthz")).headers


async def test_unexpected_error_is_generic_500(client, runtime):
    runtime.fail = True
    r = await client.get("/api/state")
    assert r.status_code == 500
    assert r.json() == {"detail": "internal error"}
    assert "secret" not in r.text
    assert r.headers["x-content-type-options"] == "nosniff"


# ---- Phase 6 control routes ----------------------------------------------------


async def test_failure_routes_need_active_run_and_validate(client, runtime):
    r = await client.post("/api/station/bake/break", headers=JSON)
    assert r.status_code == 409
    assert (await client.post("/api/run/start", json=START, headers=JSON)).status_code == 200
    assert (await client.post("/api/station/nope/break", headers=JSON)).status_code == 404
    repair = await client.post("/api/station/bake/repair", headers=JSON)
    assert repair.json() == {"accepted": True}
    ok = await client.post("/api/station/bake/break", json={"duration_s": 30}, headers=JSON)
    assert ok.status_code == 200 and ok.json() == {"accepted": True}
    bad = await client.post("/api/station/bake/break", json={"duration_s": 0}, headers=JSON)
    assert bad.status_code == 422
    r = await client.post("/api/lot/o-1/bank", json={"reason": "x"}, headers=JSON)
    assert r.status_code == 422
    r = await client.post("/api/lot/o-1/bank", json={"reason": "capacity"}, headers=JSON)
    assert r.status_code == 200
    assert (await client.post("/api/lot/o-1/release", headers=JSON)).status_code == 200
    assert (await client.post("/api/lot/-bad/release", headers=JSON)).status_code == 422


async def test_recipes_route_and_live_edit(client):
    r = await client.get("/api/recipes")
    assert r.status_code == 200
    assert r.json()["recipes"]["bake"]["margherita"] == 12.0
    r = await client.post(
        "/api/station/bake/recipe",
        json={"product": "margherita", "unit_time_s": 7.5},
        headers=JSON,
    )
    assert r.status_code == 200
    assert r.json()["unit_time_s"] == 7.5
    bad_station = await client.post(
        "/api/station/nope/recipe", json={"product": "veggie", "unit_time_s": 1}, headers=JSON
    )
    assert bad_station.status_code == 404
    bad_product = await client.post(
        "/api/station/bake/recipe", json={"product": "calzone", "unit_time_s": 1}, headers=JSON
    )
    assert bad_product.status_code == 422


async def test_failure_config_route(client, runtime):
    body = (await client.get("/api/failure-config")).json()
    assert set(body) == {
        "mtbf_s", "mttr_s", "rework_limits", "station_queue_limit", "bank_after_s",
        "bank_wait_threshold", "heartbeat_interval_s",
    }  # fmt: skip
    assert body["station_queue_limit"] == 8 and body["rework_limits"]["spin"] == 2


async def test_lot_priority_route_status_mapping(client, runtime):
    url = "/api/lot/L-0001/priority"
    # No active run -> 409.
    assert (await client.post(url, json={"priority": 5})).status_code == 409
    await client.post("/api/run/start", json=START)
    runtime.lots.update({"L-0001": "queued", "L-0002": "done", "L-0003": "failed"})
    runtime.lots["L-0004"] = "pre_release"
    runtime.lots["L-0005"] = "banked"

    runtime.exchange.publish.reset_mock()
    r = await client.post(url, json={"priority": 5})
    assert r.status_code == 200 and r.json() == {"accepted": True}
    assert runtime.exchange.publish.await_count == 1
    assert runtime.exchange.publish.await_args.kwargs["routing_key"] == "scheduler.priority"
    # Same priority is still accepted at the REST edge; banked lots can be re-prioritised.
    assert (await client.post(url, json={"priority": 50})).status_code == 200
    assert (await client.post("/api/lot/L-0005/priority", json={"priority": 3})).status_code == 200

    assert (await client.post("/api/lot/L-9999/priority", json={"priority": 5})).status_code == 404
    for oid in ("L-0002", "L-0003", "L-0004"):
        r = await client.post(f"/api/lot/{oid}/priority", json={"priority": 5})
        assert r.status_code == 409, oid

    runtime.exchange.publish.reset_mock()
    for bad in (
        {"priority": 0},
        {"priority": 100},
        {"priority": 50.5},
        {"priority": "50"},
        {"priority": True},
        {"priority": 5, "extra": 1},
        {},
    ):
        r = await client.post(url, json=bad)
        assert r.status_code == 422, bad
    assert runtime.exchange.publish.await_count == 0


async def test_lot_hold_cancel_route(client, runtime):
    assert (await client.post("/api/lot/L-0001/hold/cancel")).status_code == 409  # no run
    await client.post("/api/run/start", json=START)
    runtime.exchange.publish.reset_mock()
    r = await client.post("/api/lot/L-0001/hold/cancel")
    assert r.status_code == 200 and r.json() == {"accepted": True}
    assert runtime.exchange.publish.await_args.kwargs["routing_key"] == "scheduler.hold_cancel"


async def test_lot_scrap_route_status_mapping(client, runtime):
    url = "/api/lot/L-0001/scrap"
    body = {"reason": "operator"}
    # No active run -> 409.
    assert (await client.post(url, json=body)).status_code == 409
    await client.post("/api/run/start", json=START)
    runtime.lots.update(
        {"L-0001": "queued", "L-0002": "done", "L-0003": "failed", "L-0004": "pre_release"}
    )
    runtime.lots["L-0005"] = "banked"
    runtime.lots["L-0006"] = "running"

    runtime.exchange.publish.reset_mock()
    r = await client.post(url, json=body)
    assert r.status_code == 200 and r.json() == {"accepted": True}
    assert runtime.exchange.publish.await_args.kwargs["routing_key"] == "scheduler.scrap"
    assert (await client.post("/api/lot/L-0005/scrap", json={"reason": "other"})).status_code == 200

    assert (await client.post("/api/lot/L-9999/scrap", json=body)).status_code == 404
    assert (await client.post("/api/lot/L-0006/scrap", json=body)).status_code == 200
    for oid in ("L-0002", "L-0003", "L-0004"):
        assert (await client.post(f"/api/lot/{oid}/scrap", json=body)).status_code == 409, oid

    runtime.exchange.publish.reset_mock()
    for bad in ({"reason": "capacity"}, {"reason": "operator", "extra": 1}, {}):
        assert (await client.post(url, json=bad)).status_code == 422, bad
    assert runtime.exchange.publish.await_count == 0


async def test_lot_detail_route_200_404_422(client):
    ok = await client.get("/api/lot/L-1")
    assert ok.status_code == 200
    assert ok.json()["order_id"] == "L-1"
    assert (await client.get("/api/lot/L-2")).status_code == 404
    assert (await client.get("/api/lot/" + "x" * 65)).status_code == 422
    assert (await client.get("/api/lot/bad%20id")).status_code == 422
