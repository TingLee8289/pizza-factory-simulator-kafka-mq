"""GET /api/order/estimate route and runtime wiring (fakes only, no brokers)."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from pizza.common.constants import STATIONS
from pizza.common.rpc import RpcClient
from pizza.dashboard import app as app_mod
from pizza.dashboard.app import DashboardRuntime, create_app
from pizza.dashboard.control import ConflictError, NotAvailableError, RunController
from pizza.dashboard.hub import Hub
from pizza.dashboard.orders import EstimateRequest
from pizza.dashboard.settings import DashboardSettings

T = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def _reply(request: EstimateRequest) -> dict:
    return {
        "eta_ts": T,
        "station_eta": {s: T for s in STATIONS},
        "due_ts": T,
        "lead_s": 42.0,
        "buffer_k": request.buffer_k,
        "degraded": False,
    }


class FakeRuntime:
    def __init__(self) -> None:
        self.hub = Hub(max_clients=5)
        self.exchange = MagicMock()
        self.exchange.publish = AsyncMock()
        self.order_calls = 0
        self.raises: Exception | None = None
        self.seen: list[EstimateRequest] = []

        async def get_exchange():
            return self.exchange

        async def purge():
            return {}

        async def place(controller, request):
            self.order_calls += 1
            return {}

        async def estimate(controller, request):
            self.seen.append(request)
            if self.raises is not None:
                raise self.raises
            return _reply(request)

        self.controller = RunController(
            None, get_exchange, purge, place_order_fn=place, estimate_order_fn=estimate
        )
        self.settings = DashboardSettings()


@pytest.fixture
def runtime():
    return FakeRuntime()


@pytest.fixture
async def client(runtime):
    transport = httpx.ASGITransport(app=create_app(runtime))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def test_estimate_ok_shape(client):
    r = await client.get(
        "/api/order/estimate", params={"product": "margherita", "qty": 2, "buffer_k": 1.2}
    )
    assert r.status_code == 200
    body = r.json()
    assert body["eta_ts"].startswith("2026-01-01T12:00:00")
    assert body["due_ts"].startswith("2026-01-01T12:00:00")
    assert set(body["station_eta"]) == set(STATIONS) and len(body["station_eta"]) == 7
    assert body["lead_s"] == 42.0 and body["buffer_k"] == 1.2 and body["degraded"] is False


@pytest.mark.parametrize(
    "params",
    [
        {},
        {"product": "nope", "qty": 1},
        {"qty": 1},
        {"product": "margherita", "qty": 0},
        {"product": "margherita", "qty": 1, "buffer_k": 0.4},
        {"product": "margherita", "qty": 1, "buffer_k": 3.5},
        {"product": "margherita", "qty": 1, "buffer_k": "nan"},
        {"product": "margherita", "qty": 1, "bogus": "x"},
    ],
)
async def test_estimate_validation_422(client, runtime, params):
    r = await client.get("/api/order/estimate", params=params)
    assert r.status_code == 422, params
    assert runtime.seen == []


async def test_estimate_error_mapping(client, runtime):
    params = {"product": "margherita", "qty": 1}
    runtime.raises = ConflictError("cannot place order: no active run")
    assert (await client.get("/api/order/estimate", params=params)).status_code == 409
    runtime.raises = NotAvailableError("scheduler did not answer")
    assert (await client.get("/api/order/estimate", params=params)).status_code == 422
    runtime.raises = ValueError("invalid")
    assert (await client.get("/api/order/estimate", params=params)).status_code == 422


async def test_estimate_is_side_effect_free(client, runtime):
    r = await client.get("/api/order/estimate", params={"product": "veggie", "qty": 1})
    assert r.status_code == 200
    assert runtime.order_calls == 0
    runtime.exchange.publish.assert_not_called()


def test_runtime_wires_eta_client_and_estimate_fn(monkeypatch):
    rt = DashboardRuntime(DashboardSettings())
    assert isinstance(rt.eta_client, RpcClient)
    assert rt.eta_client is not rt.order_client
    assert rt.controller._estimate_order_fn == rt._estimate_order


async def test_runtime_estimate_delegates(monkeypatch):
    rt = DashboardRuntime(DashboardSettings())
    mock = AsyncMock(return_value={"ok": 1})
    monkeypatch.setattr(app_mod, "estimate_order", mock)
    out = await rt._estimate_order(rt.controller, "req")
    assert out == {"ok": 1}
    mock.assert_awaited_once_with(rt.controller, "req", rt.eta_client, rt.settings)


# -- GET /api/lot/{order_id}/eta ---------------------------------------------


class _FakeCtl:
    def __init__(self, state="running", run_id="r1"):
        self.state = state
        self._run_id = run_id

    def run_info(self):
        return {"state": self.state, "run_id": self._run_id}


class _FakeEtaClient:
    def __init__(self, reply=None, exc=None):
        self.reply = reply
        self.exc = exc
        self.calls: list[tuple] = []

    async def request(self, command, key, timeout_s):
        self.calls.append((command, key, timeout_s))
        if self.exc is not None:
            raise self.exc
        return self.reply


def _lot_rt(client, controller=None, replaying=False):
    rt = DashboardRuntime(DashboardSettings())
    rt.controller = controller or _FakeCtl()
    rt.eta_client = client
    rt.replaying = replaying
    return rt


ISO = "2026-01-01T12:00:05.000Z"


async def test_lot_eta_ok_passthrough_and_request_shape():
    from pizza.common.envelope import EstimateWip

    reply = {
        "ok": True,
        "stations": {
            "spin": {"in": None, "out": ISO},
            "sauce": {"in": ISO, "out": ISO},
            "bogus": {"in": ISO, "out": ISO},
            "bake": {"in": ISO, "out": 5},
        },
    }
    client = _FakeEtaClient(reply)
    out = await _lot_rt(client).lot_eta("L-0001")
    assert out == {
        "available": True,
        "stations": {"spin": {"in": None, "out": ISO}, "sauce": {"in": ISO, "out": ISO}},
    }
    cmd, key, _ = client.calls[0]
    assert isinstance(cmd, EstimateWip)
    assert cmd.order_id == "L-0001" and cmd.machine_id == "dashboard"
    assert key == "scheduler.estimate_wip"


@pytest.mark.parametrize(
    "client,kw",
    [
        (_FakeEtaClient(exc=__import__("pizza.common.rpc", fromlist=["x"]).RpcTimeout()), {}),
        (_FakeEtaClient(exc=__import__("pizza.common.rpc", fromlist=["x"]).RpcBusy()), {}),
        (_FakeEtaClient(exc=RuntimeError("boom")), {}),
        (_FakeEtaClient({"ok": False, "code": "not_found"}), {}),
        (_FakeEtaClient({"ok": True, "stations": "nope"}), {}),
        (_FakeEtaClient({"ok": True, "stations": {}}), {}),
        (_FakeEtaClient({"ok": True, "stations": {"spin": {"out": ISO}}}), {"replaying": True}),
        (
            _FakeEtaClient({"ok": True, "stations": {"spin": {"out": ISO}}}),
            {"controller": _FakeCtl("none", None)},
        ),
        (
            _FakeEtaClient({"ok": True, "stations": {"spin": {"out": ISO}}}),
            {"controller": _FakeCtl(run_id=None)},
        ),
    ],
)
async def test_lot_eta_unavailable_shapes(client, kw):
    out = await _lot_rt(client, **kw).lot_eta("L-0001")
    assert out == {"available": False, "stations": {}}


async def test_lot_eta_route_delegates_and_validates():
    class RT(FakeRuntime):
        async def lot_eta(self, order_id):
            return {
                "available": True,
                "stations": {"spin": {"in": None, "out": ISO}},
                "id": order_id,
            }

    transport = httpx.ASGITransport(app=create_app(RT()))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        r = await c.get("/api/lot/L-0001/eta")
        assert r.status_code == 200 and r.json()["id"] == "L-0001"
        assert (await c.get("/api/lot/" + "x" * 65 + "/eta")).status_code == 422
        assert (await c.get("/api/lot/bad%20id/eta")).status_code == 422


def test_snapshot_payload_has_no_per_station_eta():
    rt = DashboardRuntime(DashboardSettings())
    rt.controller = _FakeCtl()
    from pizza.dashboard.state import new_line_state

    rt.state = new_line_state("r1")
    assert "stations" not in str(rt.snapshot_payload()["line"]["lots"])
    assert not hasattr(rt, "lot_eta_cache")
