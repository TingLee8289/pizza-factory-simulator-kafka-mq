"""Background current-estimate poller and SSE merge (Phase 7.1 plan 08, D-06)."""

import asyncio
from datetime import timedelta

import pytest

from pizza.common.envelope import EstimateWip, OrderLifecycle
from pizza.common.rpc import RpcBusy, RpcTimeout
from pizza.dashboard.app import DashboardRuntime
from pizza.dashboard.settings import DashboardSettings
from pizza.dashboard.state import fold, new_line_state
from tests.unit.dashboard_fixtures import RUN, at

ETA = "2026-10-21T10:00:05.000Z"


class FakeController:
    def __init__(self, state: str = "running", run_id: str | None = RUN) -> None:
        self.state = state
        self._run_id = run_id

    def run_info(self):
        return {"state": self.state, "run_id": self._run_id}

    def adopt(self, run_id, paused=False):  # pragma: no cover - unused
        pass


class FakeClient:
    def __init__(self, reply=None, exc: Exception | None = None) -> None:
        self.reply = reply if reply is not None else {"ok": True, "eta": {"L-0001": ETA}}
        self.exc = exc
        self.calls: list[tuple] = []

    async def request(self, command, key, timeout_s):
        self.calls.append((command, key, timeout_s))
        if self.exc is not None:
            raise self.exc
        return self.reply


def _fact(state, order_id, offset=0.0):
    return OrderLifecycle.make(
        run_id=RUN,
        machine_id="scheduler",
        ts=at(offset),
        state=state,
        order_id=order_id,
        product="margherita",
        qty=1,
        due_ts=at(3600.0),
        created_ts=at(0.0),
        eta_ts=at(100.0),
    )


def _rt(client=None, controller=None, **settings):
    rt = DashboardRuntime(DashboardSettings(**settings))
    rt.controller = controller or FakeController()
    rt.eta_client = client or FakeClient()
    rt.replaying = False
    rt.state = new_line_state(RUN)
    for f in (
        _fact("released", "L-0001"),
        _fact("released", "L-0002"),
        _fact("completed", "L-0002", 50.0),
    ):
        rt.state = fold(rt.state, f)
    return rt


def _by_id(lots):
    return {lt["order_id"]: lt for lt in lots}


@pytest.mark.asyncio
async def test_poll_populates_cache_and_merges_into_payloads():
    rt = _rt()
    await rt._poll_eta_once()
    assert rt.eta_now == {"L-0001": ETA}
    snap = _by_id(rt.snapshot_payload()["line"]["lots"])
    assert snap["L-0001"]["eta_now_ts"] == ETA
    assert snap["L-0002"].get("eta_now_ts") is None
    tick = rt.tick_payload()
    assert _by_id(tick["lots"])["L-0001"]["eta_now_ts"] == ETA
    for lot in [*snap.values(), *tick["lots"], *tick["done_recent"]]:
        assert not {"visits", "steps", "station_eta", "failures", "priority_changes"} & lot.keys()


@pytest.mark.asyncio
async def test_payload_builders_do_no_rpc():
    rt = _rt()
    await rt._poll_eta_once()
    before = len(rt.eta_client.calls)
    for _ in range(100):
        rt.snapshot_payload()
        rt.tick_payload()
    assert len(rt.eta_client.calls) == before


@pytest.mark.asyncio
async def test_no_rpc_without_run_replaying_or_disabled():
    rt = _rt(controller=FakeController("none", None))
    await rt._poll_eta_once()
    rt2 = _rt()
    rt2.replaying = True
    await rt2._poll_eta_once()
    rt3 = _rt(eta_poll_enabled=False)
    await rt3._poll_eta_once()
    assert not rt.eta_client.calls
    assert not rt2.eta_client.calls
    assert not rt3.eta_client.calls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "client",
    [
        FakeClient(exc=RpcTimeout()),
        FakeClient(exc=RpcBusy()),
        FakeClient(exc=RuntimeError("boom")),
        FakeClient(reply={"ok": False, "error": "x"}),
        FakeClient(reply={"ok": True, "eta": "nope"}),
    ],
)
async def test_failures_keep_previous_cache_and_loop_continues(client):
    rt = _rt(client=client, eta_poll_s=0.01)
    rt.eta_now = {"L-0001": "old"}
    task = asyncio.create_task(rt._eta_poll_loop())
    await asyncio.sleep(0.08)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(client.calls) >= 2  # loop survived the failure
    assert rt.eta_now == {"L-0001": "old"}


@pytest.mark.asyncio
async def test_cache_filters_non_strings_and_caps():
    reply = {"ok": True, "eta": {"a": ETA, "b": 5, 7: "x", **{f"k{i}": ETA for i in range(300)}}}
    rt = _rt(client=FakeClient(reply=reply))
    await rt._poll_eta_once()
    assert "b" not in rt.eta_now
    assert all(isinstance(k, str) and isinstance(v, str) for k, v in rt.eta_now.items())
    assert len(rt.eta_now) <= 200


@pytest.mark.asyncio
async def test_cache_cleared_on_started_and_reset():
    rt = _rt()
    await rt._poll_eta_once()
    assert rt.eta_now
    await rt.on_change("started", {"run_id": "R2"})
    assert rt.eta_now == {}
    rt.eta_now = {"x": ETA}
    await rt.on_change("reset", {})
    assert rt.eta_now == {}


@pytest.mark.asyncio
async def test_request_shape():
    rt = _rt(eta_timeout_s=2.0)
    await rt._poll_eta_once()
    command, key, timeout_s = rt.eta_client.calls[0]
    assert isinstance(command, EstimateWip)
    assert command.run_id == RUN
    assert command.machine_id == "dashboard"
    assert key == "scheduler.estimate_wip"
    assert timeout_s == 2.0
    lead = (command.deadline_ts - command.ts).total_seconds()
    assert timedelta(seconds=0) < timedelta(seconds=lead) <= timedelta(seconds=2.5)


@pytest.mark.asyncio
async def test_tick_payload_has_done_recent():
    rt = _rt()
    tick = rt.tick_payload()
    assert [lt["order_id"] for lt in tick["done_recent"]] == ["L-0002"]
    assert len(tick["done_recent"]) <= 20
    for key in ("seq", "run", "paused", "rule", "stations", "lots", "counts", "kpi"):
        assert key in tick
    assert all(lt["state"] != "done" for lt in tick["lots"])


@pytest.mark.asyncio
async def test_poll_task_started_and_cancelled():
    rt = _rt(eta_poll_s=0.01)
    rt._eta_task = asyncio.create_task(rt._eta_poll_loop())
    await asyncio.sleep(0.03)
    assert not rt._eta_task.done()
    task = rt._eta_task
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
