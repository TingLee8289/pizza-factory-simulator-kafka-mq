"""Unit tests for manual order placement via the generator RPC (fakes only)."""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from pizza.common.constants import MAX_ORDER_QTY
from pizza.common.envelope import EstimateEta, ManualOrderRequest, parse_message
from pizza.common.rpc import RpcClient
from pizza.dashboard.control import ConflictError, NotAvailableError
from pizza.dashboard.orders import (
    EstimateRequest,
    ManualOrderClient,
    OrderRequest,
    estimate_order,
    place_manual_order,
)


class FakeController:
    def __init__(self, state="running", run_id="r-test") -> None:
        self.state = state
        self._run_id = run_id

    def run_info(self):
        return {"state": self.state, "run_id": self._run_id}


class FakeQueue:
    name = "amq.gen-test"

    async def consume(self, callback, no_ack=False):
        self.callback = callback


class FakeMsg:
    def __init__(self, body: dict, correlation_id: str | None) -> None:
        self.body = json.dumps(body).encode()
        self.correlation_id = correlation_id


class Env:
    """Fake channel + control exchange; ``responder`` builds the generator reply."""

    def __init__(self) -> None:
        self.published: list[tuple] = []
        self.settings = SimpleNamespace(manual_order_timeout_s=0.2)
        self.queue = FakeQueue()
        self.responder = None  # callable(cmd) -> dict | None
        env = self

        class Channel:
            async def declare_queue(inner, name, **kw):
                return env.queue

        class Exchange:
            async def publish(inner, message, routing_key):
                cmd = parse_message(message.body)
                env.published.append((routing_key, cmd, message.reply_to, message.correlation_id))
                if env.responder is not None:
                    body = env.responder(cmd)
                    if body is not None:
                        asyncio.get_running_loop().call_soon(
                            lambda: asyncio.ensure_future(
                                env.queue.callback(FakeMsg(body, message.correlation_id))
                            )
                        )

        async def get_exchange():
            return Exchange()

        self.client = ManualOrderClient(lambda: Channel(), get_exchange)
        self.rpc = RpcClient(lambda: Channel(), get_exchange)

    async def place(self, request, controller=None):
        return await place_manual_order(
            controller or FakeController(), request, self.client, self.settings
        )


def _ok(cmd):
    return {
        "ok": True,
        "order_id": "L-0007",
        "run_id": cmd.run_id,
        "due_ts": "2030-01-01T00:00:00.000Z",
        "priority": cmd.priority,
    }


@pytest.fixture
def env() -> Env:
    return Env()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"product": "margherita", "qty": 0},
        {"product": "margherita", "qty": MAX_ORDER_QTY + 1},
        {"product": "calzone", "qty": 1},
        {"product": "margherita", "qty": 1, "due_ts": datetime(2030, 1, 1)},
        {"product": "margherita", "qty": 1, "extra": 1},
        {"product": "margherita", "qty": 1, "rush": True},
        {"product": "margherita", "qty": 1, "priority": 0},
        {"product": "margherita", "qty": 1, "priority": 100},
        {"product": "margherita", "qty": 1, "priority": 50.5},
        {"product": "margherita", "qty": 1, "priority": "50"},
        {"product": "margherita", "qty": 1, "priority": True},
    ],
)
def test_order_request_rejects(kwargs):
    with pytest.raises(ValidationError):
        OrderRequest(**kwargs)


def test_order_request_accepts_bounds():
    OrderRequest(product="veggie", qty=1)
    OrderRequest(product="veggie", qty=MAX_ORDER_QTY, priority=1)
    OrderRequest(product="veggie", qty=1, priority=99)


async def test_conflict_without_run(env):
    with pytest.raises(ConflictError):
        await env.place(OrderRequest(product="margherita", qty=1), FakeController("none", None))
    assert env.published == []


async def test_publishes_one_command_and_returns_reply(env):
    env.responder = _ok
    before = datetime.now(UTC)
    due = datetime.now(UTC) + timedelta(hours=1)
    out = await env.place(OrderRequest(product="pepperoni", qty=3, priority=10, due_ts=due))
    assert len(env.published) == 1
    routing_key, cmd, reply_to, corr = env.published[0]
    assert routing_key == "generator.manual_order" and isinstance(cmd, ManualOrderRequest)
    assert (cmd.run_id, cmd.product, cmd.qty, cmd.priority) == ("r-test", "pepperoni", 3, 10)
    assert abs((cmd.due_ts - due).total_seconds()) < 0.002
    assert reply_to == "amq.gen-test" and corr == str(cmd.event_id)
    assert 0 < (cmd.deadline_ts - before).total_seconds() <= 0.2 + 1
    assert out["order_id"] == "L-0007" and out["run_id"] == "r-test" and out["priority"] == 10
    assert out["due_ts"] == datetime(2030, 1, 1, tzinfo=UTC)
    assert env.client._pending == {}


async def test_no_due_ts_sent_as_none(env):
    env.responder = _ok
    await env.place(OrderRequest(product="veggie", qty=1), FakeController("paused"))
    assert env.published[0][1].due_ts is None


@pytest.mark.parametrize(
    ("code", "exc"),
    [("no_run", ConflictError), ("expired", NotAvailableError), ("invalid", ValueError)],
)
async def test_error_replies_mapped(env, code, exc):
    env.responder = lambda cmd: {"ok": False, "code": code, "detail": "nope"}
    with pytest.raises(exc, match="nope"):
        await env.place(OrderRequest(product="veggie", qty=1))


async def test_timeout_raises_and_cleans_pending(env):
    env.responder = None
    with pytest.raises(NotAvailableError):
        await env.place(OrderRequest(product="veggie", qty=1))
    assert len(env.published) == 1
    assert env.client._pending == {}


async def test_past_due_rejected_locally(env):
    past = datetime.now(UTC) - timedelta(minutes=1)
    with pytest.raises(ValueError, match="due_ts"):
        await env.place(OrderRequest(product="veggie", qty=1, due_ts=past))
    assert env.published == []


async def test_unknown_or_late_reply_ignored(env):
    await env.client._ensure()
    await env.queue.callback(FakeMsg({"ok": True}, "unknown-corr"))
    await env.queue.callback(FakeMsg({"ok": True}, None))
    bad = FakeMsg({}, "x")
    bad.body = b"not json"
    await env.queue.callback(bad)
    assert env.client._pending == {}


# ---- Phase 7.1: dry-run estimate and buffer_k ----------------------------------------


def _est_ok(cmd):
    return {
        "ok": True,
        "eta_lead_s": 100,
        "station_lead_s": {"bake": 60, "package": 100},
        "degraded": False,
    }


async def test_estimate_returns_composed_times(env):
    env.responder = _est_ok
    env.settings.eta_timeout_s = 0.2
    before = datetime.now(UTC)
    out = await estimate_order(
        FakeController(),
        EstimateRequest(product="veggie", qty=2, priority=5, buffer_k=1.5),
        env.rpc,
        env.settings,
    )
    assert len(env.published) == 1
    key, cmd, _, _ = env.published[0]
    assert key == "scheduler.estimate_eta" and isinstance(cmd, EstimateEta)
    assert (cmd.product, cmd.qty, cmd.priority, cmd.buffer_k) == ("veggie", 2, 5, 1.5)
    assert 0 < (cmd.deadline_ts - before).total_seconds() <= 0.2 + 1
    assert abs((out["eta_ts"] - before).total_seconds() - 100) < 2
    assert abs((out["due_ts"] - before).total_seconds() - 150) < 2
    assert set(out["station_eta"]) == {"bake", "package"}
    assert out["lead_s"] == 100 and out["buffer_k"] == 1.5 and out["degraded"] is False
    assert out["eta_ts"].microsecond % 1000 == 0 and out["eta_ts"].tzinfo is not None
    assert env.rpc._pending == {}


async def test_estimate_no_run(env):
    with pytest.raises(ConflictError):
        await estimate_order(
            FakeController("none", None),
            EstimateRequest(product="veggie", qty=1),
            env.rpc,
            env.settings,
        )
    assert env.published == []


@pytest.mark.parametrize(
    ("code", "exc"),
    [("no_run", ConflictError), ("expired", NotAvailableError), ("invalid", ValueError)],
)
async def test_estimate_error_codes(env, code, exc):
    env.responder = lambda cmd: {"ok": False, "code": code, "detail": "boom"}
    with pytest.raises(exc, match="boom"):
        await estimate_order(
            FakeController(), EstimateRequest(product="veggie", qty=1), env.rpc, env.settings
        )


async def test_estimate_timeout_and_busy(env):
    env.settings.eta_timeout_s = 0.05
    with pytest.raises(NotAvailableError):
        await estimate_order(
            FakeController(), EstimateRequest(product="veggie", qty=1), env.rpc, env.settings
        )
    assert env.rpc._pending == {}

    class BusyClient:
        async def request(self, command, routing_key, timeout_s):
            from pizza.common.rpc import RpcBusy

            raise RpcBusy("x")

    with pytest.raises(NotAvailableError):
        await estimate_order(
            FakeController(), EstimateRequest(product="veggie", qty=1), BusyClient(), env.settings
        )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"product": "calzone", "qty": 1},
        {"product": "veggie", "qty": 0},
        {"product": "veggie", "qty": MAX_ORDER_QTY + 1},
        {"product": "veggie", "qty": 1, "buffer_k": 0.49},
        {"product": "veggie", "qty": 1, "buffer_k": 3.01},
        {"product": "veggie", "qty": 1, "buffer_k": float("nan")},
        {"product": "veggie", "qty": 1, "buffer_k": float("inf")},
        {"product": "veggie", "qty": 1, "extra": 1},
    ],
)
def test_estimate_request_rejects(kwargs):
    with pytest.raises(ValidationError):
        EstimateRequest(**kwargs)
    if "extra" not in kwargs and "buffer_k" not in kwargs:
        with pytest.raises(ValidationError):
            OrderRequest(**kwargs)


def test_estimate_request_defaults():
    r = EstimateRequest(product="veggie", qty=1)
    assert r.buffer_k == 1.5 and r.priority == 50
    assert OrderRequest(product="veggie", qty=1).buffer_k == 1.5
    with pytest.raises(ValidationError):
        OrderRequest(product="veggie", qty=1, buffer_k=3.5)


async def test_place_forwards_buffer_k_and_eta(env):
    env.responder = lambda cmd: {**_ok(cmd), "eta_ts": "2029-12-31T00:00:00.000Z"}
    due = datetime.now(UTC) + timedelta(hours=1)
    out = await env.place(OrderRequest(product="veggie", qty=1, buffer_k=2.0, due_ts=due))
    cmd = env.published[0][1]
    assert cmd.buffer_k == 2.0 and abs((cmd.due_ts - due).total_seconds()) < 0.002
    assert out["eta_ts"] == datetime(2029, 12, 31, tzinfo=UTC)


async def test_place_eta_none_when_absent(env):
    env.responder = _ok
    out = await env.place(OrderRequest(product="veggie", qty=1))
    assert out["eta_ts"] is None
