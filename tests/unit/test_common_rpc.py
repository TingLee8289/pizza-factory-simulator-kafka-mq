"""Unit tests for the shared RPC client (fakes only)."""

import asyncio
import json
from datetime import timedelta

import pytest

from pizza.common.envelope import EstimateWip, parse_message, utc_now
from pizza.common.rpc import RpcBusy, RpcClient, RpcTimeout


class FakeQueue:
    name = "amq.gen-rpc"

    async def consume(self, callback, no_ack=False):
        self.callback = callback


class FakeMsg:
    def __init__(self, raw: bytes, correlation_id) -> None:
        self.body = raw
        self.correlation_id = correlation_id


class Env:
    def __init__(self) -> None:
        self.published: list = []
        self.declares = 0
        self.queue = FakeQueue()
        self.responder = None  # callable(cmd, corr) -> list[FakeMsg]
        env = self

        class Channel:
            async def declare_queue(inner, name, **kw):
                env.declares += 1
                return env.queue

        class Exchange:
            async def publish(inner, message, routing_key):
                cmd = parse_message(message.body)
                env.published.append((routing_key, cmd, message.correlation_id))
                if env.responder is not None:
                    for m in env.responder(cmd, message.correlation_id):
                        asyncio.get_running_loop().call_soon(
                            lambda m=m: asyncio.ensure_future(env.queue.callback(m))
                        )

        async def get_exchange():
            return Exchange()

        self.Channel = Channel
        self.get_exchange = get_exchange

    def client(self, **kw) -> RpcClient:
        return RpcClient(lambda: self.Channel(), self.get_exchange, **kw)


def _cmd() -> EstimateWip:
    return EstimateWip.make(
        run_id="r", machine_id="t", deadline_ts=utc_now() + timedelta(seconds=5)
    )


async def test_matching_reply_resolves_and_bad_ignored():
    env = Env()
    env.responder = lambda cmd, corr: [
        FakeMsg(b"not json", corr),
        FakeMsg(json.dumps({"ok": True}).encode(), "unknown"),
        FakeMsg(json.dumps([1]).encode(), corr),
        FakeMsg(json.dumps({"ok": True, "x": 1}).encode(), corr),
    ]
    client = env.client()
    out = await client.request(_cmd(), "scheduler.estimate_wip", 1.0)
    assert out == {"ok": True, "x": 1}
    assert env.published[0][0] == "scheduler.estimate_wip"


async def test_timeout_cleans_pending():
    env = Env()
    client = env.client()
    with pytest.raises(RpcTimeout):
        await client.request(_cmd(), "k", 0.05)
    assert client._pending == {}


async def test_busy_publishes_nothing():
    env = Env()
    client = env.client(max_inflight=1)
    first = asyncio.ensure_future(client.request(_cmd(), "k", 0.3))
    await asyncio.sleep(0.05)
    assert len(client._pending) == 1
    with pytest.raises(RpcBusy):
        await client.request(_cmd(), "k", 0.3)
    assert len(env.published) == 1
    with pytest.raises(RpcTimeout):
        await first


async def test_reply_queue_declared_once():
    env = Env()
    client = env.client()
    await asyncio.gather(
        *(client.request(_cmd(), "k", 0.05) for _ in range(3)),
        return_exceptions=True,
    )
    assert env.declares == 1


async def test_publish_stall_raises_rpc_timeout():
    """A silently stalled broker publish must not hang the caller (debug host-sleep (a))."""
    env = Env()
    never = asyncio.Event()

    class StalledExchange:
        async def publish(self, message, routing_key):
            await never.wait()

    async def get_exchange():
        return StalledExchange()

    client = RpcClient(lambda: env.Channel(), get_exchange)
    with pytest.raises(RpcTimeout):
        await asyncio.wait_for(client.request(_cmd(), "k", 0.05), 2.0)
    assert client._pending == {}


async def test_exchange_lookup_stall_raises_rpc_timeout():
    env = Env()
    never = asyncio.Event()

    async def stalled_get_exchange():
        await never.wait()

    client = RpcClient(lambda: env.Channel(), stalled_get_exchange)
    with pytest.raises(RpcTimeout):
        await asyncio.wait_for(client.request(_cmd(), "k", 0.05), 2.0)
    assert client._pending == {}


async def test_reply_queue_setup_stall_raises_rpc_timeout_then_recovers():
    env = Env()
    release = asyncio.Event()

    class SlowChannel:
        async def declare_queue(self, name, **kw):
            if not release.is_set():
                await release.wait()
            env.declares += 1
            return env.queue

    client = RpcClient(lambda: SlowChannel(), env.get_exchange)
    with pytest.raises(RpcTimeout):
        await asyncio.wait_for(client.request(_cmd(), "k", 0.05), 2.0)
    assert client._pending == {} and client._reply_queue is None
    release.set()
    env.responder = lambda cmd, corr: [FakeMsg(json.dumps({"ok": True}).encode(), corr)]
    assert await client.request(_cmd(), "k", 1.0) == {"ok": True}
