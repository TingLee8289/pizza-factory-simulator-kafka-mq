import asyncio

import pytest

from pizza.dashboard.hub import Hub, TooManySubscribers, kpi_point
from tests.unit.test_kpi_snapshot_schema import _full, _warming


async def _drain(sub):
    out = []
    while not sub.queue.empty():
        out.append(await sub.get())
    return out


async def test_shared_seq_and_channel_isolation():
    hub = Hub()
    ev = hub.subscribe("events")
    st = hub.subscribe("state")
    seqs = [
        hub.publish("events", "a", {"n": 1}),
        hub.publish("state", "b", {"n": 2}),
        hub.publish("events", "c", {"n": 3}),
    ]
    assert seqs == sorted(set(seqs)) and seqs[0] >= 1
    assert hub.seq == seqs[-1]
    got_ev = await _drain(ev)
    got_st = await _drain(st)
    assert [(i.seq, i.kind, i.payload) for i in got_ev] == [
        (seqs[0], "a", {"n": 1}),
        (seqs[2], "c", {"n": 3}),
    ]
    assert [(i.seq, i.kind) for i in got_st] == [(seqs[1], "b")]


async def test_get_waits_for_publish():
    hub = Hub()
    sub = hub.subscribe("events")
    task = asyncio.create_task(sub.get())
    await asyncio.sleep(0)
    assert not task.done()
    hub.publish("events", "x", {})
    assert (await task).kind == "x"


async def test_full_queue_drops_oldest_and_never_blocks():
    hub = Hub(queue_size=3)
    sub = hub.subscribe("events")
    for i in range(10):
        hub.publish("events", "k", {"i": i})
    items = await _drain(sub)
    assert [it.payload["i"] for it in items] == [7, 8, 9]
    assert sub.dropped == 7


async def test_slow_client_does_not_affect_others():
    hub = Hub(queue_size=2)
    slow = hub.subscribe("events")
    fast = hub.subscribe("events")
    seen = []
    for i in range(6):
        hub.publish("events", "k", {"i": i})
        seen.extend(await _drain(fast))
    assert [it.payload["i"] for it in seen] == list(range(6))
    assert fast.dropped == 0
    assert slow.dropped == 4


async def test_client_cap_and_unsubscribe():
    hub = Hub(max_clients=2)
    a = hub.subscribe("events")
    hub.subscribe("state")
    assert hub.client_count == 2
    with pytest.raises(TooManySubscribers):
        hub.subscribe("events")
    hub.unsubscribe(a)
    assert hub.client_count == 1
    hub.subscribe("events")
    assert hub.client_count == 2
    hub.unsubscribe(a)  # idempotent
    assert hub.client_count == 2


def test_kpi_point_mapping():
    p = kpi_point(_full())
    assert p["t"] == _full().ts.timestamp()
    assert "throughput_per_min" not in p
    assert p["pizzas_per_min"] == pytest.approx(0.06 * 60)
    assert p["wip_now"] == 10
    assert p["wip_pizzas"] == 20
    assert p["wip_avg"] == 4.2
    assert "wip_avg_pizzas" in p
    assert p["cycle_mean_s"] == 50.0
    assert p["cycle_p95_s"] == 75.0
    assert p["little_rel_error"] == 0.02


def test_kpi_point_tolerates_none():
    p = kpi_point(_warming())
    assert "throughput_per_min" not in p
    assert p["pizzas_per_min"] is None
    assert p["wip_avg"] is None
    assert p["wip_avg_pizzas"] is None
    assert p["cycle_mean_s"] is None
    assert p["cycle_p95_s"] is None
    assert p["little_rel_error"] is None
    assert p["wip_now"] == 0
    assert p["wip_pizzas"] == 0


def test_kpi_ring_capped_at_600():
    hub = Hub()
    snap = _full()
    for i in range(700):
        hub.add_kpi(snap.model_copy(update={"wip_now": i}))
    assert len(hub.kpi_ring) == 600
    assert hub.kpi_ring[0]["wip_now"] == 100
    assert hub.kpi_ring[-1]["wip_now"] == 699


def test_add_kpi_returns_point():
    hub = Hub()
    point = hub.add_kpi(_full())
    assert point is hub.kpi_ring[-1]


def test_panel_rings_bounded_and_none_kept():
    hub = Hub(panel_ring_size=30)
    for i in range(45):
        hub.add_panel(
            {
                "rabbit.station.bake.in.ready": float(i),
                "rabbit.station.bake.in.unacked": None,
                "rabbit.dlq": 0.0,
                "kafka.kpi.lag": float(i),
            }
        )
    ring = hub.panel_rings["rabbit.station.bake.in.ready"]
    assert len(ring) == 30
    assert list(ring)[0] == 15.0 and list(ring)[-1] == 44.0
    assert all(v is None for v in hub.panel_rings["rabbit.station.bake.in.unacked"])
    assert len(hub.panel_rings["kafka.kpi.lag"]) == 30


def test_clear_rings_keeps_seq():
    hub = Hub()
    hub.publish("events", "k", {})
    hub.add_kpi(_full())
    hub.add_panel({"rabbit.dlq": 1.0})
    seq = hub.seq
    hub.clear_rings()
    assert len(hub.kpi_ring) == 0
    assert hub.panel_rings == {} or all(len(r) == 0 for r in hub.panel_rings.values())
    assert hub.seq == seq
    assert hub.publish("events", "k", {}) == seq + 1
