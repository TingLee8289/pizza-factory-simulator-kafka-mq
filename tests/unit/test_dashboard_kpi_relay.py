"""DashboardRuntime tests with injected fakes (no brokers)."""

import asyncio
from datetime import timedelta
from types import SimpleNamespace

from pizza.dashboard.app import DashboardRuntime, RuntimeDeps
from pizza.dashboard.settings import DashboardSettings
from tests.unit.dashboard_fixtures import RUN, T0, full_timeline, lifecycle
from tests.unit.test_kpi_snapshot_schema import TS, _full


def _settings(**over) -> DashboardSettings:
    return DashboardSettings(**over)


def _snap(run_id: str = RUN, i: int = 0):
    return _full().__class__.model_validate(
        {**_full().model_dump(), "run_id": run_id, "ts": TS + timedelta(seconds=i)}
    )


class FakeController:
    def __init__(self) -> None:
        self.adopted = None

    def run_info(self):
        return {"state": "none", "run_id": None}

    def adopt(self, run_id, paused=False):
        self.adopted = (run_id, paused)


class FakeTail:
    instances: list = []

    def __init__(self, bootstrap, group, start, topics, callback, loop, *, thread_name):
        self.start_offsets = start
        self.topics = topics
        self.callback = callback
        self.thread_name = thread_name
        self.started = False
        self.stopped = False
        FakeTail.instances.append(self)

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True


def test_on_kpi_relays_snapshot_untouched():
    async def go():
        rt = DashboardRuntime(_settings())
        sub = rt.hub.subscribe("state")
        rt.state.run_id = RUN
        snap = _snap()
        await rt.on_kpi(snap)
        assert rt.latest_kpi is snap
        rt.publish_tick()
        item = await sub.get()
        assert item.kind == "tick"
        assert item.payload["kpi"] == snap.model_dump(mode="json")
        assert item.payload["kpi_point"] is not None
        assert item.payload["seq"] == item.seq
        rt.publish_tick()
        item2 = await sub.get()
        assert item2.payload["kpi_point"] is None  # a point is handed out once

    asyncio.run(go())


def test_kpi_ring_capped_after_700_snapshots():
    async def go():
        rt = DashboardRuntime(_settings())
        rt.state.run_id = RUN
        for i in range(700):
            await rt.on_kpi(_snap(i=i))
        assert len(rt.hub.kpi_ring) == 600
        assert len(rt.snapshot_payload()["history"]) == 600

    asyncio.run(go())


def test_on_kpi_ignores_other_run_and_non_snapshots():
    async def go():
        rt = DashboardRuntime(_settings())
        rt.state.run_id = RUN
        await rt.on_kpi(_snap(run_id="other"))
        await rt.on_kpi(lifecycle("created"))  # not a KpiSnapshot: ignored, no raise
        assert rt.latest_kpi is None
        assert len(rt.hub.kpi_ring) == 0

    asyncio.run(go())


def test_on_change_started_and_reset_clear_state():
    async def go():
        rt = DashboardRuntime(_settings())
        sub = rt.hub.subscribe("state")
        rt.state.run_id = RUN
        await rt.on_kpi(_snap())
        rt.hub.add_panel({"a": 1.0})
        await rt.on_change("started", {"run_id": "r-new"})
        assert rt.state.run_id == "r-new" and rt.state.adopt is False
        assert rt.latest_kpi is None and len(rt.hub.kpi_ring) == 0
        assert not rt.hub.panel_rings
        assert (await sub.get()).kind == "tick"
        await rt.on_kpi(_snap())  # stale run is ignored after start
        assert rt.latest_kpi is None
        await rt.on_change("reset", {"run_id": None})
        assert rt.state.run_id is None
        assert (await sub.get()).kind == "tick"
        await rt.on_change("paused", {"run_id": None})
        assert (await sub.get()).kind == "tick"

    asyncio.run(go())


def test_line_event_folds_and_publishes_and_never_raises():
    async def go():
        rt = DashboardRuntime(_settings())
        sub = rt.hub.subscribe("events")
        msg = lifecycle("created")
        await rt.on_line_event(msg)
        assert rt.state.run_id == RUN
        item = await sub.get()
        assert item.kind == "fact" and item.payload["seq"] == item.seq
        await rt.on_line_event(object())  # garbage: swallowed
        assert sub.queue.empty()

    asyncio.run(go())


def test_tick_loop_survives_exceptions():
    async def go():
        rt = DashboardRuntime(_settings(state_interval_s=0.5))
        calls = []

        def boom():
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("x")
            return {"seq": 0}

        rt.tick_payload = boom  # type: ignore[method-assign]
        rt.settings = SimpleNamespace(state_interval_s=0.01)  # type: ignore[assignment]
        task = asyncio.create_task(rt._tick_loop())
        await asyncio.sleep(0.15)
        alive = not task.done()
        task.cancel()
        assert alive and len(calls) >= 2

    asyncio.run(go())


def test_snapshot_payload_contract_keys():
    rt = DashboardRuntime(_settings())
    payload = rt.snapshot_payload()
    assert set(payload) == {
        "seq", "server_ts", "replaying", "run", "line", "kpi", "history", "panel",
        "panel_history", "comparison", "runs", "time_scale", "host_sleep",
    }  # fmt: skip
    tick = rt.tick_payload()
    assert set(tick) == {
        "seq", "server_ts", "run", "paused", "rule", "stations", "lots", "counts", "kpi", "time_scale",
        "kpi_point", "panel", "done_recent", "host_sleep",
    }  # fmt: skip


def test_on_panel_feeds_ring():
    rt = DashboardRuntime(_settings())
    rt.on_panel(
        {
            "as_of": "x",
            "error": False,
            "rabbit": {"queues": {"q1": {"ready": 3, "unacked": 1}}, "dlq": 2},
            "kafka": {"lag": {"kpi": 5}},
        }
    )
    assert list(rt.hub.panel_rings["rabbit.q1.ready"]) == [3]
    assert list(rt.hub.panel_rings["kafka.kpi.lag"]) == [5]
    assert rt.snapshot_payload()["panel_history"]["rabbit.dlq"] == [2]


def test_startup_replays_newest_run_and_starts_tails():
    async def go():
        FakeTail.instances = []
        old = full_timeline(2, run_id="r-old")
        new = full_timeline(2, run_id=RUN, t0=T0 + timedelta(hours=1))
        snaps = [_snap(run_id="r-old", i=1), _snap(i=2), _snap(i=1)]

        def fake_replay(bootstrap, topics, timeout, group):
            if topics == ["kpi.snapshots"]:
                return snaps, {("kpi.snapshots", 0): 3}
            return old + new, {("station.events", 0): 9}

        class FakeConn:
            async def channel(self):
                return SimpleNamespace(close=_aclose)

            async def close(self):
                pass

        async def _aclose():
            pass

        async def fake_connect(url):
            return FakeConn()

        async def noop_verify(rt, conn):
            pass

        class FakePanel:
            async def run(self, on_update, stop):
                await stop.wait()

            async def aclose(self):
                pass

        deps = RuntimeDeps(
            replay=fake_replay,
            tail_factory=FakeTail,
            connect=fake_connect,
            make_producer=lambda b: object(),
            verify=noop_verify,
            panel=FakePanel(),
        )
        rt = DashboardRuntime(_settings(), deps)
        assert rt.replaying is True
        await rt.startup()
        try:
            assert rt.replaying is False
            assert rt.state.run_id == RUN
            assert rt.controller.run_info()["run_id"] == RUN
            assert len(rt.hub.kpi_ring) == 2  # newest run only
            assert rt.latest_kpi.ts == TS + timedelta(seconds=2)
            names = sorted(t.thread_name for t in FakeTail.instances)
            assert names == ["dashboard-kpi-tail", "dashboard-tail"]
            assert all(t.started for t in FakeTail.instances)
            kpi_tail = next(t for t in FakeTail.instances if t.thread_name == "dashboard-kpi-tail")
            assert kpi_tail.start_offsets == {("kpi.snapshots", 0): 3}
        finally:
            await rt.shutdown()
        assert all(t.stopped for t in FakeTail.instances)

    asyncio.run(go())


def test_empty_replay_has_no_run():
    async def go():
        rt = DashboardRuntime(_settings())
        await rt._recover([], [])
        assert rt.state.run_id is None and rt.latest_kpi is None

    asyncio.run(go())
