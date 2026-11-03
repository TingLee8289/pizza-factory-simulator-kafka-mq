"""Plan 07-08: rule fact, comparison/run-history wiring and Phase 7 REST routes.

Task 1 exercises DashboardRuntime (state.py fold + app.py wiring) with injected
fakes, mirroring tests/unit/test_dashboard_kpi_relay.py's fixtures. Task 2
(appended separately) exercises the new REST routes with a FakeRuntime wiring
a real RunController + ComparisonRunner behind a fake AMQP exchange, mirroring
test_dashboard_sse.py.
"""

import asyncio
import contextlib
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pytest

from pizza.common.envelope import (
    ComparisonResult,
    KpiSnapshot,
    RuleChanged,
    RunParams,
    parse_message,
)
from pizza.dashboard.app import DashboardRuntime, create_app
from pizza.dashboard.comparison import ComparisonRunner, detect_interrupted
from pizza.dashboard.control import RunController
from pizza.dashboard.hub import Hub
from pizza.dashboard.settings import DashboardSettings
from pizza.dashboard.state import event_payload, fold, line_snapshot, new_line_state
from pizza.kpi.recompute import NoStoredSnapshot
from tests.unit.dashboard_fixtures import RUN, at, full_timeline
from tests.unit.test_kpi_snapshot_schema import TS, _full


def _settings(**over) -> DashboardSettings:
    return DashboardSettings(**over)


def _rule_fact(rule: str, offset: float = 0.0, run_id: str = RUN) -> RuleChanged:
    return RuleChanged.make(run_id=run_id, machine_id="scheduler", ts=at(offset), rule=rule)


def _kpi_snap(
    *,
    run_id: str = RUN,
    i: int = 0,
    run_params: RunParams | None = None,
    run_started_ts=None,
):
    base = _full().model_dump()
    base.update(
        run_id=run_id,
        ts=TS + timedelta(seconds=i),
        run_params=run_params,
        run_started_ts=run_started_ts,
    )
    return KpiSnapshot.model_validate(base)


def _comparison_result(**over) -> ComparisonResult:
    kwargs = dict(
        run_id="r-x",
        machine_id="kpi",
        comparison_id="c-abc",
        round=1,
        rule="fifo",
        seed=42,
        n_orders=5,
        n_completed=5,
        n_failed=0,
        late_count=0,
        mean_cycle_time_s=10.0,
        mean_tardiness_s=0.0,
        max_tardiness_s=0.0,
        pct_late=0.0,
        makespan_s=50.0,
        partial=False,
        k=1.5,
        arrival_rate_per_min=2.0,
        random_breakdowns=True,
    )
    kwargs.update(over)
    return ComparisonResult.make(**kwargs)


# =====================================================================
# Task 1: LineState.rule, runtime wiring
# =====================================================================


def test_fold_rule_changed_newest_ts_wins_and_other_run_ignored():
    """Test 1: RuleChanged folds with newest-ts-wins; other run ids are ignored."""
    state = new_line_state(RUN)
    assert line_snapshot(state)["rule"] is None  # default before any fact (D-04 choice)

    state = fold(state, _rule_fact("edd", offset=1.0))
    assert line_snapshot(state)["rule"] == "edd"

    # An older-ts fact does not override the newest value.
    state = fold(state, _rule_fact("fifo", offset=0.0))
    assert line_snapshot(state)["rule"] == "edd"

    # A newer-ts fact does override.
    state = fold(state, _rule_fact("spt", offset=2.0))
    assert line_snapshot(state)["rule"] == "spt"

    # A RuleChanged for another run is ignored entirely (run-id gate in fold()).
    state = fold(state, _rule_fact("fifo", offset=5.0, run_id="other"))
    assert line_snapshot(state)["rule"] == "spt"

    payload = event_payload(_rule_fact("edd", offset=1.0))
    assert payload["kind"] == "rule_changed"
    assert payload["rule"] == "edd"


async def test_on_kpi_comparison_result_routes_to_runner_and_publishes():
    """Test 2: ComparisonResult is handled before the run_id filter; does not set latest_kpi."""
    rt = DashboardRuntime(_settings())
    sub = rt.hub.subscribe("events")
    rt.state.run_id = RUN
    result = _comparison_result(run_id="other-run")  # run_id differs from state.run_id

    await rt.on_kpi(result)

    assert rt.latest_kpi is None
    first = await sub.get()
    assert first.kind == "comparison_result"
    assert first.payload["run_id"] == "other-run"
    assert first.payload["rule"] == "fifo"
    second = await sub.get()
    assert second.kind == "comparison"
    assert len(second.payload["results"]) == 1

    # A duplicate (same comparison_id/round key) is not double counted.
    await rt.on_kpi(result)
    status = rt.comparison_status()
    assert len(status["results"]) == 1


async def test_on_kpi_other_run_kpisnapshot_updates_runs_not_latest():
    """Test 3: a KpiSnapshot for another run updates the runs list, not latest_kpi."""
    rt = DashboardRuntime(_settings())
    rt.state.run_id = RUN
    params = RunParams(seed=7, rule="edd", max_orders=30, comparison_id="c-x", round=2)
    snap = _kpi_snap(run_id="other-run", run_params=params, run_started_ts=TS)

    await rt.on_kpi(snap)

    assert rt.latest_kpi is None
    runs = rt.runs_list()
    assert len(runs) == 1
    row = runs[0]
    assert row["run_id"] == "other-run"
    assert row["rule"] == "edd"
    assert row["seed"] == 7
    assert row["n_orders"] == 30
    assert row["comparison_id"] == "c-x"
    assert row["round"] == 2
    assert row["has_snapshot"] is True


async def test_runs_list_capped_internally_returns_20_newest():
    """Test 3: the internal runs dict stays bounded; runs_list returns 20 newest."""
    rt = DashboardRuntime(_settings())
    for i in range(205):
        await rt.on_kpi(_kpi_snap(run_id=f"r-{i:04d}", i=i))

    assert len(rt.runs) <= 200
    runs = rt.runs_list()
    assert len(runs) == 20
    assert runs[0]["run_id"] == "r-0204"  # newest first


async def test_recover_rebuilds_result_store_and_runs_and_flags_interrupted():
    """Test 4: _recover rebuilds comparison results/runs; detect_interrupted -> status()."""
    rt = DashboardRuntime(_settings())
    comp_id = "c-old"
    snaps = []
    for i, (rule, rnd) in enumerate((("fifo", 1), ("edd", 2), ("spt", 3))):
        params = RunParams(seed=1, rule=rule, max_orders=5, comparison_id=comp_id, round=rnd)
        snaps.append(
            _kpi_snap(
                run_id=f"r-round{rnd}",
                i=i,
                run_params=params,
                run_started_ts=TS + timedelta(seconds=i),
            )
        )
    result = _comparison_result(run_id="r-round1", comparison_id=comp_id, round=1, rule="fifo")

    await rt._recover([], [*snaps, result])

    assert len(rt.runs_list()) == 3
    status = rt.comparison_status()
    assert len(status["results"]) == 1
    assert status["interrupted"] is True  # only 1/3 results, comp_id not active


def test_detect_interrupted_pure_helper():
    runs = {"r-1": {"comparison_id": "c-1"}, "r-2": {"comparison_id": None}}
    assert detect_interrupted(runs, {}, None) == "c-1"
    assert detect_interrupted(runs, {}, "c-1") is None
    complete = {("c-1", 1): object(), ("c-1", 2): object(), ("c-1", 3): object()}
    assert detect_interrupted(runs, complete, None) is None


async def test_tick_loop_calls_on_progress_and_publishes_round_close_once():
    """Test 5: the tick loop drives on_progress only while round_running; close fires once."""
    rt = DashboardRuntime(_settings())
    rt.settings = SimpleNamespace(state_interval_s=0.01)  # type: ignore[assignment]
    for msg in full_timeline(2, run_id=RUN):
        rt.state = fold(rt.state, msg)
    rt.state.run_id = RUN
    rt.controller._info.update(run_id=RUN, state="running")
    rt.comparison._state = "round_running"
    rt.comparison._comparison_id = "c-x"
    rt.comparison._round = 1
    rt.comparison._n_orders = 2  # matches the 2 completed lots in full_timeline(2, ...)

    calls: list[bool] = []

    async def fake_close(partial: bool) -> None:
        calls.append(partial)

    rt.controller.send_round_close = fake_close  # type: ignore[method-assign]

    task = asyncio.create_task(rt._tick_loop())
    await asyncio.sleep(0.08)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert calls == [False]  # published exactly once despite multiple tick iterations


def test_snapshot_and_tick_payload_contract_keys_include_phase7_fields():
    """Test 6: snapshot_payload gains comparison/runs; tick_payload gains rule."""
    rt = DashboardRuntime(_settings())
    snapshot = rt.snapshot_payload()
    assert "comparison" in snapshot
    assert "runs" in snapshot
    assert snapshot["comparison"]["state"] == "idle"
    assert snapshot["runs"] == []
    tick = rt.tick_payload()
    assert "rule" in tick
    assert tick["rule"] is None


async def test_on_kpi_legacy_snapshot_without_run_params_lists_with_none_fields():
    """Test 7: pre-Phase-7 snapshots (no run_params) are listed with None fields."""
    rt = DashboardRuntime(_settings())
    snap = _kpi_snap(run_id="legacy-run", run_params=None, run_started_ts=None)

    await rt.on_kpi(snap)

    runs = rt.runs_list()
    assert len(runs) == 1
    row = runs[0]
    assert row["run_id"] == "legacy-run"
    assert row["seed"] is None
    assert row["n_orders"] is None
    assert row["rule"] is None
    assert row["comparison_id"] is None
    assert row["round"] is None
    assert row["has_snapshot"] is True


# =====================================================================
# Task 2: REST routes (comparison control, CSV, runs, recompute)
# =====================================================================


class FakeExchange:
    def __init__(self) -> None:
        self.sent: list[tuple[str, object]] = []

    async def publish(self, message, routing_key: str) -> None:
        self.sent.append((routing_key, parse_message(message.body)))

    def keys(self) -> list[str]:
        return [k for k, _ in self.sent]


class ComparisonFakeRuntime:
    """A FakeRuntime wiring a real RunController + ComparisonRunner (no brokers)."""

    def __init__(self) -> None:
        self.hub = Hub(max_clients=20)
        self.exchange = FakeExchange()

        async def get_exchange():
            return self.exchange

        async def purge():
            return {}

        self.controller = RunController(SimpleNamespace(), get_exchange, purge)
        self.comparison = ComparisonRunner(self.controller, on_change=self._on_comparison_change)
        self.runs: dict[str, dict] = {}
        self.settings = DashboardSettings()
        self._recompute_cfg = (SimpleNamespace(), {})
        self.recompute_sem = asyncio.Semaphore(1)

    async def _on_comparison_change(self, _event, _status) -> None:
        self.hub.publish("events", "comparison", self.comparison_status())

    def comparison_status(self) -> dict:
        return self.comparison.status(self.runs)

    def runs_list(self, limit: int = 20) -> list[dict]:
        items = sorted(self.runs.values(), key=lambda r: r["started_ts"] or "", reverse=True)
        return items[:limit]

    def recompute_cfg(self):
        return self._recompute_cfg

    def snapshot_payload(self) -> dict:
        return {
            "seq": self.hub.seq,
            "replaying": False,
            "comparison": self.comparison_status(),
            "runs": self.runs_list(),
        }


@pytest.fixture
def runtime() -> ComparisonFakeRuntime:
    return ComparisonFakeRuntime()


@pytest.fixture
async def client(runtime):
    app = create_app(runtime)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def test_comparison_start_defaults_validation_and_conflict(client):
    """Test 1: start with defaults -> round_running; invalid bodies 422; second start 409."""
    r = await client.post("/api/comparison/start", json={})
    assert r.status_code == 200
    assert r.json()["state"] == "round_running"

    for bad in (
        {"n_orders": 4},
        {"n_orders": 100},
        {"k": 0.5},
        {"rule": "fifo"},
        {"bogus_field": 1},
    ):
        r2 = await client.post("/api/comparison/start", json=bad)
        assert r2.status_code == 422, bad

    r3 = await client.post("/api/comparison/start", json={})
    assert r3.status_code == 409


async def test_comparison_next_before_result_and_end_without_round_conflict(client):
    """Test 2: next before a result is 409; end with no round is 409; cancel is 200."""
    r = await client.post("/api/comparison/next")
    assert r.status_code == 409

    r2 = await client.post("/api/comparison/end")
    assert r2.status_code == 409

    await client.post("/api/comparison/start", json={})
    r3 = await client.post("/api/comparison/cancel")
    assert r3.status_code == 200
    assert r3.json()["state"] == "cancelled"


async def test_get_comparison_returns_status_dict(client, runtime):
    """Test 3: GET /api/comparison returns the status dict (state, round, results, best, ...)."""
    r = await client.get("/api/comparison")
    assert r.status_code == 200
    body = r.json()
    for key in ("state", "round", "results", "best", "interrupted"):
        assert key in body
    assert body["state"] == "idle"


async def test_comparison_csv_route(client, runtime):
    """Test 4: CSV route (header-only + filename when empty; validates comparison_id)."""
    r = await client.get("/api/comparison.csv")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/csv")
    assert 'filename="comparison-0.csv"' in r.headers["content-disposition"]
    lines = r.text.strip().split("\n")
    assert len(lines) == 1  # header only, no results yet

    r2 = await client.get("/api/comparison.csv", params={"comparison_id": "bad id"})
    assert r2.status_code == 422

    # Seed the result store directly and fetch again, filtered by comparison_id.
    result = _comparison_result(comparison_id="c-seed", round=1, seed=99)
    await runtime.comparison.on_result(result)
    r3 = await client.get("/api/comparison.csv", params={"comparison_id": "c-seed"})
    assert r3.status_code == 200
    assert 'filename="comparison-99.csv"' in r3.headers["content-disposition"]
    body_lines = r3.text.strip().split("\n")
    assert len(body_lines) == 2  # header + 1 row


async def test_list_runs_caps_at_20_newest_first(client, runtime):
    """Test 5: GET /api/runs returns at most 20 runs, newest first, with has_snapshot."""
    for i in range(25):
        runtime.runs[f"r-{i:02d}"] = {
            "run_id": f"r-{i:02d}",
            "rule": "fifo",
            "seed": i,
            "n_orders": 10,
            "comparison_id": None,
            "round": None,
            "started_ts": f"2026-01-01T00:00:{i:02d}.000Z",
            "has_snapshot": True,
        }
    r = await client.get("/api/runs")
    assert r.status_code == 200
    runs = r.json()["runs"]
    assert len(runs) == 20
    assert runs[0]["run_id"] == "r-24"
    assert all(row["has_snapshot"] for row in runs)


async def test_recompute_route_validates_and_serialises(client, runtime, monkeypatch):
    """Test 6: recompute validates run_id, maps NoStoredSnapshot/other errors, serialises."""
    import pizza.dashboard.routes as routes_mod

    # Malformed run_id: 422 without calling the recompute function at all.
    bad_run_recompute = MagicMock(side_effect=AssertionError("should not be called"))
    monkeypatch.setattr(routes_mod, "run_recompute", bad_run_recompute)
    r = await client.post("/api/runs/not-a-run-id/recompute")
    assert r.status_code == 422
    bad_run_recompute.assert_not_called()

    valid_run_id = "r-20260101-120000-abcd"

    def ok_recompute(*args, **kwargs):
        return {"run_id": valid_run_id, "matched": True, "fields": []}

    monkeypatch.setattr(routes_mod, "run_recompute", ok_recompute)
    r2 = await client.post(f"/api/runs/{valid_run_id}/recompute")
    assert r2.status_code == 200
    assert r2.json() == {"run_id": valid_run_id, "matched": True, "fields": []}

    def missing_recompute(*args, **kwargs):
        raise NoStoredSnapshot("nope")

    monkeypatch.setattr(routes_mod, "run_recompute", missing_recompute)
    r3 = await client.post(f"/api/runs/{valid_run_id}/recompute")
    assert r3.status_code == 404
    assert r3.json() == {"detail": "No stored snapshot for this run"}

    def broken_recompute(*args, **kwargs):
        raise ConnectionError("amqp://user:secret@broker boom")

    monkeypatch.setattr(routes_mod, "run_recompute", broken_recompute)
    r4 = await client.post(f"/api/runs/{valid_run_id}/recompute")
    assert r4.status_code == 502
    assert r4.json() == {"detail": "Replay failed"}
    assert "secret" not in r4.text

    # Two concurrent calls are serialised by the semaphore: the second only
    # proceeds once the first (slow) call releases it.
    order: list[str] = []

    def slow_recompute(*args, **kwargs):
        order.append("start")
        return {"run_id": valid_run_id, "matched": True, "fields": []}

    monkeypatch.setattr(routes_mod, "run_recompute", slow_recompute)
    assert runtime.recompute_sem.locked() is False
    async with runtime.recompute_sem:
        assert runtime.recompute_sem.locked() is True
        task = asyncio.create_task(client.post(f"/api/runs/{valid_run_id}/recompute"))
        await asyncio.sleep(0.02)
        assert order == []  # blocked while the semaphore is held
    r5 = await task
    assert r5.status_code == 200
    assert order == ["start"]


async def test_comparison_routes_reject_cross_origin_post(client):
    """Test 7: cross-origin POST to the new routes is rejected like /api/rule."""
    r = await client.post(
        "/api/comparison/start", json={}, headers={"origin": "http://evil.example"}
    )
    assert r.status_code == 403


async def test_set_rule_accepts_edd_spt_and_blocks_during_comparison(client, runtime):
    """Test 8: /api/rule accepts edd/spt (SCHED-03) and 409s while a comparison runs."""
    r = await client.post("/api/rule", json={"rule": "edd"})
    assert r.status_code == 409  # no active run yet

    from pizza.dashboard.control import StartRequest

    await runtime.controller.start(StartRequest())
    r2 = await client.post("/api/rule", json={"rule": "edd"})
    assert r2.status_code == 200
    assert r2.json() == {"rule": "edd"}
    r3 = await client.post("/api/rule", json={"rule": "spt"})
    assert r3.status_code == 200
    assert r3.json() == {"rule": "spt"}

    runtime.controller.comparison_active = True
    r4 = await client.post("/api/rule", json={"rule": "fifo"})
    assert r4.status_code == 409
