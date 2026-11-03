"""Unit tests for the dashboard run controller (fake exchange, no brokers)."""

import re
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from pizza.common.constants import STATIONS
from pizza.common.envelope import (
    LotBank,
    LotHoldCancel,
    LotRelease,
    LotScrap,
    MachineBreak,
    MachineRepair,
    RunPause,
    RunReset,
    RunResume,
    RunStarted,
    SetArrivalRate,
    SetRecipe,
    SetRule,
    parse_message,
)
from pizza.dashboard.control import (
    BankRequest,
    BreakRequest,
    ConflictError,
    NotAvailableError,
    PriorityRequest,
    RateRequest,
    RecipeRequest,
    RuleRequest,
    RunController,
    ScrapRequest,
    StartRequest,
    UnknownLotError,
    UnknownStationError,
    command_targets,
    line_queue_names,
    purge_line_queues,
)


class FakeExchange:
    def __init__(self) -> None:
        self.sent: list[tuple[str, object]] = []
        self.fail_at: int | None = None

    async def publish(self, message, routing_key: str) -> None:
        if self.fail_at is not None and len(self.sent) >= self.fail_at:
            raise ConnectionError("broker down")
        self.sent.append((routing_key, parse_message(message.body)))

    def keys(self) -> list[str]:
        return [k for k, _ in self.sent]


class Env:
    def __init__(self) -> None:
        self.exchange = FakeExchange()
        self.purge_calls = 0
        self.events: list[tuple[str, dict]] = []
        self.order: list[str] = []

        async def get_exchange():
            return self.exchange

        async def purge():
            self.purge_calls += 1
            self.order.append(f"purge@{len(self.exchange.sent)}")

        async def on_change(event, info):
            self.events.append((event, info))

        self.lots: dict[str, str] = {}
        self.controller = RunController(
            SimpleNamespace(),
            get_exchange,
            purge,
            on_change=on_change,
            lot_state_fn=self.lots.get,
        )


@pytest.fixture
def env() -> Env:
    return Env()


def _start() -> StartRequest:
    return StartRequest(seed=7, arrival_rate_per_min=3.0, weights={"margherita": 2, "veggie": 1})


# -- command_targets ----------------------------------------------------------
def test_command_targets_exact_lists():
    stations = list(STATIONS)
    assert [k for k, _ in command_targets("pause")] == [
        "scheduler.pause",
        *(f"station.{s}.pause" for s in stations),
        "generator.pause",
    ]
    assert [k for k, _ in command_targets("resume")] == [
        *(f"station.{s}.resume" for s in stations),
        "scheduler.resume",
        "generator.resume",
    ]
    reset = command_targets("reset")
    assert reset[0] == ("generator.reset", "generator")
    assert reset[-1] == ("kpi.reset", "kpi-service")
    assert len(reset) == 2 + len(stations) + 1
    assert command_targets("rate") == [("generator.rate", "generator")]
    assert command_targets("rule") == [("scheduler.rule", "scheduler")]
    start = command_targets("start")
    assert start[-1] == ("generator.run", "generator")
    with pytest.raises(ValueError):
        command_targets("nope")


# -- request validation -------------------------------------------------------
@pytest.mark.parametrize(
    "kwargs",
    [
        {"arrival_rate_per_min": 0},
        {"arrival_rate_per_min": 12.5},
        {"seed": -1},
        {"seed": 2**31},
        {"weights": {"calzone": 1}},
        {"weights": {"margherita": -1}},
        {"weights": {"margherita": 0}},
        {"extra": 1},
    ],
)
def test_start_request_rejects(kwargs):
    with pytest.raises(ValidationError):
        StartRequest(**kwargs)


def test_rate_request_bounds():
    RateRequest(arrival_rate_per_min=12)
    for bad in (0, 12.01, -1):
        with pytest.raises(ValidationError):
            RateRequest(arrival_rate_per_min=bad)


# -- start --------------------------------------------------------------------
async def test_start_publishes_with_shared_ts_generator_last(env):
    info = await env.controller.start(_start())
    assert env.controller.state == "running"
    assert re.fullmatch(r"r-\d{8}-\d{6}-[0-9a-f]{4}", info["run_id"])
    assert info["seed"] == 7
    assert info["weights"] == {"margherita": 2, "veggie": 1}
    sent = env.exchange.sent
    assert [k for k, _ in sent] == [k for k, _ in command_targets("start")]
    assert sent[-1][0] == "generator.run"
    assert all(isinstance(c, RunStarted) for _, c in sent)
    assert len({c.ts for _, c in sent}) == 1
    assert {c.run_id for _, c in sent} == {info["run_id"]}
    params = sent[0][1].params
    assert params.seed == 7
    assert params.arrival_rate == pytest.approx(3.0 / 60.0)
    assert [e for e, _ in env.events] == ["started"]


async def test_start_mints_random_seed_when_none(env):
    info = await env.controller.start(StartRequest())
    assert isinstance(info["seed"], int)
    assert 0 <= info["seed"] < 2**31
    assert env.exchange.sent[0][1].params.seed == info["seed"]


async def test_start_twice_conflicts_without_publishing(env):
    await env.controller.start(_start())
    n = len(env.exchange.sent)
    with pytest.raises(ConflictError):
        await env.controller.start(_start())
    assert len(env.exchange.sent) == n


async def test_start_publish_failure_leaves_state(env):
    env.exchange.fail_at = 2
    with pytest.raises(ConnectionError):
        await env.controller.start(_start())
    assert env.controller.state == "none"
    assert env.controller.run_info()["run_id"] is None
    assert env.events == []


# -- pause / resume -------------------------------------------------------------
async def test_pause_resume_cycle(env):
    info = await env.controller.start(_start())
    env.exchange.sent.clear()
    await env.controller.pause()
    assert env.controller.state == "paused"
    assert env.exchange.keys() == [k for k, _ in command_targets("pause")]
    assert all(isinstance(c, RunPause) and c.run_id == info["run_id"] for _, c in env.exchange.sent)
    env.exchange.sent.clear()
    with pytest.raises(ConflictError):
        await env.controller.pause()
    assert env.exchange.sent == []
    await env.controller.resume()
    assert env.controller.state == "running"
    assert env.exchange.keys() == [k for k, _ in command_targets("resume")]
    assert all(isinstance(c, RunResume) for _, c in env.exchange.sent)
    assert [e for e, _ in env.events] == ["started", "paused", "resumed"]


async def test_pause_slept_s_is_carried_on_every_run_pause(env):
    await env.controller.start(_start())
    env.exchange.sent.clear()
    await env.controller.pause(slept_s=1027)
    assert env.exchange.sent
    assert all(isinstance(c, RunPause) and c.slept_s == 1027 for _, c in env.exchange.sent)


async def test_manual_pause_has_no_slept_s(env):
    await env.controller.start(_start())
    env.exchange.sent.clear()
    await env.controller.pause()
    assert all(c.slept_s is None for _, c in env.exchange.sent)


async def test_pause_resume_conflict_when_none_or_running(env):
    with pytest.raises(ConflictError):
        await env.controller.pause()
    with pytest.raises(ConflictError):
        await env.controller.resume()
    await env.controller.start(_start())
    with pytest.raises(ConflictError):
        await env.controller.resume()


async def test_pause_failure_keeps_state(env):
    await env.controller.start(_start())
    env.exchange.fail_at = len(env.exchange.sent) + 1
    with pytest.raises(ConnectionError):
        await env.controller.pause()
    assert env.controller.state == "running"


# -- reset ----------------------------------------------------------------------
async def test_reset_from_running_and_paused(env):
    for pause_first in (False, True):
        await env.controller.start(_start())
        if pause_first:
            await env.controller.pause()
        env.exchange.sent.clear()
        env.order.clear()
        await env.controller.reset()
        assert env.controller.state == "none"
        assert env.controller.run_info()["run_id"] is None
        assert env.exchange.keys() == [k for k, _ in command_targets("reset")]
        assert all(isinstance(c, RunReset) for _, c in env.exchange.sent)
        # purge strictly after all reset commands
        assert env.order == [f"purge@{len(command_targets('reset'))}"]
    assert env.purge_calls == 2
    assert env.events[-1][0] == "reset"


async def test_reset_conflicts_when_none_and_new_run_gets_new_id(env):
    with pytest.raises(ConflictError):
        await env.controller.reset()
    first = await env.controller.start(_start())
    await env.controller.reset()
    second = await env.controller.start(_start())
    assert first["run_id"] != second["run_id"]
    assert env.purge_calls == 1


# -- rate / rule ----------------------------------------------------------------
async def test_set_rate_publishes_per_second(env):
    info = await env.controller.start(_start())
    env.exchange.sent.clear()
    out = await env.controller.set_rate(RateRequest(arrival_rate_per_min=6))
    assert env.exchange.keys() == ["generator.rate"]
    cmd = env.exchange.sent[0][1]
    assert isinstance(cmd, SetArrivalRate)
    assert cmd.arrival_rate == pytest.approx(0.1)
    assert out["run_id"] == info["run_id"]
    assert out["arrival_rate_per_min"] == 6


async def test_set_rate_conflict_when_none(env):
    with pytest.raises(ConflictError):
        await env.controller.set_rate(RateRequest(arrival_rate_per_min=1))
    assert env.exchange.sent == []


async def test_set_rule_all_three_rules_accepted(env):
    """Task 1, Test 1: set_rule accepts fifo, edd, spt without NotAvailableError."""
    await env.controller.start(_start())
    env.exchange.sent.clear()
    for rule in ("fifo", "edd", "spt"):
        env.exchange.sent.clear()
        await env.controller.set_rule(RuleRequest(rule=rule))
        assert env.exchange.keys() == ["scheduler.rule"]
        cmd = env.exchange.sent[0][1]
        assert isinstance(cmd, SetRule)
        assert cmd.rule == rule


async def test_set_recipe_publishes_to_station_scheduler_and_kpi(env):
    info = await env.controller.start(_start())
    env.exchange.sent.clear()
    out = await env.controller.set_recipe(
        "bake", RecipeRequest(product="margherita", unit_time_s=9.5)
    )
    assert env.exchange.keys() == ["station.bake.recipe", "scheduler.recipe", "kpi.recipe"]
    cmds = [cmd for _, cmd in env.exchange.sent]
    assert all(isinstance(cmd, SetRecipe) for cmd in cmds)
    assert [(c.machine_id, c.station, c.product, c.unit_time_s) for c in cmds] == [
        ("bake", "bake", "margherita", 9.5),
        ("scheduler", "bake", "margherita", 9.5),
        ("kpi-service", "bake", "margherita", 9.5),
    ]
    assert {c.run_id for c in cmds} == {info["run_id"]}
    assert out == {
        "accepted": True,
        "station": "bake",
        "product": "margherita",
        "unit_time_s": 9.5,
    }


async def test_set_recipe_allowed_without_a_run(env):
    await env.controller.set_recipe("slice", RecipeRequest(product="veggie", unit_time_s=2))
    assert {c.run_id for _, c in env.exchange.sent} == {"config"}


async def test_set_recipe_rejects_unknown_station(env):
    with pytest.raises(UnknownStationError):
        await env.controller.set_recipe("fryer", RecipeRequest(product="veggie", unit_time_s=2))
    assert env.exchange.sent == []


def test_recipe_request_rejects_bad_values():
    for kwargs in (
        {"product": "calzone", "unit_time_s": 1},
        {"product": "veggie", "unit_time_s": 0},
        {"product": "veggie", "unit_time_s": 3601},
        {"product": "veggie", "unit_time_s": 1, "extra": 1},
    ):
        with pytest.raises(ValidationError):
            RecipeRequest(**kwargs)


# -- adopt / place_order ---------------------------------------------------------
async def test_adopt(env):
    env.controller.adopt("r-x", paused=True)
    assert env.controller.state == "paused"
    info = env.controller.run_info()
    assert info["run_id"] == "r-x" and info["seed"] is None
    env.controller.adopt("r-y")
    assert env.controller.state == "running"


async def test_place_order_delegates(env):
    async def fn(controller, request):
        return {"ok": request}

    env.controller._place_order_fn = fn
    assert await env.controller.place_order("req") == {"ok": "req"}


# -- purge -----------------------------------------------------------------------
async def test_purge_line_queues_tolerates_failures():
    names = line_queue_names()
    assert "dispatch.dlq" in names and "workorders.intake" in names
    assert "scheduler.rework.spin.hold" in names and "scheduler.rework.return" in names
    assert not any(n.endswith(".control") for n in names)

    purged: list[str] = []

    async def get_queue(name, ensure=False):
        q = MagicMock()
        if name == "station.bake.jobs":
            q.purge = AsyncMock(side_effect=RuntimeError("boom"))
        else:

            async def purge(name=name):
                purged.append(name)
                return SimpleNamespace(message_count=3)

            q.purge = purge
        return q

    channel = SimpleNamespace(get_queue=get_queue)
    out = await purge_line_queues(channel)
    assert set(out) == set(names)
    assert out["station.bake.jobs"] is None
    assert out["workorders.intake"] == 3
    assert "station.bake.jobs" not in purged
    assert len(purged) == len(names) - 1


# -- Phase 6 controls ------------------------------------------------------------


async def test_break_repair_publish_to_station_keys(env):
    c = env.controller
    with pytest.raises(ConflictError):
        await c.break_station("bake", BreakRequest(duration_s=30))
    with pytest.raises(ConflictError):
        await c.repair_station("bake")
    await c.start(_start())
    env.exchange.sent.clear()
    await c.break_station("bake", BreakRequest(duration_s=30))
    await c.break_station("bake")
    await c.repair_station("bake")
    keys = env.exchange.keys()
    assert keys == ["station.bake.break", "station.bake.break", "station.bake.repair"]
    brk = env.exchange.sent[0][1]
    assert isinstance(brk, MachineBreak) and brk.duration_s == 30 and brk.machine_id == "bake"
    assert brk.run_id == c.run_info()["run_id"]
    assert env.exchange.sent[1][1].duration_s is None
    assert isinstance(env.exchange.sent[2][1], MachineRepair)
    with pytest.raises(UnknownStationError):
        await c.break_station("nope")


@pytest.mark.parametrize("value", [0, -1, 3601])
def test_break_request_bounds(value):
    with pytest.raises(ValidationError):
        BreakRequest(duration_s=value)
    with pytest.raises(ValidationError):
        BreakRequest(duration_s=5, extra=1)


async def test_bank_release_publish_and_validate(env):
    c = env.controller
    await c.start(_start())
    env.exchange.sent.clear()
    await c.bank_lot("o-1", BankRequest(reason="capacity"))
    await c.release_lot("o-1")
    assert env.exchange.keys() == ["scheduler.bank", "scheduler.release"]
    bank = env.exchange.sent[0][1]
    assert isinstance(bank, LotBank) and bank.machine_id == "scheduler" and bank.order_id == "o-1"
    assert isinstance(env.exchange.sent[1][1], LotRelease)
    with pytest.raises(ValidationError):
        BankRequest(reason="bogus")
    for bad in ("", "-x", "a b", "x" * 65, "a/b"):
        with pytest.raises(ValueError):
            await c.release_lot(bad)


async def test_cancel_hold_publishes_and_validates(env):
    c = env.controller
    with pytest.raises(ConflictError):  # no active run
        await c.cancel_hold_lot("o-1")
    await c.start(_start())
    env.exchange.sent.clear()
    assert await c.cancel_hold_lot("o-1") == {"accepted": True}
    assert env.exchange.keys() == ["scheduler.hold_cancel"]
    cmd = env.exchange.sent[0][1]
    assert isinstance(cmd, LotHoldCancel) and cmd.order_id == "o-1"
    assert cmd.machine_id == "scheduler"
    for bad in ("", "-x", "a b", "x" * 65, "a/b"):
        with pytest.raises(ValueError):
            await c.cancel_hold_lot(bad)


async def test_random_breakdowns_flag_reaches_run_params(env):
    c = env.controller
    assert c.run_info()["random_breakdowns"] is False
    assert StartRequest().random_breakdowns is False
    await c.start(StartRequest(seed=1, random_breakdowns=True))
    started = [m for _, m in env.exchange.sent if isinstance(m, RunStarted)]
    assert started and all(m.params.random_breakdowns for m in started)
    assert c.run_info()["random_breakdowns"] is True
    c.adopt("r-x")
    assert c.run_info()["random_breakdowns"] is False


async def test_bank_publish_failure_keeps_info(env):
    c = env.controller
    await c.start(_start())
    before = c.run_info()
    env.exchange.fail_at = len(env.exchange.sent)
    with pytest.raises(ConnectionError):
        await c.bank_lot("o-1", BankRequest(reason="customer"))
    assert c.run_info() == before


# -- Phase 7: Comparison Runner extensions -----


def test_start_request_comparison_fields_and_defaults():
    """Task 1, Test 2: StartRequest carries comparison fields with proper defaults."""
    req = StartRequest()
    assert req.k == 2.0
    assert req.max_orders is None
    assert req.rule is None
    assert req.comparison_id is None
    assert req.round is None

    req = StartRequest(k=1.5, max_orders=30, rule="edd", comparison_id="c-x-1", round=2)
    assert req.k == 1.5
    assert req.max_orders == 30
    assert req.rule == "edd"
    assert req.comparison_id == "c-x-1"
    assert req.round == 2


def test_start_request_comparison_validation():
    """Task 1, Test 2: StartRequest validates comparison fields."""
    # Valid bounds
    StartRequest(k=1.0, max_orders=1)
    StartRequest(k=10.0, max_orders=10000)
    StartRequest(round=1)
    StartRequest(round=3)
    StartRequest(rule="fifo")
    StartRequest(rule="edd")
    StartRequest(rule="spt")
    StartRequest(comparison_id="c-x")
    StartRequest(comparison_id="c-a1b2c3d4")

    # Invalid bounds
    with pytest.raises(ValidationError):
        StartRequest(k=0.9)
    with pytest.raises(ValidationError):
        StartRequest(k=10.1)
    with pytest.raises(ValidationError):
        StartRequest(max_orders=0)
    with pytest.raises(ValidationError):
        StartRequest(max_orders=10001)
    with pytest.raises(ValidationError):
        StartRequest(round=0)
    with pytest.raises(ValidationError):
        StartRequest(round=4)
    with pytest.raises(ValidationError):
        StartRequest(rule="invalid")
    with pytest.raises(ValidationError):
        StartRequest(comparison_id="invalid")


async def test_start_with_comparison_params_reaches_run_params(env):
    """Task 1, Test 2: StartRequest comparison fields reach RunParams in RunStarted."""
    await env.controller.start(
        StartRequest(seed=42, k=1.5, max_orders=30, rule="edd", comparison_id="c-x-1", round=2)
    )
    # Check that all StartRequest fields are in run_info
    info = env.controller.run_info()
    assert info["k"] == 1.5
    assert info["max_orders"] == 30
    assert info["rule"] == "edd"
    assert info["comparison_id"] == "c-x-1"
    assert info["round"] == 2

    # Check that RunStarted.params carries them
    started = [m for _, m in env.exchange.sent if isinstance(m, RunStarted)]
    assert all(m.params.k == 1.5 for m in started)
    assert all(m.params.max_orders == 30 for m in started)
    assert all(m.params.rule == "edd" for m in started)
    assert all(m.params.comparison_id == "c-x-1" for m in started)
    assert all(m.params.round == 2 for m in started)


async def test_comparison_lock_blocks_mutations_except_repair_release(env):
    """Task 1, Test 3: comparison_active=True blocks
    pause/resume/reset/rate/rule/order/break/bank."""
    c = env.controller
    await c.start(_start())
    c.comparison_active = True

    # These should raise ConflictError
    with pytest.raises(ConflictError, match="Locked while a comparison is running"):
        await c.pause()
    with pytest.raises(ConflictError, match="Locked while a comparison is running"):
        await c.resume()
    with pytest.raises(ConflictError, match="Locked while a comparison is running"):
        await c.reset()
    with pytest.raises(ConflictError, match="Locked while a comparison is running"):
        await c.set_rate(RateRequest(arrival_rate_per_min=1))
    with pytest.raises(ConflictError, match="Locked while a comparison is running"):
        await c.set_rule(RuleRequest(rule="fifo"))
    with pytest.raises(ConflictError, match="Locked while a comparison is running"):
        await c.set_recipe("bake", RecipeRequest(product="veggie", unit_time_s=1))
    with pytest.raises(ConflictError, match="Locked while a comparison is running"):
        await c.place_order({"product": "margherita"})
    with pytest.raises(ConflictError, match="Locked while a comparison is running"):
        await c.break_station("bake")
    with pytest.raises(ConflictError, match="Locked while a comparison is running"):
        await c.bank_lot("o-1", BankRequest(reason="customer"))

    # These should NOT raise ConflictError
    await c.repair_station("bake")
    await c.release_lot("o-1")


async def test_reset_and_start_with_internal_bypass(env):
    """Task 1, Test 4: reset(_internal=True) and start(_internal=True) succeed
    while comparison_active."""
    c = env.controller
    await c.start(_start())
    c.comparison_active = True

    # _internal=True should bypass the lock
    await c.reset(_internal=True)
    assert c.state == "none"

    # start() without _internal should still work
    await c.start(StartRequest(seed=123), _internal=True)
    assert c.state == "running"


async def test_command_targets_close_round_and_stop_generation(env):
    """Task 1, Test 5: command_targets includes close_round and stop_generation."""
    assert command_targets("close_round") == [("kpi.close", "kpi-service")]
    assert command_targets("stop_generation") == [("generator.reset", "generator")]

    # Unknown kinds still raise ValueError
    with pytest.raises(ValueError):
        command_targets("unknown")


async def test_stop_generation_leaves_run_state_unchanged(env):
    """Task 1, Test 6: stop_generation publishes only to generator, doesn't mutate state."""
    c = env.controller
    await c.start(_start())
    env.exchange.sent.clear()
    env.events.clear()

    before_info = c.run_info()

    await c.stop_generation()

    # Should publish only to generator.reset
    assert env.exchange.keys() == ["generator.reset"]

    # Run info should be unchanged
    after_info = c.run_info()
    assert after_info == before_info

    # on_change should not be called
    assert env.events == []


async def test_send_round_close_publishes_to_kpi(env):
    """Task 1, Test 6b: send_round_close publishes RoundClose to kpi.close."""
    from pizza.common.envelope import RoundClose

    c = env.controller
    await c.start(_start())
    env.exchange.sent.clear()

    await c.send_round_close(partial=False)

    assert env.exchange.keys() == ["kpi.close"]
    cmd = env.exchange.sent[0][1]
    assert isinstance(cmd, RoundClose)
    assert cmd.partial is False
    assert cmd.run_id == c.run_info()["run_id"]


# -- quick-et6: simulation speed -------------------------------------------------------
def test_time_scale_request_validation():
    from pizza.dashboard.control import TimeScaleRequest

    for ok in (1, 2, 4, 8):
        assert TimeScaleRequest(scale=ok).scale == ok
    for bad in (0, -1, 3, 32, "x", None):
        with pytest.raises(ValidationError):
            TimeScaleRequest(scale=bad)


async def test_runtime_set_time_scale_publishes_fact(monkeypatch):
    from pizza.common.envelope import TimeScaleChanged
    from pizza.common.topology import TOPIC_TIMESCALE
    from pizza.dashboard import app as app_mod
    from pizza.dashboard.settings import DashboardSettings

    produced: list = []
    monkeypatch.setattr(
        app_mod,
        "produce_fact",
        lambda producer, topic, fact, key: produced.append((topic, fact, key)),
    )
    rt = app_mod.DashboardRuntime(DashboardSettings())
    rt.producer = object()
    assert rt.snapshot_payload()["time_scale"] == 1.0
    assert rt.tick_payload()["time_scale"] == 1.0
    assert await rt.set_time_scale(8) == {"scale": 8}
    topic, fact, key = produced[0]
    assert (topic, key) == (TOPIC_TIMESCALE, "line")
    assert isinstance(fact, TimeScaleChanged) and fact.scale == 8
    assert rt.snapshot_payload()["time_scale"] == 8.0
    assert rt.tick_payload()["time_scale"] == 8.0


async def test_runtime_time_scale_unchanged_on_publish_failure(monkeypatch):
    from pizza.dashboard import app as app_mod
    from pizza.dashboard.settings import DashboardSettings

    def boom(*_a):
        raise RuntimeError("kafka down")

    monkeypatch.setattr(app_mod, "produce_fact", boom)
    rt = app_mod.DashboardRuntime(DashboardSettings())
    with pytest.raises(RuntimeError):
        await rt.set_time_scale(4)
    assert rt.time_scale == 1.0


async def test_runtime_restores_time_scale_from_replay_and_tail():
    from pizza.common.envelope import TimeScaleChanged
    from pizza.dashboard.app import DashboardRuntime
    from pizza.dashboard.settings import DashboardSettings

    def scale(n):
        return TimeScaleChanged.make(run_id="unknown", machine_id="dashboard", scale=n)

    rt = DashboardRuntime(DashboardSettings())
    await rt._recover([scale(2), scale(8)], [])
    assert rt.time_scale == 8.0
    await rt.on_line_event(scale(4))
    assert rt.time_scale == 4.0


async def test_time_scale_route():
    import httpx

    from pizza.dashboard.app import create_app

    class FakeRuntime:
        def __init__(self) -> None:
            self.calls: list[int] = []
            self.fail = False

        async def set_time_scale(self, scale: int) -> dict:
            if self.fail:
                raise RuntimeError("secret broker detail")
            self.calls.append(scale)
            return {"scale": scale}

    rt = FakeRuntime()
    transport = httpx.ASGITransport(app=create_app(rt))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        ok = await client.post("/api/time_scale", json={"scale": 8})
        assert ok.status_code == 200 and ok.json() == {"scale": 8}
        for bad in ({"scale": 3}, {"scale": 0}, {"scale": -1}, {"scale": "x"}, {}):
            r = await client.post("/api/time_scale", json=bad)
            assert r.status_code == 422, bad
        rt.fail = True
        r = await client.post("/api/time_scale", json={"scale": 2})
        assert r.status_code == 503 and "secret" not in r.text
    assert rt.calls == [8]


# -- Phase 7.1: buffer_k / estimate_order / eta settings ---------------------------
@pytest.mark.parametrize("bad", [0.4, 10.1, float("nan")])
def test_start_request_buffer_k_rejects(bad):
    with pytest.raises(ValidationError):
        StartRequest(buffer_k=bad)


def test_start_request_buffer_k_default_none_and_accepts():
    assert StartRequest().buffer_k is None
    assert StartRequest(buffer_k=0.5).buffer_k == 0.5
    assert StartRequest(buffer_k=10).buffer_k == 10


@pytest.mark.parametrize(
    ("kwargs", "expected_buffer_k", "expected_k"),
    [
        ({"buffer_k": 1.5}, 1.5, 2.0),
        ({}, 1.5, 2.0),
        ({"comparison_id": "c-x-1", "k": 1.8}, 1.8, 1.8),
        ({"comparison_id": "c-x-1", "k": 1.8, "buffer_k": 2.5}, 2.5, 1.8),
        ({"k": 1.8}, 1.5, 1.8),
    ],
)
async def test_start_maps_buffer_k_into_run_params(env, kwargs, expected_buffer_k, expected_k):
    await env.controller.start(StartRequest(seed=1, **kwargs))
    started = [m for _, m in env.exchange.sent if isinstance(m, RunStarted)]
    assert started
    assert all(m.params.buffer_k == expected_buffer_k for m in started)
    assert all(m.params.k == expected_k for m in started)


async def test_estimate_order_guards_and_delegates(env):
    with pytest.raises(NotAvailableError, match="order estimates are not wired"):
        await env.controller.estimate_order("req")

    async def fn(controller, request):
        return {"ok": request}

    env.controller._estimate_order_fn = fn
    assert await env.controller.estimate_order("req") == {"ok": "req"}


async def test_estimate_order_propagates_comparison_lock(env):
    env.controller._estimate_order_fn = AsyncMock()
    env.controller.comparison_active = True
    with pytest.raises(ConflictError):
        await env.controller.estimate_order("req")


def test_eta_settings_defaults_and_bounds():
    from pizza.dashboard.settings import DashboardSettings

    s = DashboardSettings()
    assert s.eta_timeout_s == 1.5
    assert s.eta_poll_s == 1.0
    assert s.eta_poll_enabled is True
    with pytest.raises(ValidationError):
        DashboardSettings(eta_timeout_s=0)
    with pytest.raises(ValidationError):
        DashboardSettings(eta_timeout_s=11)
    with pytest.raises(ValidationError):
        DashboardSettings(eta_poll_s=0)


async def test_set_lot_priority_publishes_and_maps_errors(env):
    from pizza.common.envelope import LotSetPriority

    c = env.controller
    with pytest.raises(ConflictError):  # no active run
        await c.set_lot_priority("o-1", PriorityRequest(priority=5))
    await c.start(_start())
    env.lots.update({"o-1": "running", "o-2": "done", "o-3": "failed", "o-4": "pre_release"})
    env.exchange.sent.clear()

    out = await c.set_lot_priority("o-1", PriorityRequest(priority=5))
    assert out == {"accepted": True}
    key, cmd = env.exchange.sent[0]
    assert key == "scheduler.priority" and isinstance(cmd, LotSetPriority)
    assert (cmd.order_id, cmd.priority, cmd.machine_id) == ("o-1", 5, "scheduler")
    assert cmd.run_id == c.run_info()["run_id"]

    with pytest.raises(UnknownLotError):
        await c.set_lot_priority("o-9", PriorityRequest(priority=5))
    for oid in ("o-2", "o-3", "o-4"):
        with pytest.raises(ConflictError):
            await c.set_lot_priority(oid, PriorityRequest(priority=5))
    with pytest.raises(ValueError):
        await c.set_lot_priority("a b", PriorityRequest(priority=5))
    assert len(env.exchange.sent) == 1

    for bad in (0, 100, 50.5, "50", True):
        with pytest.raises(ValidationError):
            PriorityRequest(priority=bad)
    with pytest.raises(ValidationError):
        PriorityRequest(priority=5, extra=1)

    c.comparison_active = True
    with pytest.raises(ConflictError, match="Locked while a comparison is running"):
        await c.set_lot_priority("o-1", PriorityRequest(priority=5))


async def test_scrap_lot_publishes_and_maps_errors(env):
    c = env.controller
    with pytest.raises(ConflictError):  # no active run
        await c.scrap_lot("o-1", ScrapRequest(reason="operator"))
    await c.start(_start())
    env.lots.update(
        {
            "o-1": "queued",
            "o-2": "banked",
            "o-3": "running",
            "o-4": "done",
            "o-5": "failed",
            "o-6": "pre_release",
        }
    )
    env.exchange.sent.clear()

    for oid in ("o-1", "o-2", "o-3"):
        assert await c.scrap_lot(oid, ScrapRequest(reason="quality")) == {"accepted": True}
    assert env.exchange.keys() == ["scheduler.scrap"] * 3
    key, cmd = env.exchange.sent[0]
    assert isinstance(cmd, LotScrap) and cmd.reason == "quality"
    run_id = c.run_info()["run_id"]
    assert (cmd.order_id, cmd.machine_id, cmd.run_id) == ("o-1", "scheduler", run_id)

    with pytest.raises(UnknownLotError):
        await c.scrap_lot("o-9", ScrapRequest(reason="other"))
    for oid in ("o-4", "o-5", "o-6"):
        with pytest.raises(ConflictError):
            await c.scrap_lot(oid, ScrapRequest(reason="other"))
    with pytest.raises(ValueError):
        await c.scrap_lot("a b", ScrapRequest(reason="other"))
    assert len(env.exchange.sent) == 3

    for bad in ("capacity", "", "bogus"):
        with pytest.raises(ValidationError):
            ScrapRequest(reason=bad)
    with pytest.raises(ValidationError):
        ScrapRequest(reason="other", extra=1)
