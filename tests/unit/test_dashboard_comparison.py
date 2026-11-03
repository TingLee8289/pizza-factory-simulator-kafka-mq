"""Unit tests for the dashboard comparison runner (fake exchange, no brokers)."""

import re
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from pizza.common.envelope import ComparisonResult, RunStarted, parse_message
from pizza.dashboard.comparison import (
    ComparisonRunner,
    ComparisonStartRequest,
    comparison_csv,
    comparison_rows,
)
from pizza.dashboard.control import ConflictError, RunController, StartRequest


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

        self.controller = RunController(SimpleNamespace(), get_exchange, purge, on_change=on_change)
        self.runner = ComparisonRunner(self.controller, on_change=on_change)


@pytest.fixture
def env() -> Env:
    return Env()


# -- ComparisonStartRequest validation ----------------------------------------


def test_comparison_start_request_bounds():
    """Task 2, Test 2: ComparisonStartRequest validates all fields."""
    # Valid bounds
    ComparisonStartRequest(n_orders=5)
    ComparisonStartRequest(n_orders=99)
    ComparisonStartRequest(arrival_rate_per_min=0.1)
    ComparisonStartRequest(arrival_rate_per_min=12.0)
    ComparisonStartRequest(k=1.0)
    ComparisonStartRequest(k=10.0)
    ComparisonStartRequest(seed=0)
    ComparisonStartRequest(seed=2**31 - 1)
    ComparisonStartRequest(weights={"margherita": 1})
    ComparisonStartRequest(random_breakdowns=True)

    # Invalid bounds
    with pytest.raises(ValidationError):
        ComparisonStartRequest(n_orders=4)
    with pytest.raises(ValidationError):
        ComparisonStartRequest(n_orders=100)
    with pytest.raises(ValidationError):
        ComparisonStartRequest(arrival_rate_per_min=0)
    with pytest.raises(ValidationError):
        ComparisonStartRequest(arrival_rate_per_min=12.1)
    with pytest.raises(ValidationError):
        ComparisonStartRequest(k=0.9)
    with pytest.raises(ValidationError):
        ComparisonStartRequest(seed=-1)
    with pytest.raises(ValidationError):
        ComparisonStartRequest(seed=2**31)
    with pytest.raises(ValidationError):
        ComparisonStartRequest(extra_field=1)


# -- ComparisonRunner start --------------------------------------------------


async def test_start_creates_comparison_and_starts_fifo_round_1(env):
    """Task 2, Test 1: start() mints comparison_id, seed, sets state to round_running."""
    req = ComparisonStartRequest(n_orders=30, k=1.5)
    await env.runner.start(req)

    status = env.runner.status()
    assert status["state"] == "round_running"
    assert status["round"] == 1
    assert re.match(r"^c-[0-9a-z-]{1,36}$", status["comparison_id"])
    assert isinstance(status["n_orders"], int)
    assert status["n_orders"] == 30
    assert status["k"] == 1.5
    assert isinstance(status["seed"], int)
    assert 0 <= status["seed"] < 2**31
    assert status["rule"] == "fifo"
    assert env.controller.comparison_active is True

    # Verify that RunStarted was sent with fifo rule
    started = [m for _, m in env.exchange.sent if isinstance(m, RunStarted)]
    assert started and all(m.params.rule == "fifo" for m in started)


async def test_start_mints_seed_when_not_provided(env):
    """Task 2, Test 1: start() mints a random seed when not provided."""
    req = ComparisonStartRequest(n_orders=30)
    await env.runner.start(req)

    status = env.runner.status()
    assert isinstance(status["seed"], int)
    assert 0 <= status["seed"] < 2**31


async def test_start_uses_provided_seed(env):
    """Task 2, Test 1: start() uses provided seed."""
    req = ComparisonStartRequest(n_orders=30, seed=12345)
    await env.runner.start(req)

    status = env.runner.status()
    assert status["seed"] == 12345


async def test_start_while_comparison_active_conflicts(env):
    """Task 2, Test 7: start() while a comparison is active raises ConflictError."""
    req = ComparisonStartRequest(n_orders=30)
    await env.runner.start(req)

    with pytest.raises(ConflictError):
        await env.runner.start(req)


async def test_start_with_active_run_conflicts(env):
    """Task 2, Test 7: start() with a non-comparison run active raises ConflictError."""
    await env.controller.start(StartRequest(seed=7))

    req = ComparisonStartRequest(n_orders=30)
    with pytest.raises(ConflictError):
        await env.runner.start(req)


# -- ComparisonRunner on_progress ------------------------------------------


async def test_on_progress_no_close_until_all_complete(env):
    """Task 2, Test 3: on_progress doesn't publish RoundClose until completed+failed >= N."""
    req = ComparisonStartRequest(n_orders=30)
    await env.runner.start(req)
    run_id = env.controller.run_info()["run_id"]
    env.exchange.sent.clear()

    await env.runner.on_progress(run_id, completed=29, failed=0)

    # Should not publish any RoundClose yet
    assert "kpi.close" not in env.exchange.keys()


async def test_on_progress_publishes_round_close_when_done(env):
    """Task 2, Test 3: on_progress publishes RoundClose when completed+failed >= N."""
    from pizza.common.envelope import RoundClose

    req = ComparisonStartRequest(n_orders=30)
    await env.runner.start(req)
    run_id = env.controller.run_info()["run_id"]
    env.exchange.sent.clear()

    await env.runner.on_progress(run_id, completed=29, failed=1)

    # Should publish exactly one RoundClose
    assert env.exchange.keys() == ["kpi.close"]
    cmd = env.exchange.sent[0][1]
    assert isinstance(cmd, RoundClose)
    assert cmd.partial is False


async def test_on_progress_idempotent_after_close(env):
    """Task 2, Test 3: on_progress doesn't republish RoundClose on second call."""
    req = ComparisonStartRequest(n_orders=30)
    await env.runner.start(req)
    run_id = env.controller.run_info()["run_id"]
    env.exchange.sent.clear()

    await env.runner.on_progress(run_id, completed=29, failed=1)
    assert len([k for k in env.exchange.keys() if k == "kpi.close"]) == 1

    env.exchange.sent.clear()
    await env.runner.on_progress(run_id, completed=30, failed=0)
    assert "kpi.close" not in env.exchange.keys()


async def test_on_progress_ignores_different_run_id(env):
    """Task 2, Test 3: on_progress ignores events for a different run_id."""
    req = ComparisonStartRequest(n_orders=30)
    await env.runner.start(req)
    env.exchange.sent.clear()

    await env.runner.on_progress("different-run-id", completed=30, failed=0)

    # Should not publish any RoundClose
    assert "kpi.close" not in env.exchange.keys()


# -- ComparisonRunner on_result ------------------------------------------


async def test_on_result_transitions_to_round_done(env):
    """Task 2, Test 4: on_result receives ComparisonResult and transitions to round_done."""
    req = ComparisonStartRequest(n_orders=30)
    await env.runner.start(req)
    status_before = env.runner.status()

    result = ComparisonResult.make(
        run_id="r-x",
        machine_id="kpi",
        comparison_id=status_before["comparison_id"],
        round=1,
        rule="fifo",
        seed=status_before["seed"],
        n_orders=30,
        n_completed=30,
        n_failed=0,
        late_count=2,
        mean_cycle_time_s=10.0,
        mean_tardiness_s=0.5,
        max_tardiness_s=5.0,
        pct_late=0.1,
        makespan_s=100.0,
        partial=False,
        k=1.5,
        arrival_rate_per_min=2.0,
        random_breakdowns=True,
    )

    await env.runner.on_result(result)

    status = env.runner.status()
    assert status["state"] == "round_done"


async def test_on_result_stores_results(env):
    """Task 2, Test 4: on_result stores ComparisonResult in results."""
    req = ComparisonStartRequest(n_orders=30)
    await env.runner.start(req)
    status_before = env.runner.status()

    result = ComparisonResult.make(
        run_id="r-x",
        machine_id="kpi",
        comparison_id=status_before["comparison_id"],
        round=1,
        rule="fifo",
        seed=status_before["seed"],
        n_orders=30,
        n_completed=28,
        n_failed=2,
        late_count=5,
        mean_cycle_time_s=10.0,
        mean_tardiness_s=0.5,
        max_tardiness_s=5.0,
        pct_late=0.1,
        makespan_s=100.0,
        partial=False,
        k=1.5,
        arrival_rate_per_min=2.0,
        random_breakdowns=True,
    )

    await env.runner.on_result(result)

    # Results should be stored
    status = env.runner.status()
    assert len(status["results"]) == 1
    row = status["results"][0]
    assert row["rule"] == "fifo"
    assert row["n_orders"] == 30


async def test_on_result_after_round_3_finishes_comparison(env):
    """Task 2, Test 4: on_result after round 3 transitions to finished."""
    req = ComparisonStartRequest(n_orders=30)
    await env.runner.start(req)
    comparison_id = env.runner.status()["comparison_id"]
    seed = env.runner.status()["seed"]

    # Complete rounds 1, 2, 3
    for round_num, rule in [(1, "fifo"), (2, "edd"), (3, "spt")]:
        # Wait for round to be running
        assert env.runner.status()["round"] == round_num

        result = ComparisonResult.make(
            run_id=f"r-round{round_num}",
            machine_id="kpi",
            comparison_id=comparison_id,
            round=round_num,
            rule=rule,
            seed=seed,
            n_orders=30,
            n_completed=30,
            n_failed=0,
            late_count=0,
            mean_cycle_time_s=10.0,
            mean_tardiness_s=0.0,
            max_tardiness_s=0.0,
            pct_late=0.0,
            makespan_s=100.0,
            partial=False,
            k=1.5,
            arrival_rate_per_min=2.0,
            random_breakdowns=True,
        )
        await env.runner.on_result(result)

        if round_num < 3:
            # After rounds 1 and 2, state should be round_done and comparison_active still true
            assert env.runner.status()["state"] == "round_done"
            assert env.controller.comparison_active is True
            # Advance to next round
            await env.runner.next_round()

    # After round 3, comparison should be finished and comparison_active cleared
    assert env.runner.status()["state"] == "finished"
    assert env.controller.comparison_active is False


# -- ComparisonRunner next_round ------------------------------------------


async def test_next_round_blocked_until_result_received(env):
    """Task 2, Test 4: next_round raises ConflictError until result received."""
    req = ComparisonStartRequest(n_orders=30)
    await env.runner.start(req)

    with pytest.raises(ConflictError):
        await env.runner.next_round()


async def test_next_round_starts_round_2_with_edd(env):
    """Task 2, Test 4: next_round resets and starts round 2 with EDD rule."""

    req = ComparisonStartRequest(n_orders=30, seed=42)
    await env.runner.start(req)
    comparison_id = env.runner.status()["comparison_id"]
    seed = env.runner.status()["seed"]

    # Complete round 1
    result = ComparisonResult.make(
        run_id="r-1",
        machine_id="kpi",
        comparison_id=comparison_id,
        round=1,
        rule="fifo",
        seed=seed,
        n_orders=30,
        n_completed=30,
        n_failed=0,
        late_count=0,
        mean_cycle_time_s=10.0,
        mean_tardiness_s=0.0,
        max_tardiness_s=0.0,
        pct_late=0.0,
        makespan_s=100.0,
        partial=False,
        k=1.5,
        arrival_rate_per_min=2.0,
        random_breakdowns=True,
    )
    await env.runner.on_result(result)

    # Clear sent messages before next_round
    env.exchange.sent.clear()

    # Advance to next round
    await env.runner.next_round()

    # Check state
    status = env.runner.status()
    assert status["round"] == 2
    assert status["state"] == "round_running"
    assert status["rule"] == "edd"
    # Seed should be the same
    assert status["seed"] == seed
    # Same n_orders and k
    assert status["n_orders"] == 30
    assert status["k"] == 1.5

    # Check that RunStarted for EDD was sent
    started = [m for _, m in env.exchange.sent if isinstance(m, RunStarted)]
    assert any(m.params.rule == "edd" for m in started if m.params)


async def test_next_round_round_3_uses_spt(env):
    """Task 2, Test 4: next_round for round 3 uses SPT rule."""
    req = ComparisonStartRequest(n_orders=30, seed=42)
    await env.runner.start(req)
    comparison_id = env.runner.status()["comparison_id"]
    seed = env.runner.status()["seed"]

    # Complete rounds 1 and 2
    for round_num in [1, 2]:
        result = ComparisonResult.make(
            run_id=f"r-{round_num}",
            machine_id="kpi",
            comparison_id=comparison_id,
            round=round_num,
            rule=["fifo", "edd"][round_num - 1],
            seed=seed,
            n_orders=30,
            n_completed=30,
            n_failed=0,
            late_count=0,
            mean_cycle_time_s=10.0,
            mean_tardiness_s=0.0,
            max_tardiness_s=0.0,
            pct_late=0.0,
            makespan_s=100.0,
            partial=False,
            k=1.5,
            arrival_rate_per_min=2.0,
            random_breakdowns=True,
        )
        await env.runner.on_result(result)
        if round_num < 2:
            await env.runner.next_round()

    env.exchange.sent.clear()
    await env.runner.next_round()

    status = env.runner.status()
    assert status["round"] == 3
    assert status["rule"] == "spt"


async def test_next_round_after_round_3_conflicts(env):
    """Task 2, Test 4: next_round after round 3 raises ConflictError."""
    req = ComparisonStartRequest(n_orders=30)
    await env.runner.start(req)
    comparison_id = env.runner.status()["comparison_id"]
    seed = env.runner.status()["seed"]

    # Complete all 3 rounds
    for round_num in [1, 2, 3]:
        result = ComparisonResult.make(
            run_id=f"r-{round_num}",
            machine_id="kpi",
            comparison_id=comparison_id,
            round=round_num,
            rule=["fifo", "edd", "spt"][round_num - 1],
            seed=seed,
            n_orders=30,
            n_completed=30,
            n_failed=0,
            late_count=0,
            mean_cycle_time_s=10.0,
            mean_tardiness_s=0.0,
            max_tardiness_s=0.0,
            pct_late=0.0,
            makespan_s=100.0,
            partial=False,
            k=1.5,
            arrival_rate_per_min=2.0,
            random_breakdowns=True,
        )
        await env.runner.on_result(result)
        if round_num < 3:
            await env.runner.next_round()

    # After round 3, next_round should conflict
    with pytest.raises(ConflictError):
        await env.runner.next_round()


# -- ComparisonRunner end_round ------------------------------------------


async def test_end_round_while_running(env):
    """Task 2, Test 5: end_round calls stop_generation and send_round_close with partial=True."""
    from pizza.common.envelope import RoundClose

    req = ComparisonStartRequest(n_orders=30)
    await env.runner.start(req)
    env.exchange.sent.clear()

    await env.runner.end_round()

    # Should send generator.reset and kpi.close
    keys = env.exchange.keys()
    assert "generator.reset" in keys
    assert "kpi.close" in keys

    # The RoundClose should have partial=True
    close_msgs = [m for k, m in zip(keys, [m for _, m in env.exchange.sent]) if k == "kpi.close"]
    assert close_msgs and isinstance(close_msgs[0], RoundClose)
    assert close_msgs[0].partial is True


async def test_end_round_when_not_running_conflicts(env):
    """Task 2, Test 5: end_round when not running raises ConflictError."""
    with pytest.raises(ConflictError):
        await env.runner.end_round()


# -- ComparisonRunner cancel -----------------------------------------------


async def test_cancel_clears_active_and_resets_if_needed(env):
    """Task 2, Test 6: cancel clears comparison_active and optionally resets."""
    req = ComparisonStartRequest(n_orders=30)
    await env.runner.start(req)
    assert env.controller.comparison_active is True

    await env.runner.cancel()

    assert env.runner.status()["state"] == "cancelled"
    assert env.controller.comparison_active is False


async def test_cancel_keeps_results(env):
    """Task 2, Test 6: cancel keeps recorded results."""
    req = ComparisonStartRequest(n_orders=30)
    await env.runner.start(req)
    comparison_id = env.runner.status()["comparison_id"]
    seed = env.runner.status()["seed"]

    # Record a result
    result = ComparisonResult.make(
        run_id="r-1",
        machine_id="kpi",
        comparison_id=comparison_id,
        round=1,
        rule="fifo",
        seed=seed,
        n_orders=30,
        n_completed=30,
        n_failed=0,
        late_count=0,
        mean_cycle_time_s=10.0,
        mean_tardiness_s=0.0,
        max_tardiness_s=0.0,
        pct_late=0.0,
        makespan_s=100.0,
        partial=False,
        k=1.5,
        arrival_rate_per_min=2.0,
        random_breakdowns=True,
    )
    await env.runner.on_result(result)

    await env.runner.cancel()

    status = env.runner.status()
    assert len(status["results"]) == 1


# -- ComparisonRunner error handling ----------------------------------------


async def test_start_error_sets_error_state_and_clears_active(env):
    """Task 2, Test 8: error during start sets state to error and clears comparison_active."""
    # Simulate a broker error
    env.exchange.fail_at = 2

    req = ComparisonStartRequest(n_orders=30)
    with pytest.raises(RuntimeError, match="could not start round"):
        await env.runner.start(req)

    status = env.runner.status()
    assert status["state"] == "error"
    assert env.controller.comparison_active is False


async def test_error_state_cleared_on_cancel(env):
    """Task 2, Test 8: cancel() can be called to clear error state."""
    env.exchange.fail_at = 2

    req = ComparisonStartRequest(n_orders=30)
    with pytest.raises(RuntimeError):
        await env.runner.start(req)

    await env.runner.cancel()

    status = env.runner.status()
    assert status["state"] == "cancelled"


# -- Pure helpers: comparison_rows and comparison_csv -------------------------


def test_comparison_rows_empty():
    """Task 3, Test 1: comparison_rows returns empty list when no results."""
    rows = comparison_rows({})
    assert rows == []


def test_comparison_rows_single_result():
    """Task 3, Test 1: comparison_rows returns rows with expected columns."""
    results = {
        ("c-1", 1): ComparisonResult.make(
            run_id="r-1",
            machine_id="kpi",
            comparison_id="c-1",
            round=1,
            rule="fifo",
            seed=42,
            n_orders=30,
            n_completed=28,
            n_failed=2,
            late_count=5,
            mean_cycle_time_s=10.5,
            mean_tardiness_s=1.2,
            max_tardiness_s=5.3,
            pct_late=0.18,
            makespan_s=105.0,
            partial=False,
            k=1.5,
            arrival_rate_per_min=2.0,
            random_breakdowns=True,
        )
    }

    rows = comparison_rows(results)

    assert len(rows) == 1
    row = rows[0]
    assert row["rule"] == "fifo"
    assert row["run_id"] == "r-1"
    assert row["seed"] == 42
    assert row["n_orders"] == 30
    assert row["mean_cycle_time"] == 10.5
    assert row["mean_tardiness"] == 1.2
    assert row["pct_late"] == 0.18
    assert row["makespan"] == 105.0
    assert row["partial"] is False
    assert row["n_failed"] == 2
    assert row["max_tardiness"] == 5.3


def test_comparison_csv_injection_protection():
    """Task 3, Test 4: CSV cells starting with = + - @ are quoted."""
    rows = [
        {
            "rule": "=MALICIOUS",
            "run_id": "+FORMULA",
            "seed": 42,
            "n_orders": 30,
            "mean_cycle_time": 10.0,
            "mean_tardiness": 0.0,
            "pct_late": 0.0,
            "makespan": 100.0,
        }
    ]

    csv = comparison_csv(rows)

    # Check that no CSV cell starts with dangerous characters
    lines = csv.strip().split("\n")
    for line in lines[1:]:  # Skip header
        cells = line.split(",")
        for cell in cells:
            # Remove quotes if present
            unquoted = cell.lstrip("'")
            assert not re.match(r"^[=+\-@]", unquoted)
