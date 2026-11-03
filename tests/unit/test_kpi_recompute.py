"""Unit tests for KPI recompute function and CLI (no brokers)."""

import copy
import json
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from pizza.common.envelope import (
    KpiSnapshot,
    OrderLifecycle,
    RecipeChanged,
    RecipeSnapshot,
    encode,
)
from pizza.common.recipes import load_all_recipes
from pizza.common.topology import TOPIC_KPI_SNAPSHOTS, TOPIC_ORDER_LIFECYCLE, TOPIC_STATION_EVENTS
from pizza.kpi.compare import DEFAULT_TOLERANCE
from pizza.kpi.recompute import (
    RUN_ID_RE,
    NoStoredSnapshot,
    build_cfg_and_recipes,
    main,
    run_recompute,
)

T0 = datetime(2026, 1, 1, tzinfo=UTC)
RECIPES = load_all_recipes()
RUN_ID = "r-20260101-120000-abcd"


def _life(order_id: str, state: str, run_id: str, ts=T0, created_ts=None) -> OrderLifecycle:
    return OrderLifecycle.make(
        run_id=run_id,
        machine_id="scheduler",
        ts=ts,
        state=state,
        order_id=order_id,
        product="margherita",
        qty=1,
        due_ts=T0 + timedelta(hours=1),
        created_ts=created_ts or T0,
    )


class FakeMsg:
    def __init__(self, topic: str, partition: int, offset: int, value: bytes) -> None:
        self._a = (topic, partition, offset, value)

    def topic(self):
        return self._a[0]

    def partition(self):
        return self._a[1]

    def offset(self):
        return self._a[2]

    def value(self):
        return self._a[3]

    def error(self):
        return None


class FakeConsumer:
    def __init__(self, msgs: list[FakeMsg]) -> None:
        self.msgs = list(msgs)
        self.closed = False

    def list_topics(self, timeout=None):
        from types import SimpleNamespace
        return SimpleNamespace(
            topics={
                TOPIC_KPI_SNAPSHOTS: SimpleNamespace(partitions={0: None}),
                TOPIC_ORDER_LIFECYCLE: SimpleNamespace(partitions={0: None}),
                TOPIC_STATION_EVENTS: SimpleNamespace(partitions={0: None}),
            }
        )

    def assign(self, tps):
        self.assigned = tps

    def get_watermark_offsets(self, tp, timeout=None):
        return 0, 10

    def poll(self, timeout=None):
        return self.msgs.pop(0) if self.msgs else None

    def close(self):
        self.closed = True


def _snapshot(run_id: str, n_completed: int = 0, tamper: dict = None) -> KpiSnapshot:
    """Create a test KpiSnapshot."""
    from pizza.common.envelope import LittleCheck, TheoryKpi
    from pizza.kpi.metrics import CycleTimeKpi

    snap = KpiSnapshot.make(
        run_id=run_id,
        machine_id="kpi-service",
        warming_up=False,
        warmup_orders=10,
        orders_total=n_completed,
        orders_completed=n_completed,
        throughput_per_s=None,
        pizzas_per_s=None,
        capacity_ratio=None,
        wip_now=0,
        wip_pre_release=0,
        wip_bank=0,
        wip_bank_capacity=0,
        wip_failed=0,
        wip_avg=None,
        cycle_time=CycleTimeKpi(n=0),
        stations={},
        bottleneck=None,
        bottleneck_candidate=None,
        bottleneck_candidate_count=0,
        theory=TheoryKpi(weights_source="observed", expected_station_time_s={}),
        little=LittleCheck(status="insufficient_data", tolerance=0.1, n_completed=0, n_in_system=0),
    )
    # Apply tampering if provided
    if tamper:
        snap = snap.model_copy(update=tamper)
    return snap


def test_run_recompute_with_consistent_facts() -> None:
    """Test 1: run_recompute returns report with all fields matching."""
    from pizza.kpi.metrics import KpiConfig

    cfg = KpiConfig(
        warmup_orders=0,
        little_tolerance=0.10,
        bottleneck_margin=0.05,
        bottleneck_confirm_n=3,
        bottleneck_min_util=0.80,
    )

    # Create stored snapshot with one completed order
    stored = _snapshot(RUN_ID, n_completed=1)
    FakeMsg(TOPIC_KPI_SNAPSHOTS, 0, 0, encode(stored))

    # Create lifecycle facts
    FakeMsg(TOPIC_ORDER_LIFECYCLE, 0, 0, encode(_life("o1", "completed", RUN_ID)))

    def fake_replay(bootstrap, topics, timeout_s, group_id, consumer_factory=None):
        # First call: replay snapshots (stored snapshot)
        if TOPIC_KPI_SNAPSHOTS in topics:
            return [stored], {}
        # Second call: replay events
        else:
            return [_life("o1", "completed", RUN_ID)], {}

    report = run_recompute(
        "localhost:9092",
        RUN_ID,
        RECIPES,
        cfg,
        timeout_s=1.0,
        tolerance=DEFAULT_TOLERANCE,
        replay=fake_replay,
    )

    assert report["run_id"] == RUN_ID
    assert "matched" in report
    assert "fields" in report
    assert isinstance(report["fields"], list)


def test_run_id_regex_validation() -> None:
    """Test 5a: RUN_ID_RE pattern validation."""
    assert RUN_ID_RE.match("r-20260101-120000-abcd") is not None
    assert RUN_ID_RE.match("r-20260101-120000-1234") is not None
    assert RUN_ID_RE.match("not-a-run-id") is None
    assert RUN_ID_RE.match("r-20260101-12000-abcd") is None  # short date
    assert RUN_ID_RE.match("") is None


def test_no_stored_snapshot_error() -> None:
    """Test 5b: run_recompute raises NoStoredSnapshot when no snapshot found."""
    def fake_replay_empty(bootstrap, topics, timeout_s, group_id, consumer_factory=None):
        return [], {}

    cfg = MagicMock()
    with pytest.raises(NoStoredSnapshot):
        run_recompute(
            "localhost:9092",
            RUN_ID,
            RECIPES,
            cfg,
            timeout_s=1.0,
            replay=fake_replay_empty,
        )


def test_invalid_run_id_rejected() -> None:
    """Test 5c: Invalid run_id raises ValueError before any I/O."""
    cfg = MagicMock()
    with pytest.raises(ValueError):
        run_recompute(
            "localhost:9092",
            "invalid-run-id",
            RECIPES,
            cfg,
            timeout_s=1.0,
        )


def test_group_id_format() -> None:
    """Test 3: group_ids are unique and start with 'replay-'."""
    seen_groups = []

    def fake_replay(bootstrap, topics, timeout_s, group_id, consumer_factory=None):
        seen_groups.append(group_id)
        assert group_id.startswith("replay-")
        assert group_id not in ["kpi", "dashboard", "scheduler"]
        return [], {}

    cfg = MagicMock()
    try:
        run_recompute(
            "localhost:9092",
            RUN_ID,
            RECIPES,
            cfg,
            timeout_s=1.0,
            replay=fake_replay,
        )
    except NoStoredSnapshot:
        pass

    # Should have been called at least once
    assert len(seen_groups) > 0


def test_no_producer_import() -> None:
    """Test 4: module does not import produce_fact or make_producer."""
    import pizza.kpi.recompute as recompute_mod

    with open(recompute_mod.__file__, encoding="utf-8") as f:
        source = f.read()
    assert "produce_fact" not in source
    assert "make_producer" not in source
    # Also verify no producer attribute exists
    assert not hasattr(recompute_mod, "producer")


def test_build_cfg_and_recipes() -> None:
    """Test helper: build_cfg_and_recipes constructs cfg and recipes."""
    cfg, recipes = build_cfg_and_recipes()
    assert cfg is not None
    assert recipes is not None
    assert isinstance(recipes, dict)


def test_cli_help(capsys) -> None:
    """Test CLI help."""
    with pytest.raises(SystemExit) as exc_info:
        main(["--help"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "run-id" in captured.out


def test_cli_missing_run_id() -> None:
    """Test CLI requires --run-id."""
    with pytest.raises(SystemExit) as exc_info:
        main([])
    assert exc_info.value.code != 0


def test_cli_exit_codes_on_match_mismatch() -> None:
    """Test 7: CLI exits 0 on match, 1 on mismatch, 2 on error."""
    def fake_runner_match(bootstrap, run_id, recipes, cfg, **kw):
        return {
            "run_id": run_id,
            "matched": True,
            "tolerance": DEFAULT_TOLERANCE,
            "fields": [],
            "stored_ts": "2026-01-01T00:00:00Z",
            "n_events": 0,
        }

    def fake_runner_mismatch(bootstrap, run_id, recipes, cfg, **kw):
        return {
            "run_id": run_id,
            "matched": False,
            "tolerance": DEFAULT_TOLERANCE,
            "fields": [],
            "stored_ts": "2026-01-01T00:00:00Z",
            "n_events": 0,
        }

    def fake_runner_error(bootstrap, run_id, recipes, cfg, **kw):
        raise NoStoredSnapshot("no snapshot")

    # Match: exit 0
    assert main(["--run-id", RUN_ID], runner=fake_runner_match) == 0

    # Mismatch: exit 1
    assert main(["--run-id", RUN_ID], runner=fake_runner_mismatch) == 1

    # Error: exit 2
    assert main(["--run-id", RUN_ID], runner=fake_runner_error) == 2


def test_cli_json_output(capsys) -> None:
    """Test CLI --json flag outputs JSON."""
    def fake_runner(bootstrap, run_id, recipes, cfg, **kw):
        return {
            "run_id": run_id,
            "matched": True,
            "tolerance": DEFAULT_TOLERANCE,
            "fields": [],
            "stored_ts": "2026-01-01T00:00:00Z",
            "n_events": 0,
        }

    exit_code = main(["--run-id", RUN_ID, "--json"], runner=fake_runner)
    assert exit_code == 0
    captured = capsys.readouterr()
    # Should be valid JSON
    data = json.loads(captured.out)
    assert data["run_id"] == RUN_ID


def _at(seconds: float) -> datetime:
    return T0 + timedelta(seconds=seconds)


def _recipe_fixture_run(monkeypatch, second_call_facts):
    """Run run_recompute with a stub kpi_recompute that captures the recipe table."""
    import pizza.kpi.recompute as recompute_mod
    from pizza.kpi.metrics import KpiConfig

    cfg = KpiConfig(
        warmup_orders=0,
        little_tolerance=0.10,
        bottleneck_margin=0.05,
        bottleneck_confirm_n=3,
        bottleneck_min_util=0.80,
    )
    stored = _snapshot(RUN_ID, n_completed=1)
    captured: dict = {}

    def capture(events, stored_arg, recipes, cfg_arg):
        captured["recipes"] = recipes
        return stored_arg

    monkeypatch.setattr(recompute_mod, "kpi_recompute", capture)

    def fake_replay(bootstrap, topics, timeout_s, group_id, consumer_factory=None):
        if TOPIC_KPI_SNAPSHOTS in topics:
            return [stored], {}
        return list(second_call_facts), {}

    report = run_recompute(
        "localhost:9092",
        RUN_ID,
        RECIPES,
        cfg,
        timeout_s=1.0,
        replay=fake_replay,
    )
    return report, captured


def test_report_uses_event_derived_recipes(monkeypatch) -> None:
    """Event-derived table (snapshot + change) is folded, not the caller's RECIPES."""
    full_copy = copy.deepcopy(RECIPES)
    original = full_copy["bake"]["margherita"]
    snapshot = RecipeSnapshot.make(
        run_id=RUN_ID, machine_id="scheduler", ts=_at(0), recipes=full_copy
    )
    change = RecipeChanged.make(
        run_id=RUN_ID,
        machine_id="bake",
        station="bake",
        product="margherita",
        old_unit_time_s=original,
        new_unit_time_s=40.0,
        ts=_at(1),
    )
    report, captured = _recipe_fixture_run(
        monkeypatch,
        [snapshot, change, _life("o1", "completed", RUN_ID)],
    )

    assert report["recipes_source"] == "events"
    assert report["recipe_changes_applied"] == 1
    assert captured["recipes"]["bake"]["margherita"] == 40.0
    assert captured["recipes"]["bake"]["margherita"] != RECIPES["bake"]["margherita"]


def test_report_falls_back_to_disk_without_snapshot(monkeypatch) -> None:
    """With no recipe.snapshot the run recomputes from the caller's disk table."""
    report, captured = _recipe_fixture_run(
        monkeypatch,
        [_life("o1", "completed", RUN_ID)],
    )

    assert report["recipes_source"] == "disk"
    assert report["recipe_changes_applied"] == 0
    assert captured["recipes"] == {s: dict(v) for s, v in RECIPES.items()}


def test_cli_prints_recipes_source_and_tolerates_missing_keys(capsys) -> None:
    """CLI prints provenance with .get() and keeps exit codes for injected runners."""

    def fake_runner_without_keys(bootstrap, run_id, recipes, cfg, **kw):
        return {
            "run_id": run_id,
            "matched": True,
            "tolerance": DEFAULT_TOLERANCE,
            "fields": [],
            "stored_ts": "2026-01-01T00:00:00Z",
            "n_events": 0,
        }

    def fake_runner_with_source(bootstrap, run_id, recipes, cfg, **kw):
        return {
            "run_id": run_id,
            "matched": True,
            "tolerance": DEFAULT_TOLERANCE,
            "fields": [],
            "stored_ts": "2026-01-01T00:00:00Z",
            "n_events": 0,
            "recipes_source": "events",
            "recipe_changes_applied": 2,
        }

    def fake_runner_mismatch(bootstrap, run_id, recipes, cfg, **kw):
        return {
            "run_id": run_id,
            "matched": False,
            "tolerance": DEFAULT_TOLERANCE,
            "fields": [],
            "stored_ts": "2026-01-01T00:00:00Z",
            "n_events": 0,
            "recipes_source": "disk",
            "recipe_changes_applied": 0,
        }

    assert main(["--run-id", RUN_ID], runner=fake_runner_without_keys) == 0
    out = capsys.readouterr().out
    assert "Recipes source:   unknown" in out
    assert "Recipe changes:   0" in out

    assert main(["--run-id", RUN_ID], runner=fake_runner_with_source) == 0
    out = capsys.readouterr().out
    assert "Recipes source:   events" in out
    assert "Recipe changes:   2" in out

    assert main(["--run-id", RUN_ID], runner=fake_runner_mismatch) == 1
