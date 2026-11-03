"""Schema tests for the Phase 4 KpiSnapshot fact."""

import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from pizza.common.envelope import (
    CycleTimeKpi,
    Fact,
    InvalidEnvelope,
    KpiSnapshot,
    LittleCheck,
    StationKpi,
    TheoryKpi,
    encode,
    parse_message,
)

TS = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def _full(**over) -> KpiSnapshot:
    kwargs = dict(
        warming_up=False,
        warmup_orders=5,
        window_start_ts=TS,
        window_s=120.5,
        orders_total=30,
        orders_completed=20,
        throughput_per_s=0.03,
        pizzas_per_s=0.06,
        capacity_ratio=0.9,
        wip_now=10,
        wip_pizzas=20,
        wip_pre_release=2,
        wip_avg=4.2,
        cycle_time=CycleTimeKpi(n=20, mean_s=100.0, p50_s=95.0, p95_s=150.0,
            per_pizza_mean_s=50.0, per_pizza_p50_s=48.0, per_pizza_p95_s=75.0),
        stations={
            "bake": StationKpi(
                utilization=0.8,
                wip_wait=1,
                wip_track_in=0,
                wip_run=1,
                wait_mean_s=1.0,
                wait_p95_s=2.0,
                track_in_mean_s=0.5,
                track_in_p95_s=0.9,
                queue_mean_s=1.5,
                queue_p95_s=2.9,
                queue_samples=20,
            )
        },
        bottleneck="bake",
        bottleneck_candidate="bake",
        bottleneck_candidate_count=3,
        theory=TheoryKpi(
            weights_source="run_started",
            expected_station_time_s={"bake": 30.0},
            bottleneck_station="bake",
            capacity_per_s=0.033,
            raw_process_time_s=100.0,
            critical_wip=3.3,
        ),
        little=LittleCheck(
            status="pass",
            l=4.2,
            lambda_per_s=0.03,
            w_s=100.0,
            lambda_w=3.0,
            rel_error=0.02,
            tolerance=0.1,
            n_completed=20,
            n_in_system=10,
        ),
        negative_durations=0,
    )
    kwargs.update(over)
    return KpiSnapshot.make(run_id="r1", machine_id="kpi-service", ts=TS, **kwargs)


def _warming() -> KpiSnapshot:
    return KpiSnapshot.make(
        run_id="r1",
        machine_id="kpi-service",
        ts=TS,
        warming_up=True,
        warmup_orders=5,
        orders_total=0,
        orders_completed=0,
        wip_now=0,
        wip_pre_release=0,
        cycle_time=CycleTimeKpi(n=0),
        stations={},
        theory=TheoryKpi(weights_source="uniform", expected_station_time_s={}),
        little=LittleCheck(status="insufficient_data", tolerance=0.1, n_completed=0, n_in_system=0),
    )


def test_full_snapshot_round_trip():
    snap = _full()
    parsed = parse_message(encode(snap))
    assert isinstance(parsed, KpiSnapshot)
    assert parsed.type == "kpi.snapshot"
    assert parsed == snap


def test_warming_up_snapshot_round_trip():
    snap = _warming()
    parsed = parse_message(encode(snap))
    assert parsed == snap
    assert parsed.throughput_per_s is None
    assert parsed.window_start_ts is None
    assert json.loads(encode(snap))["window_start_ts"] is None


def test_is_fact_and_schema_version():
    assert issubclass(KpiSnapshot, Fact)
    assert _full().schema_version == 1


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_nan_inf_rejected_in_each_float_field(bad):
    with pytest.raises(ValidationError):
        _full(throughput_per_s=bad)
    with pytest.raises(ValidationError):
        _full(wip_avg=bad)
    with pytest.raises(ValidationError):
        _full(cycle_time=CycleTimeKpi(n=1, mean_s=bad))
    with pytest.raises(ValidationError):
        StationKpi(utilization=bad)
    with pytest.raises(ValidationError):
        TheoryKpi(weights_source="uniform", expected_station_time_s={"a": bad})
    with pytest.raises(ValidationError):
        LittleCheck(status="pass", l=bad, tolerance=0.1, n_completed=1, n_in_system=1)


def test_nan_rejected_on_wire():
    raw = encode(_full()).replace(b'"wip_avg":4.2', b'"wip_avg":NaN')
    assert b"NaN" in raw
    with pytest.raises(InvalidEnvelope):
        parse_message(raw)


@pytest.mark.parametrize("status", ["pass", "fail", "insufficient_data"])
def test_little_status_accepted(status):
    lc = LittleCheck(status=status, tolerance=0.1, n_completed=0, n_in_system=0)
    assert lc.status == status


def test_little_status_other_rejected():
    with pytest.raises(ValidationError):
        LittleCheck(status="ok", tolerance=0.1, n_completed=0, n_in_system=0)


def test_negative_wip_rejected():
    with pytest.raises(ValidationError):
        _full(wip_now=-1)


@pytest.mark.parametrize("util", [-0.01, 1.5])
def test_utilization_out_of_range_rejected(util):
    with pytest.raises(ValidationError):
        StationKpi(utilization=util)


def test_utilization_boundary_ok():
    assert StationKpi(utilization=1.0001).utilization == 1.0001
    assert StationKpi(utilization=0).utilization == 0


def test_no_capacity_pass_field():
    assert "capacity_pass" not in KpiSnapshot.model_fields
    assert "throughput_status" not in KpiSnapshot.model_fields


def test_run_params_round_trip():
    snap = _full(run_weights={"margherita": 3.0, "veggie": 1.0}, run_started_ts=TS)
    parsed = parse_message(encode(snap))
    assert isinstance(parsed, KpiSnapshot)
    assert parsed.run_weights == {"margherita": 3.0, "veggie": 1.0}
    assert parsed.run_started_ts == TS
    assert parsed == snap


def test_legacy_snapshot_without_run_params_parses():
    raw = json.loads(encode(_full()))
    raw.pop("run_weights", None)
    raw.pop("run_started_ts", None)
    parsed = parse_message(json.dumps(raw).encode())
    assert isinstance(parsed, KpiSnapshot)
    assert parsed.run_weights is None
    assert parsed.run_started_ts is None


@pytest.mark.parametrize("bad", [{"a": -1.0, "b": 2.0}, {"a": 0.0}, {"a": float("nan")}])
def test_bad_run_weights_rejected(bad):
    with pytest.raises(ValidationError):
        _full(run_weights=bad)


# ---- Phase 7 schema tests ----


def test_snapshot_with_due_and_run_params_round_trip():
    """Test 4: KpiSnapshot with both new optional fields parses correctly."""
    from pizza.common.envelope import DueKpi, RunParams

    due = DueKpi(
        late_count=5,
        n_completed=20,
        n_failed=0,
        late_pct=0.25,
        tardiness_mean_s=15.0,
        tardiness_max_s=60.0,
    )
    run_params = RunParams(rule="edd", comparison_id="c-abc-1", round=1, seed=42)

    snap = _full(due=due, run_params=run_params)
    parsed = parse_message(encode(snap))
    assert isinstance(parsed, KpiSnapshot)
    assert parsed.due is not None
    assert parsed.due.late_count == 5
    assert parsed.run_params is not None
    assert parsed.run_params.rule == "edd"
    assert parsed == snap


def test_legacy_snapshot_without_due_and_run_params_parses():
    """Old KpiSnapshots without due and run_params must still parse."""
    raw = json.loads(encode(_full()))
    raw.pop("due", None)
    raw.pop("run_params", None)
    parsed = parse_message(json.dumps(raw).encode())
    assert isinstance(parsed, KpiSnapshot)
    assert parsed.due is None
    assert parsed.run_params is None


def test_snapshot_with_only_due_parses():
    """KpiSnapshot can have just due without run_params."""
    from pizza.common.envelope import DueKpi

    due = DueKpi(late_count=3, n_completed=15, n_failed=0)
    snap = _full(due=due, run_params=None)
    parsed = parse_message(encode(snap))
    assert parsed.due is not None
    assert parsed.run_params is None


def test_snapshot_with_only_run_params_parses():
    """KpiSnapshot can have just run_params without due."""
    from pizza.common.envelope import RunParams

    run_params = RunParams(rule="spt", comparison_id="c-test-1", round=2)
    snap = _full(due=None, run_params=run_params)
    parsed = parse_message(encode(snap))
    assert parsed.due is None
    assert parsed.run_params is not None
    assert parsed.run_params.rule == "spt"
