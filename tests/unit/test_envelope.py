"""Unit tests for the unified message envelope (INFRA-03)."""

import json
import uuid
from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from pizza.common import envelope as _env
from pizza.common.constants import PRODUCTS, STATIONS
from pizza.common.envelope import (
    RUN_CONTROL_TYPES,
    Command,
    CycleTimeKpi,
    Envelope,
    Fact,
    InvalidEnvelope,
    JobCommand,
    KpiSnapshot,
    LittleCheck,
    OpCompleted,
    OpStarted,
    OrderLifecycle,
    OrderSubmitted,
    RunParams,
    RunPause,
    RunPaused,
    RunReset,
    RunResume,
    RunResumed,
    RunStarted,
    SetArrivalRate,
    SetRecipe,
    SetRule,
    SmokeCommand,
    SmokeFact,
    StationStatus,
    TheoryKpi,
    encode,
    parse_message,
)

REQUIRED = ["event_id", "run_id", "line_id", "machine_id", "ts", "schema_version"]


def _raw(cls=SmokeCommand) -> dict:
    return json.loads(encode(cls.make(run_id="run-1", machine_id="spin")))


@pytest.mark.parametrize("cls", [SmokeCommand, SmokeFact])
def test_round_trip(cls):
    msg = cls.make(run_id="run-1", machine_id="bake")
    parsed = parse_message(encode(msg))
    assert type(parsed) is cls
    assert parsed == msg


@pytest.mark.parametrize("field", REQUIRED)
def test_missing_field_rejected_on_parse(field):
    raw = _raw()
    del raw[field]
    with pytest.raises(InvalidEnvelope):
        parse_message(json.dumps(raw).encode())


@pytest.mark.parametrize("field", REQUIRED)
def test_missing_field_rejected_on_construct(field):
    kwargs = {
        "event_id": uuid.uuid4(),
        "run_id": "r",
        "line_id": "pizza-1",
        "machine_id": "spin",
        "ts": datetime.now(UTC),
        "schema_version": 1,
    }
    del kwargs[field]
    with pytest.raises(ValidationError):
        SmokeCommand(**kwargs)


def test_naive_ts_rejected():
    raw = _raw()
    raw["ts"] = "2026-10-08T10:00:00.000"
    with pytest.raises(InvalidEnvelope):
        parse_message(json.dumps(raw).encode())


def test_offset_ts_serialised_as_utc_z():
    tz = timezone(timedelta(hours=8))
    msg = SmokeFact.make(run_id="r", machine_id="spin").model_copy(
        update={"ts": datetime(2026, 10, 8, 18, 0, 0, 123000, tzinfo=tz)}
    )
    data = encode(msg).decode()
    assert '"ts":"2026-10-08T10:00:00.123Z"' in data


def test_offset_ts_accepted_on_parse():
    raw = _raw(SmokeFact)
    raw["ts"] = "2026-10-08T18:00:00+08:00"
    parsed = parse_message(json.dumps(raw).encode())
    assert parsed.ts.utcoffset() == timedelta(0) or parsed.ts.tzinfo is not None
    assert '"ts":"2026-10-08T10:00:00.000Z"' in encode(parsed).decode()


def test_schema_version_2_rejected():
    raw = _raw()
    raw["schema_version"] = 2
    with pytest.raises(InvalidEnvelope):
        parse_message(json.dumps(raw).encode())


def test_schema_version_string_rejected():
    raw = _raw()
    raw["schema_version"] = "1"
    with pytest.raises(InvalidEnvelope):
        parse_message(json.dumps(raw).encode())


def test_schema_version_1_accepted():
    assert parse_message(json.dumps(_raw()).encode()).schema_version == 1


@pytest.mark.parametrize("field", ["run_id", "machine_id", "line_id"])
def test_empty_string_rejected(field):
    raw = _raw()
    raw[field] = ""
    with pytest.raises(InvalidEnvelope):
        parse_message(json.dumps(raw).encode())


def test_extra_field_ignored():
    raw = _raw()
    raw["future_field"] = "x"
    parsed = parse_message(json.dumps(raw).encode())
    assert not hasattr(parsed, "future_field")


def test_unknown_type_rejected():
    raw = _raw()
    raw["type"] = "nope"
    with pytest.raises(InvalidEnvelope):
        parse_message(json.dumps(raw).encode())


def test_invalid_json_rejected():
    with pytest.raises(InvalidEnvelope):
        parse_message(b"{not json")


def test_station_id_not_a_field():
    assert "station_id" not in Envelope.model_fields


def test_role_markers():
    assert issubclass(SmokeCommand, Command) and not issubclass(SmokeCommand, Fact)
    assert issubclass(SmokeFact, Fact) and not issubclass(SmokeFact, Command)
    assert issubclass(SmokeCommand, Envelope) and issubclass(SmokeFact, Envelope)


def test_make_fills_defaults():
    msg = SmokeCommand.make(run_id="r", machine_id="spin")
    assert msg.event_id.version == 4
    assert msg.line_id == "pizza-1"
    assert msg.ts.utcoffset() == timedelta(0)
    assert msg.schema_version == 1


def test_encoded_ts_ends_with_z():
    data = json.loads(encode(SmokeFact.make(run_id="r", machine_id="spin")))
    assert data["ts"].endswith("Z")


_JOB = {"order_id": "o1", "step": "bake", "qty": 2, "unit_time_s": 5.0}
_OP = {
    "order_id": "o1",
    "step": "bake",
    "visit": 1,
    "attempt": 1,
    "qty": 2,
    "command_event_id": uuid.uuid4(),
}


def _roundtrip(msg):
    parsed = parse_message(encode(msg))
    assert type(parsed) is type(msg)
    assert parsed == msg
    return parsed


def test_job_command_round_trip_and_default_visit():
    msg = JobCommand.make(run_id="r", machine_id="bake", **_JOB)
    assert _roundtrip(msg).visit == 1
    assert msg.product is None


def test_run_started_round_trip():
    _roundtrip(RunStarted.make(run_id="r", machine_id="bake"))


@pytest.mark.parametrize("status", ["idle", "busy", "offline"])
def test_station_status_round_trip(status):
    _roundtrip(StationStatus.make(run_id="r", machine_id="bake", status=status))


def test_op_started_round_trip():
    _roundtrip(OpStarted.make(run_id="r", machine_id="bake", redelivered=False, **_OP))


def test_op_completed_round_trip_carries_duration():
    msg = OpCompleted.make(run_id="r", machine_id="bake", duration_s=10.0, **_OP)
    assert _roundtrip(msg).duration_s == 10.0


@pytest.mark.parametrize(
    "override",
    [
        {"unit_time_s": 0},
        {"unit_time_s": -1},
        {"qty": 0},
        {"visit": 0},
        {"order_id": ""},
        {"qty": 1000, "unit_time_s": 4.0},
    ],
    ids=["unit_time_s=0", "unit_time_s=-1", "qty=0", "visit=0", "empty_order_id", "over-3600"],
)
def test_job_command_invalid_rejected(override):
    raw = _raw(SmokeCommand)
    raw.update({"type": "job.command", **_JOB, **override})
    with pytest.raises(InvalidEnvelope):
        parse_message(json.dumps(raw).encode())


def test_station_status_unknown_rejected():
    raw = _raw(SmokeFact)
    raw.update({"type": "station.status", "status": "broken"})
    with pytest.raises(InvalidEnvelope):
        parse_message(json.dumps(raw).encode())


def test_op_started_without_command_event_id_rejected():
    msg = OpStarted.make(run_id="r", machine_id="bake", redelivered=False, **_OP)
    raw = json.loads(encode(msg))
    del raw["command_event_id"]
    with pytest.raises(InvalidEnvelope):
        parse_message(json.dumps(raw).encode())


def test_kpi_snapshot_round_trip():
    msg = KpiSnapshot.make(
        run_id="r",
        machine_id="kpi-service",
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
    assert _roundtrip(msg).type == "kpi.snapshot"


def test_phase2_role_markers():
    assert issubclass(JobCommand, Command) and not issubclass(JobCommand, Fact)
    assert issubclass(RunStarted, Command) and not issubclass(RunStarted, Fact)
    for cls in (StationStatus, OpStarted, OpCompleted):
        assert issubclass(cls, Fact) and not issubclass(cls, Command)


@pytest.mark.parametrize("cls", [RunPause, RunResume, RunReset, RunPaused, RunResumed])
def test_phase5_round_trip(cls):
    msg = cls.make(run_id="r1", machine_id="scheduler")
    assert type(parse_message(encode(msg))) is cls


def test_set_arrival_rate_bounds():
    for bad in (0, -1, 100.5):
        with pytest.raises(ValidationError):
            SetArrivalRate.make(run_id="r", machine_id="generator", arrival_rate=bad)
    msg = SetArrivalRate.make(run_id="r", machine_id="generator", arrival_rate=0.05)
    assert parse_message(encode(msg)).arrival_rate == 0.05


def test_set_rule_values():
    for rule in ("fifo", "edd", "spt"):
        msg = SetRule.make(run_id="r", machine_id="scheduler", rule=rule)
        assert parse_message(encode(msg)).rule == rule
    with pytest.raises(ValidationError):
        SetRule.make(run_id="r", machine_id="scheduler", rule="lifo")


def test_set_recipe_values():
    msg = SetRecipe.make(
        run_id="r",
        machine_id="bake",
        station="bake",
        product="margherita",
        unit_time_s=9.5,
    )
    parsed = parse_message(encode(msg))
    assert isinstance(parsed, SetRecipe)
    assert (parsed.station, parsed.product, parsed.unit_time_s) == ("bake", "margherita", 9.5)
    for kwargs in (
        {"station": "fryer", "product": "margherita", "unit_time_s": 1.0},
        {"station": "bake", "product": "Margherita", "unit_time_s": 1.0},
        {"station": "bake", "product": "margherita", "unit_time_s": 0},
        {"station": "bake", "product": "margherita", "unit_time_s": 3601},
    ):
        with pytest.raises(ValidationError):
            SetRecipe.make(run_id="r", machine_id="bake", **kwargs)


def test_phase5_role_markers():
    for cls in (RunPause, RunResume, RunReset, SetArrivalRate, SetRule):
        assert issubclass(cls, Command) and not issubclass(cls, Fact)
    for cls in (RunPaused, RunResumed):
        assert issubclass(cls, Fact) and not issubclass(cls, Command)
    assert set(RUN_CONTROL_TYPES) == {
        RunStarted,
        RunPause,
        RunResume,
        RunReset,
        SetArrivalRate,
        SetRule,
    }


def test_phase5_schema_version_rejected():
    raw = _raw(RunPause)
    raw["schema_version"] = 2
    with pytest.raises(InvalidEnvelope):
        parse_message(json.dumps(raw).encode())


def _order_raw(cls, **extra) -> dict:
    ts = datetime(2026, 1, 1, tzinfo=UTC)
    kw = {"state": "created"} if cls is OrderLifecycle else {}
    msg = cls.make(
        run_id="r",
        machine_id="scheduler",
        order_id="o1",
        product="margherita",
        qty=1,
        due_ts=ts,
        created_ts=ts,
        **kw,
    )
    raw = json.loads(encode(msg))
    raw.pop("source", None)
    raw.pop("priority", None)
    raw.update(extra)
    return raw


@pytest.mark.parametrize("cls", [OrderSubmitted, OrderLifecycle])
def test_order_markers_optional_and_round_trip(cls):
    old = parse_message(json.dumps(_order_raw(cls)).encode())
    assert old.source is None and old.priority == 50
    new = parse_message(json.dumps(_order_raw(cls, source="manual", priority=10)).encode())
    assert new.source == "manual" and new.priority == 10


# ---- Phase 07.2: numeric priority replaces rush (D-05, D-06) ----

_PRIORITY_MODELS = [OrderSubmitted, OrderLifecycle, _env.ManualOrderRequest, _env.EstimateEta]


def _priority_kwargs(cls, **over) -> dict:
    """Build make() kwargs for each of the four priority-carrying models."""
    ts = datetime(2026, 1, 1, tzinfo=UTC)
    base = {"run_id": "r", "machine_id": "scheduler", "product": "margherita", "qty": 1}
    if cls in (OrderSubmitted, OrderLifecycle):
        base.update({"order_id": "o1", "due_ts": ts, "created_ts": ts})
    if cls is OrderLifecycle:
        base["state"] = "created"
    if cls in (_env.ManualOrderRequest, _env.EstimateEta):
        base["deadline_ts"] = ts
    base.update(over)
    return base


@pytest.mark.parametrize("cls", _PRIORITY_MODELS)
def test_priority_defaults_to_50(cls):
    msg = cls.make(**_priority_kwargs(cls))
    assert msg.priority == 50


@pytest.mark.parametrize("cls", _PRIORITY_MODELS)
@pytest.mark.parametrize("value", [1, 50, 99])
def test_priority_in_range_accepted(cls, value):
    msg = cls.make(**_priority_kwargs(cls, priority=value))
    assert parse_message(encode(msg)).priority == value


@pytest.mark.parametrize("cls", _PRIORITY_MODELS)
@pytest.mark.parametrize("value", [0, 100, -1])
def test_priority_out_of_range_rejected(cls, value):
    with pytest.raises(ValidationError):
        cls.make(**_priority_kwargs(cls, priority=value))


@pytest.mark.parametrize("cls", [OrderSubmitted, OrderLifecycle])
def test_priority_dumps_no_rush_key(cls):
    msg = cls.make(**_priority_kwargs(cls, priority=10))
    data = json.loads(encode(msg))
    assert data["priority"] == 10
    assert "rush" not in data


@pytest.mark.parametrize("cls", [OrderSubmitted, OrderLifecycle])
@pytest.mark.parametrize("rush,expected", [(True, 10), (False, 50)])
def test_legacy_rush_raw_json_maps_priority(cls, rush, expected):
    raw = _order_raw(cls)
    raw["rush"] = rush
    parsed = parse_message(json.dumps(raw).encode())
    assert parsed.priority == expected
    assert not hasattr(parsed, "rush")


@pytest.mark.parametrize("cls", [OrderSubmitted, OrderLifecycle])
def test_legacy_missing_both_keys_defaults_50(cls):
    raw = _order_raw(cls)
    parsed = parse_message(json.dumps(raw).encode())
    assert parsed.priority == 50


@pytest.mark.parametrize("cls", [OrderSubmitted, OrderLifecycle])
def test_legacy_priority_wins_over_rush(cls):
    raw = _order_raw(cls)
    raw["priority"] = 7
    raw["rush"] = True
    parsed = parse_message(json.dumps(raw).encode())
    assert parsed.priority == 7


def test_legacy_rush_dict_input_manual_order_and_estimate():
    ts = datetime(2026, 1, 1, tzinfo=UTC)
    base = {
        "run_id": "r",
        "machine_id": "d",
        "product": "margherita",
        "qty": 1,
        "deadline_ts": ts,
    }
    for cls in (_env.ManualOrderRequest, _env.EstimateEta):
        # Build a complete envelope dict from the model's own make(), then drop
        # the modern key so the legacy-rush branch is what gets exercised.
        raw = json.loads(encode(cls.make(**base)))
        legacy = {k: v for k, v in raw.items() if k != "priority"}
        assert cls.model_validate({**legacy, "rush": True}).priority == 10
        assert cls.model_validate({**legacy, "rush": False}).priority == 50
        assert cls.model_validate(legacy).priority == 50
        assert cls.model_validate({**legacy, "priority": 3, "rush": True}).priority == 3


def test_schema_version_still_literal_1_for_priority_models():
    for cls in _PRIORITY_MODELS:
        msg = cls.make(**_priority_kwargs(cls, priority=10))
        assert msg.schema_version == 1
        raw = json.loads(encode(msg))
        raw["schema_version"] = 2
        with pytest.raises(InvalidEnvelope):
            parse_message(json.dumps(raw).encode())


# ---- Phase 6 contracts ----


def _rt(msg):
    return parse_message(encode(msg))


def _op_kw():
    return dict(
        order_id="o1", step="bake", visit=1, attempt=1, qty=1, command_event_id=uuid.uuid4()
    )


def test_op_failed_round_trip_and_empty_reason():
    msg = _env.OpFailed.make(run_id="r", machine_id="bake", reason="injected", **_op_kw())
    assert _rt(msg) == msg
    with pytest.raises(ValidationError):
        _env.OpFailed.make(run_id="r", machine_id="bake", reason="", **_op_kw())


def test_op_failed_cause_defaults_none():
    msg = _env.OpFailed.make(run_id="r", machine_id="bake", reason="x", **_op_kw())
    assert msg.cause is None
    assert _rt(msg).cause is None


@pytest.mark.parametrize("cause", ["breakdown", "quality"])
def test_op_failed_cause_roundtrip(cause):
    msg = _env.OpFailed.make(run_id="r", machine_id="bake", reason="x", cause=cause, **_op_kw())
    assert json.loads(encode(msg))["cause"] == cause
    assert _rt(msg) == msg


def test_op_failed_cause_rejects_unknown():
    with pytest.raises(ValidationError):
        _env.OpFailed.make(run_id="r", machine_id="bake", reason="x", cause="bogus", **_op_kw())


def test_op_failed_old_json_without_cause_parses():
    msg = _env.OpFailed.make(run_id="r", machine_id="bake", reason="x", **_op_kw())
    data = json.loads(encode(msg))
    data.pop("cause", None)
    data["unknown_extra"] = 1
    parsed = parse_message(json.dumps(data).encode())
    assert isinstance(parsed, _env.OpFailed)
    assert parsed.cause is None


def test_op_failed_schema_version_unchanged():
    msg = _env.OpFailed.make(
        run_id="r", machine_id="bake", reason="x", cause="breakdown", **_op_kw()
    )
    assert msg.schema_version == 1


def test_station_status_down_fields():
    ok = _env.StationStatus.make(
        run_id="r", machine_id="bake", status="down", cause="manual", repair_in_s=12.5
    )
    assert _rt(ok).repair_in_s == 12.5
    with pytest.raises(ValidationError):
        _env.StationStatus.make(run_id="r", machine_id="bake", status="down", cause="x")
    with pytest.raises(ValidationError):
        _env.StationStatus.make(run_id="r", machine_id="bake", status="down", repair_in_s=-1)
    legacy = _env.StationStatus.make(run_id="r", machine_id="bake", status="idle")
    raw = json.loads(encode(legacy))
    raw.pop("cause")
    raw.pop("repair_in_s")
    assert parse_message(json.dumps(raw).encode()).cause is None


def test_station_heartbeat():
    for st in ("idle", "busy", "down"):
        assert _rt(_env.StationHeartbeat.make(run_id="unknown", machine_id="bake", status=st))
    with pytest.raises(ValidationError):
        _env.StationHeartbeat.make(run_id="r", machine_id="bake", status="offline")


@pytest.mark.parametrize("cls", [OrderSubmitted, OrderLifecycle])
def test_fail_flags_need_valid_fail_step(cls):
    base = _order_raw(cls)
    for extra in ({"poison": True}, {"fail_times": 1}, {"fail_step": "oven", "poison": True}):
        with pytest.raises(InvalidEnvelope):
            parse_message(json.dumps({**base, **extra}).encode())
    ok = parse_message(json.dumps({**base, "fail_step": "bake", "fail_times": 1}).encode())
    assert ok.fail_step == "bake"


def test_lifecycle_failed_state():
    raw = _order_raw(OrderLifecycle, state="failed", step="bake", visit=1)
    assert parse_message(json.dumps(raw).encode()).state == "failed"


def test_job_command_fail_bounds():
    kw = dict(run_id="r", machine_id="bake", order_id="o", step="bake", qty=1, unit_time_s=1.0)
    assert JobCommand.make(fail_times=2, poison=True, **kw).fail_times == 2
    for bad in (100, -1):
        with pytest.raises(ValidationError):
            JobCommand.make(fail_times=bad, **kw)


def test_lot_and_machine_messages():
    with pytest.raises(ValidationError):
        _env.LotBank.make(run_id="r", machine_id="scheduler", order_id="o", reason="other")
    with pytest.raises(ValidationError):
        _env.LotScrap.make(run_id="r", machine_id="scheduler", order_id="o", reason="capacity")
    for d in (0, 3601):
        with pytest.raises(ValidationError):
            _env.MachineBreak.make(run_id="r", machine_id="bake", duration_s=d)
    for msg in (
        _env.LotBanked.make(
            run_id="r",
            machine_id="s",
            order_id="o",
            station="bake",
            reason="capacity",
            source="manual",
        ),
        _env.LotReleased.make(run_id="r", machine_id="s", order_id="o", station="bake", by="auto"),
        _env.LotStocked.make(
            run_id="r", machine_id="s", order_id="o", station="bake", kind="overflow"
        ),
        _env.LotUnstocked.make(run_id="r", machine_id="s", order_id="o", station="bake"),
        _env.LotBank.make(run_id="r", machine_id="s", order_id="o", reason="customer"),
        _env.LotRelease.make(run_id="r", machine_id="s", order_id="o"),
        _env.LotScrap.make(run_id="r", machine_id="s", order_id="o", reason="operator"),
        _env.LotHold.make(
            run_id="r",
            machine_id="s",
            order_id="o",
            station="bake",
            state="requested",
            kind="bank",
            reason="customer",
        ),
        _env.LotHoldCancel.make(run_id="r", machine_id="s", order_id="o"),
        _env.MachineBreak.make(run_id="r", machine_id="bake", duration_s=5),
        _env.MachineRepair.make(run_id="r", machine_id="bake"),
    ):
        assert _rt(msg) == msg


def test_control_type_tuples():
    assert len(RUN_CONTROL_TYPES) == 6
    assert set(RUN_CONTROL_TYPES) < set(_env.STATION_CONTROL_TYPES)
    assert {_env.MachineBreak, _env.MachineRepair} <= set(_env.STATION_CONTROL_TYPES)
    assert set(RUN_CONTROL_TYPES) < set(_env.SCHEDULER_CONTROL_TYPES)
    assert {_env.LotBank, _env.LotRelease, _env.LotScrap} <= set(_env.SCHEDULER_CONTROL_TYPES)
    assert _env.LotHoldCancel in _env.SCHEDULER_CONTROL_TYPES


def test_lot_hold_validation():
    with pytest.raises(ValidationError):
        _env.LotHold.make(
            run_id="r", machine_id="s", order_id="o", station="bake",
            state="bogus", kind="bank", reason="x",
        )
    with pytest.raises(ValidationError):
        _env.LotHold.make(
            run_id="r", machine_id="s", order_id="o", station="bake",
            state="requested", kind="bank", reason="",
        )
    with pytest.raises(ValidationError):
        _env.LotHoldCancel.make(run_id="r", machine_id="s", order_id="")


def test_station_kpi_availability_bound():
    assert _env.StationKpi(availability=1.0).availability == 1.0
    with pytest.raises(ValidationError):
        _env.StationKpi(availability=1.5)


# ---- Phase 7 contracts (dispatch rules and comparison) ----


def test_rule_changed_fact_round_trip():
    """Test 1: RuleChanged round-trips and rejects invalid rule."""
    msg = _env.RuleChanged.make(run_id="r1", machine_id="scheduler", rule="edd")
    parsed = _rt(msg)
    assert isinstance(parsed, _env.RuleChanged)
    assert parsed.rule == "edd"
    assert parsed.type == "rule.changed"

    # Test that invalid rule is rejected
    with pytest.raises(ValidationError):
        _env.RuleChanged.make(run_id="r1", machine_id="scheduler", rule="lifo")


def test_rule_changed_is_fact():
    """RuleChanged must be a Fact (travels on Kafka)."""
    msg = _env.RuleChanged.make(run_id="r1", machine_id="scheduler", rule="fifo")
    assert isinstance(msg, Fact)
    assert not isinstance(msg, Command)


def test_round_close_command():
    """Test 2: RoundClose defaults partial=False and round-trips."""
    msg = _env.RoundClose.make(run_id="r1", machine_id="kpi-service")
    parsed = _rt(msg)
    assert isinstance(parsed, _env.RoundClose)
    assert parsed.partial is False
    assert parsed.type == "kpi.close_round"

    # Test partial=True
    msg2 = _env.RoundClose.make(run_id="r1", machine_id="kpi-service", partial=True)
    parsed2 = _rt(msg2)
    assert parsed2.partial is True


def test_round_close_is_command():
    """RoundClose must be a Command (travels on RabbitMQ)."""
    msg = _env.RoundClose.make(run_id="r1", machine_id="kpi-service")
    assert isinstance(msg, Command)
    assert not isinstance(msg, Fact)


def test_kpi_control_types_includes_round_close():
    """RoundClose must be in KPI_CONTROL_TYPES but NOT in RUN_CONTROL_TYPES."""
    assert _env.RoundClose in _env.KPI_CONTROL_TYPES
    assert _env.RoundClose not in _env.RUN_CONTROL_TYPES


def test_run_params_extension_with_rule_comparison_round():
    """Test 3: RunParams with new rule, comparison_id, round fields."""
    # Test old behavior still works
    old = RunParams()
    assert old.rule is None
    assert old.comparison_id is None
    assert old.round is None

    # Test new fields validate
    new = RunParams(rule="spt", comparison_id="c-abc-1", round=2)
    assert new.rule == "spt"
    assert new.comparison_id == "c-abc-1"
    assert new.round == 2

    # Test round bounds
    with pytest.raises(ValidationError):
        RunParams(round=0)
    with pytest.raises(ValidationError):
        RunParams(round=4)

    # Test comparison_id pattern
    with pytest.raises(ValidationError):
        RunParams(comparison_id="invalid id")  # space not allowed
    with pytest.raises(ValidationError):
        RunParams(comparison_id="c-" + "a" * 37)  # > 36 chars after "c-"


def test_due_kpi_model():
    """Test 5: DueKpi rejects negative counts and late_pct outside 0..1."""
    # Valid DueKpi
    due = _env.DueKpi(late_count=5, n_completed=10, n_failed=0)
    assert due.late_count == 5
    assert due.n_completed == 10
    assert due.late_pct is None

    # Test bounds
    with pytest.raises(ValidationError):
        _env.DueKpi(late_count=-1, n_completed=10, n_failed=0)

    with pytest.raises(ValidationError):
        _env.DueKpi(late_count=5, n_completed=10, n_failed=0, late_pct=1.5)

    with pytest.raises(ValidationError):
        _env.DueKpi(late_count=5, n_completed=10, n_failed=0, late_pct=-0.1)

    # Test all metric fields can be None
    due_sparse = _env.DueKpi()
    assert due_sparse.late_count is None
    assert due_sparse.n_completed is None
    assert due_sparse.late_pct is None


def test_comparison_result_round_trip():
    """Test 6: ComparisonResult round-trips and validates fields."""
    msg = _env.ComparisonResult.make(
        run_id="r1",
        machine_id="kpi-service",
        comparison_id="c-abc-1",
        round=1,
        rule="fifo",
        seed=42,
        n_orders=30,
        n_completed=28,
        n_failed=2,
        late_count=5,
        mean_cycle_time_s=100.5,
        mean_tardiness_s=10.2,
        max_tardiness_s=45.0,
        pct_late=0.18,
        makespan_s=1500.0,
        partial=False,
        k=1.5,
        arrival_rate_per_min=2.0,
        random_breakdowns=True,
        max_orders=30,
    )
    parsed = _rt(msg)
    assert isinstance(parsed, _env.ComparisonResult)
    assert parsed.type == "comparison.result"
    assert parsed.rule == "fifo"
    assert parsed.n_orders == 30


def test_comparison_result_is_fact():
    """ComparisonResult must be a Fact (travels on Kafka)."""
    msg = _env.ComparisonResult.make(
        run_id="r1",
        machine_id="kpi-service",
        comparison_id="c-abc-1",
        round=1,
        rule="edd",
        seed=42,
        n_orders=30,
        n_completed=30,
        n_failed=0,
        late_count=10,
        mean_cycle_time_s=100.0,
        pct_late=0.33,
        k=1.5,
        arrival_rate_per_min=2.0,
    )
    assert isinstance(msg, Fact)
    assert not isinstance(msg, Command)


def test_comparison_key_helper():
    """Test that comparison_key formats correctly."""
    key = _env.comparison_key("c-abc-1", 2)
    assert key == "cmp:c-abc-1:2"

    key2 = _env.comparison_key("c-test-123", 1)
    assert key2 == "cmp:c-test-123:1"


# ---- Phase 8: recipe change facts ----


def _full_recipe_table(value: float = 1.0) -> dict[str, dict[str, float]]:
    return {station: {product: value for product in PRODUCTS} for station in STATIONS}


def test_recipe_changed_round_trip_with_old_value():
    """RecipeChanged round-trips through parse_message with both unit times."""
    msg = _env.RecipeChanged.make(
        run_id="r1",
        machine_id="bake",
        station="bake",
        product="margherita",
        old_unit_time_s=12.0,
        new_unit_time_s=20.0,
    )
    parsed = _rt(msg)
    assert isinstance(parsed, _env.RecipeChanged)
    assert parsed.type == "recipe.changed"
    assert parsed.station == "bake"
    assert parsed.product == "margherita"
    assert parsed.old_unit_time_s == 12.0
    assert parsed.new_unit_time_s == 20.0


def test_recipe_changed_round_trip_without_old_value():
    """old_unit_time_s may be None and still round-trips."""
    msg = _env.RecipeChanged.make(
        run_id="r1",
        machine_id="spin",
        station="spin",
        product="pepperoni",
        new_unit_time_s=5.0,
    )
    parsed = _rt(msg)
    assert isinstance(parsed, _env.RecipeChanged)
    assert parsed.old_unit_time_s is None
    assert parsed.new_unit_time_s == 5.0


def test_recipe_changed_rejects_unknown_station():
    with pytest.raises(ValidationError):
        _env.RecipeChanged.make(
            run_id="r1",
            machine_id="oven",
            station="oven",
            product="margherita",
            new_unit_time_s=5.0
        )


@pytest.mark.parametrize("bad_value", [0, -1, 3601.0])
def test_recipe_changed_rejects_out_of_bounds_new_time(bad_value):
    with pytest.raises(ValidationError):
        _env.RecipeChanged.make(
            run_id="r1",
            machine_id="bake",
            station="bake",
            product="margherita",
            new_unit_time_s=bad_value,
        )


def test_recipe_facts_are_facts_not_commands():
    changed = _env.RecipeChanged.make(
        run_id="r1", machine_id="bake", station="bake", product="margherita", new_unit_time_s=20.0
    )
    snap = _env.RecipeSnapshot.make(
        run_id="r1", machine_id="scheduler", recipes=_full_recipe_table()
    )
    for msg in (changed, snap):
        assert isinstance(msg, Fact)
        assert not isinstance(msg, Command)


def test_recipe_snapshot_round_trip_preserves_full_table():
    table = {
        station: {product: float(i + j + 1) for j, product in enumerate(sorted(PRODUCTS))}
        for i, station in enumerate(sorted(STATIONS))
    }
    msg = _env.RecipeSnapshot.make(run_id="r1", machine_id="scheduler", recipes=table)
    parsed = _rt(msg)
    assert isinstance(parsed, _env.RecipeSnapshot)
    assert parsed.type == "recipe.snapshot"
    assert set(parsed.recipes) == set(STATIONS)
    assert len(parsed.recipes) == 7
    for station in STATIONS:
        assert set(parsed.recipes[station]) == set(PRODUCTS)
        assert parsed.recipes[station] == table[station]


def test_recipe_snapshot_rejects_six_stations():
    table = _full_recipe_table()
    table.pop(next(iter(table)))
    with pytest.raises(ValidationError):
        _env.RecipeSnapshot.make(run_id="r1", machine_id="scheduler", recipes=table)


def test_recipe_snapshot_rejects_station_missing_product():
    table = _full_recipe_table()
    assert "veggie" in PRODUCTS
    table["bake"].pop("veggie")
    with pytest.raises(ValidationError):
        _env.RecipeSnapshot.make(run_id="r1", machine_id="scheduler", recipes=table)


def test_recipe_snapshot_rejects_zero_value():
    table = _full_recipe_table()
    station = next(iter(table))
    table[station][next(iter(table[station]))] = 0.0
    with pytest.raises(ValidationError):
        _env.RecipeSnapshot.make(run_id="r1", machine_id="scheduler", recipes=table)


# ---- Phase 07.1: ETA contracts -------------------------------------------------

_DT_US = datetime(2026, 10, 21, 1, 2, 3, 456789, tzinfo=UTC)
_ORDER = {
    "order_id": "L-0001",
    "product": "margherita",
    "qty": 2,
    "due_ts": _DT_US,
    "created_ts": _DT_US,
}


def _eta_fields():
    return {"eta_ts": _DT_US, "station_eta": dict.fromkeys(STATIONS, _DT_US)}


@pytest.mark.parametrize("cls", [_env.OrderLifecycle, _env.OrderSubmitted])
def test_eta_fields_serialise_ms_z_and_roundtrip(cls):
    extra = {"state": "created"} if cls is _env.OrderLifecycle else {}
    msg = cls.make(run_id="r", machine_id="scheduler", **_ORDER, **extra, **_eta_fields())
    data = json.loads(encode(msg))
    assert data["eta_ts"] == "2026-10-21T01:02:03.456Z"
    assert set(data["station_eta"]) == set(STATIONS)
    assert all(v == "2026-10-21T01:02:03.456Z" for v in data["station_eta"].values())
    parsed = parse_message(encode(msg))
    assert parsed.eta_ts.tzinfo is not None
    assert set(parsed.station_eta) == set(STATIONS)


@pytest.mark.parametrize("cls", [_env.OrderLifecycle, _env.OrderSubmitted])
def test_pre_phase_fact_parses_with_eta_none(cls):
    extra = {"state": "created"} if cls is _env.OrderLifecycle else {}
    msg = cls.make(run_id="r", machine_id="scheduler", **_ORDER, **extra)
    data = json.loads(encode(msg))
    data.pop("eta_ts", None)
    data.pop("station_eta", None)
    parsed = parse_message(json.dumps(data).encode())
    assert parsed.eta_ts is None
    assert parsed.station_eta is None
    encode(parsed)


@pytest.mark.parametrize(
    "bad",
    [
        {"oven": _DT_US},
        {"spin": datetime(2026, 10, 21, 1, 2, 3)},
        {f"s{i}": _DT_US for i in range(8)},
    ],
)
def test_station_eta_invalid_rejected(bad):
    with pytest.raises(ValidationError):
        _env.OrderLifecycle.make(
            run_id="r", machine_id="scheduler", state="created", **_ORDER, station_eta=bad
        )


def test_estimate_commands_routing():
    for cls in (_env.EstimateEta, _env.EstimateWip):
        assert cls in _env.SCHEDULER_CONTROL_TYPES
        assert cls not in _env.GENERATOR_CONTROL_TYPES
        assert cls not in _env.KPI_CONTROL_TYPES
        assert cls not in _env.STATION_CONTROL_TYPES
    dt_ms = _DT_US.replace(microsecond=456000)
    eta = _env.EstimateEta.make(
        run_id="r", machine_id="dashboard", product="margherita", qty=1, deadline_ts=dt_ms
    )
    assert eta.type == "scheduler.estimate_eta"
    assert parse_message(encode(eta)) == eta
    wip = _env.EstimateWip.make(run_id="r", machine_id="dashboard", deadline_ts=dt_ms)
    assert wip.type == "scheduler.estimate_wip"
    assert parse_message(encode(wip)) == wip


def _eta_kwargs(**over):
    base = {"product": "margherita", "qty": 1, "deadline_ts": _DT_US}
    base.update(over)
    return base


@pytest.mark.parametrize("k", [0.49, 3.01, float("nan"), float("inf")])
def test_estimate_eta_buffer_k_rejected(k):
    with pytest.raises(ValidationError):
        _env.EstimateEta.make(run_id="r", machine_id="d", **_eta_kwargs(buffer_k=k))


@pytest.mark.parametrize("k", [0.5, 1.0, 3])
def test_estimate_eta_buffer_k_accepted(k):
    _env.EstimateEta.make(run_id="r", machine_id="d", **_eta_kwargs(buffer_k=k))


@pytest.mark.parametrize(
    "over",
    [{"qty": 0}, {"product": "Bad Slug!"}, {"due_ts": datetime(2026, 10, 21, 1, 2, 3)}],
)
def test_estimate_eta_invalid_rejected(over):
    with pytest.raises(ValidationError):
        _env.EstimateEta.make(run_id="r", machine_id="d", **_eta_kwargs(**over))


def test_manual_order_buffer_k_default_and_bounds():
    ok = _env.ManualOrderRequest.make(run_id="r", machine_id="d", **_eta_kwargs())
    assert ok.buffer_k == 1.5
    for k in (0.49, 3.01, float("nan"), float("inf")):
        with pytest.raises(ValidationError):
            _env.ManualOrderRequest.make(run_id="r", machine_id="d", **_eta_kwargs(buffer_k=k))


def test_run_params_due_mode_and_buffer_k():
    p = _env.RunParams()
    assert p.due_mode == "eta"
    assert p.buffer_k == 1.5
    assert _env.RunParams.model_validate({"seed": 1}).due_mode == "eta"
    with pytest.raises(ValidationError):
        _env.RunParams(due_mode="bogus")
    for k in (0.49, 10.01, float("nan")):
        with pytest.raises(ValidationError):
            _env.RunParams(buffer_k=k)
    assert _env.RunParams(buffer_k=0.5).buffer_k == 0.5
    assert _env.RunParams(buffer_k=10).buffer_k == 10


# ---- Phase 07.2: priority.changed fact + lot.set_priority command (D-11) ----


def test_priority_changed_round_trip():
    msg = _env.PriorityChanged.make(
        run_id="r1",
        machine_id="scheduler",
        order_id="o1",
        previous_priority=50,
        new_priority=10,
        source="manual",
    )
    parsed = _rt(msg)
    assert isinstance(parsed, _env.PriorityChanged)
    assert parsed.type == "priority.changed"
    assert parsed.order_id == "o1"
    assert parsed.previous_priority == 50
    assert parsed.new_priority == 10
    assert parsed.source == "manual"
    assert parsed.schema_version == 1


def test_priority_changed_is_fact():
    msg = _env.PriorityChanged.make(
        run_id="r1",
        machine_id="scheduler",
        order_id="o1",
        previous_priority=50,
        new_priority=10,
        source="manual",
    )
    assert isinstance(msg, Fact)
    assert not isinstance(msg, Command)


@pytest.mark.parametrize(
    "over",
    [
        {"previous_priority": 0},
        {"previous_priority": 100},
        {"new_priority": 0},
        {"new_priority": 100},
        {"order_id": ""},
        {"source": "auto"},
    ],
)
def test_priority_changed_invalid_rejected(over):
    kw = {
        "run_id": "r1",
        "machine_id": "scheduler",
        "order_id": "o1",
        "previous_priority": 50,
        "new_priority": 10,
        "source": "manual",
    }
    kw.update(over)
    with pytest.raises(ValidationError):
        _env.PriorityChanged.make(**kw)


def test_lot_set_priority_command_round_trip():
    msg = _env.LotSetPriority.make(
        run_id="r1", machine_id="dashboard", order_id="o1", priority=10
    )
    parsed = _rt(msg)
    assert isinstance(parsed, _env.LotSetPriority)
    assert parsed.type == "lot.set_priority"
    assert parsed.order_id == "o1"
    assert parsed.priority == 10
    assert isinstance(parsed, Command)
    assert not isinstance(parsed, Fact)


@pytest.mark.parametrize("bad", [0, 100, -1])
def test_lot_set_priority_bounds(bad):
    with pytest.raises(ValidationError):
        _env.LotSetPriority.make(run_id="r1", machine_id="dashboard", order_id="o1", priority=bad)


def test_lot_set_priority_in_scheduler_control_types():
    assert _env.LotSetPriority in _env.SCHEDULER_CONTROL_TYPES
    assert _env.LotSetPriority not in _env.GENERATOR_CONTROL_TYPES
    assert _env.LotSetPriority not in _env.KPI_CONTROL_TYPES
    assert _env.LotSetPriority not in _env.STATION_CONTROL_TYPES


def test_priority_changed_in_message_union():
    raw = encode(
        _env.PriorityChanged.make(
            run_id="r1",
            machine_id="scheduler",
            order_id="o1",
            previous_priority=50,
            new_priority=10,
            source="manual",
        )
    )
    assert isinstance(parse_message(raw), _env.PriorityChanged)


def test_scheduler_priority_routing_key_defined_once():
    from pizza.common import topology

    assert topology.ROUTING_SCHEDULER_PRIORITY == "scheduler.priority"
