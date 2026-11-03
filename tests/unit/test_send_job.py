import uuid

import pytest

from pizza.common.constants import STATIONS
from pizza.common.envelope import LotBank, LotRelease, MachineBreak, MachineRepair, utc_now
from pizza.tools.send_job import (
    build_bank,
    build_break,
    build_job,
    build_order,
    build_release,
    build_repair,
    build_run_started,
    control_target,
    parse_args,
    run_targets,
)


def test_job_defaults():
    args = parse_args(["--station", "bake", "--run-id", "r1"])
    job = build_job(args)
    assert args.mode == "job"
    assert job.qty == 1 and job.unit_time_s == 5.0 and job.visit == 1
    assert job.step == "bake" and job.machine_id == "bake"
    assert job.order_id.startswith("o-")


def test_event_id_reused():
    eid = uuid.uuid4()
    argv = ["job", "--station", "bake", "--run-id", "r1", "--event-id", str(eid)]
    assert build_job(parse_args(argv)).event_id == eid
    other = build_job(parse_args(["--station", "bake", "--run-id", "r1"]))
    assert other.event_id != eid


def test_start_run():
    args = parse_args(["start-run", "--station", "bake", "--run-id", "r1"])
    assert args.mode == "start-run"
    msg = build_run_started(args.station, args.run_id)
    assert msg.machine_id == "bake" and msg.type == "run.started"


def test_job_fail_flags():
    job = build_job(parse_args(["--station", "spin", "--run-id", "r", "--poison"]))
    assert job.poison is True
    job = build_job(parse_args(["--station", "spin", "--run-id", "r", "--fail-times", "2"]))
    assert job.fail_times == 2
    job = build_job(parse_args(["--station", "spin", "--run-id", "r", "--fail-times", "0"]))
    assert job.fail_times == 0
    for bad in ("-1", "100"):
        with pytest.raises(SystemExit):
            parse_args(["--station", "spin", "--run-id", "r", "--fail-times", bad])


def test_build_order_pair():
    args = parse_args(
        [
            "order",
            "--run-id",
            "r",
            "--product",
            "margherita",
            "--qty",
            "1",
            "--fail-step",
            "spin",
            "--poison",
        ]
    )
    created, submitted = build_order(args)
    assert created.state == "created"
    for field in ("order_id", "product", "qty", "due_ts", "created_ts", "fail_step", "poison"):
        assert getattr(created, field) == getattr(submitted, field)
    assert submitted.fail_step == "spin" and submitted.poison is True


def test_order_invalid_exits():
    with pytest.raises(SystemExit):
        parse_args(["order", "--run-id", "r", "--poison"])
    with pytest.raises(SystemExit):
        parse_args(["order", "--run-id", "r", "--product", "nope"])


def test_control_builders_and_targets():
    brk = build_break("bake", "r", 5.0)
    assert isinstance(brk, MachineBreak) and brk.machine_id == "bake" and brk.duration_s == 5.0
    assert isinstance(build_repair("bake", "r"), MachineRepair)
    bank = build_bank("o1", "capacity", "r")
    assert isinstance(bank, LotBank) and bank.machine_id == "scheduler"
    rel = build_release("o1", "r")
    assert isinstance(rel, LotRelease) and rel.machine_id == "scheduler"
    cases = {
        ("break", "--station", "bake"): "station.bake.break",
        ("repair", "--station", "bake"): "station.bake.repair",
        ("bank", "--order-id", "o1"): "scheduler.bank",
        ("release", "--order-id", "o1"): "scheduler.release",
    }
    for (mode, flag, value), key in cases.items():
        args = parse_args([mode, flag, value, "--run-id", "r"])
        assert control_target(args)[0] == key


def test_random_breakdowns_flag():
    on = parse_args(["start-run", "--run-id", "r", "--random-breakdowns"])
    assert on.params.random_breakdowns is True
    assert parse_args(["start-run", "--run-id", "r"]).params is None


def test_run_targets_scheduler():
    assert run_targets("scheduler") == [("scheduler.run", "scheduler")]


def test_invalid_station_exits():
    with pytest.raises(SystemExit):
        parse_args(["--station", "nope", "--run-id", "r1"])


def test_zero_unit_time_rejected():
    with pytest.raises(SystemExit):
        parse_args(["--station", "bake", "--run-id", "r1", "--unit-time-s", "0"])


def test_recipe_job():
    args = parse_args(
        ["job", "--station", "bake", "--run-id", "r", "--recipe", "veggie", "--qty", "2"]
    )
    job = build_job(args)
    assert job.recipe == "veggie" and job.unit_time_s is None and job.step == "bake"


def test_recipe_with_override():
    args = parse_args(
        ["--station", "bake", "--run-id", "r", "--recipe", "veggie", "--unit-time-s", "0.2"]
    )
    job = build_job(args)
    assert job.recipe == "veggie" and job.unit_time_s == 0.2


def test_run_targets_all_generator_last():
    targets = run_targets("all")
    assert targets[0] == ("scheduler.run", "scheduler")
    assert [k for k, _ in targets[1:-2]] == [f"station.{s}.run" for s in STATIONS]
    assert targets[-2] == ("kpi.run", "kpi-service")
    assert targets[-1] == ("generator.run", "generator")


def test_run_targets_single_station():
    assert run_targets("sauce") == [("station.sauce.run", "sauce")]
    args = parse_args(["start-run", "--station", "sauce", "--run-id", "r"])
    assert args.station == "sauce" and args.params is None


def test_start_run_defaults_to_all_with_params():
    args = parse_args(
        [
            "start-run",
            "--run-id",
            "r",
            "--seed",
            "7",
            "--arrival-rate",
            "0.5",
            "--weights",
            "margherita=2,pepperoni=1,veggie=1",
            "--k",
            "2",
            "--k-max",
            "3",
            "--max-orders",
            "10",
        ]
    )
    assert args.station == "all"
    assert args.params.seed == 7
    assert args.params.weights == {"margherita": 2.0, "pepperoni": 1.0, "veggie": 1.0}
    assert args.params.max_orders == 10


def test_targets_share_run_id_and_ts_with_distinct_event_ids():
    ts = utc_now()
    msgs = [build_run_started(m, "r", ts=ts) for _, m in run_targets("all")]
    assert {m.run_id for m in msgs} == {"r"}
    assert {m.ts for m in msgs} == {ts}
    assert len({m.event_id for m in msgs}) == len(msgs)
    assert msgs[0].machine_id == "scheduler" and msgs[-1].machine_id == "generator"


@pytest.mark.parametrize(
    "extra",
    [
        ["--arrival-rate", "0"],
        ["--weights", "veggie=-1"],
        ["--weights", "veggie"],
        ["--weights", "veggie=0"],
        ["--k", "3", "--k-max", "2"],
        ["--max-orders", "0"],
    ],
)
def test_invalid_run_params_exit(extra):
    with pytest.raises(SystemExit):
        parse_args(["start-run", "--run-id", "r", *extra])
