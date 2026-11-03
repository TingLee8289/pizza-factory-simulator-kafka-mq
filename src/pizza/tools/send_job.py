"""Operator CLI: publish jobs, orders, run/break/repair/bank/release control commands."""

import argparse
import asyncio
import re
import sys
import uuid
from collections.abc import Sequence
from datetime import datetime, timedelta

from pydantic import ValidationError

from pizza.common.config import get_settings
from pizza.common.constants import LINE_ID, MAX_ORDER_QTY, PRODUCTS, SLUG_PATTERN, STATIONS
from pizza.common.envelope import (
    Command,
    JobCommand,
    LotBank,
    LotRelease,
    MachineBreak,
    MachineRepair,
    OrderLifecycle,
    OrderSubmitted,
    RunParams,
    RunStarted,
    new_event_id,
    utc_now,
)
from pizza.common.kafka_io import make_producer, produce_fact
from pizza.common.rabbit_io import connect, publish_command
from pizza.common.topology import (
    EX_CONTROL,
    EX_DISPATCH,
    EX_WORKORDERS,
    TOPIC_ORDER_LIFECYCLE,
)

MACHINE_ID = "send_job"
INTAKE_KEY = "intake"
SCHEDULER_ID = "scheduler"
CONTROL_MODES = ("break", "repair", "bank", "release")


def _positive_float(value: str) -> float:
    number = float(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be > 0")
    return number


def _positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be >= 1")
    return number


def _non_negative_int(value: str) -> int:
    number = int(value)
    if not 0 <= number <= 99:
        raise argparse.ArgumentTypeError("must be between 0 and 99")
    return number


def _qty(value: str) -> int:
    number = int(value)
    if not 1 <= number <= MAX_ORDER_QTY:
        raise argparse.ArgumentTypeError(f"must be between 1 and {MAX_ORDER_QTY}")
    return number


def _slug(value: str) -> str:
    if not re.match(SLUG_PATTERN, value):
        raise argparse.ArgumentTypeError(f"invalid slug {value!r}")
    return value


def _weights(value: str) -> dict[str, float]:
    """Parse "margherita=2,pepperoni=1" into a dict; reject malformed or negative."""
    out: dict[str, float] = {}
    for part in value.split(","):
        name, sep, raw = part.partition("=")
        if not sep:
            raise argparse.ArgumentTypeError("expected name=weight[,name=weight...]")
        _slug(name)
        try:
            weight = float(raw)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(f"bad weight {raw!r}") from exc
        if not weight >= 0:
            raise argparse.ArgumentTypeError("weights must be >= 0")
        out[name] = weight
    return out


def _add_job_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--station", choices=STATIONS, default=argparse.SUPPRESS)
    p.add_argument("--order-id", default=argparse.SUPPRESS)
    p.add_argument("--qty", type=_positive_int, default=argparse.SUPPRESS)
    p.add_argument("--recipe", type=_slug, default=argparse.SUPPRESS)
    p.add_argument("--unit-time-s", type=_positive_float, default=argparse.SUPPRESS)
    p.add_argument("--visit", type=_positive_int, default=argparse.SUPPRESS)
    p.add_argument("--run-id", default=argparse.SUPPRESS)
    p.add_argument("--event-id", type=uuid.UUID, default=argparse.SUPPRESS)
    p.add_argument("--product", default=argparse.SUPPRESS)
    p.add_argument("--fail-times", type=_non_negative_int, default=argparse.SUPPRESS)
    p.add_argument("--poison", action="store_true", default=argparse.SUPPRESS)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m pizza.tools.send_job")
    sub = parser.add_subparsers(dest="mode")

    job = sub.add_parser("job", help="publish a JobCommand (default)")
    _add_job_args(job)

    run = sub.add_parser("start-run", help="publish RunStarted on the control exchange")
    run.add_argument("--station", choices=[*STATIONS, "all", "scheduler"], default="all")
    run.add_argument("--run-id", required=True)
    run.add_argument("--event-id", type=uuid.UUID, default=None)
    run.add_argument("--seed", type=int, default=None)
    run.add_argument("--arrival-rate", type=_positive_float, default=None)
    run.add_argument("--weights", type=_weights, default=None)
    run.add_argument("--k", type=_positive_float, default=None)
    run.add_argument("--k-max", type=_positive_float, default=None)
    run.add_argument("--max-orders", type=_positive_int, default=None)
    run.add_argument("--random-breakdowns", action="store_true")

    order = sub.add_parser("order", help="submit an order (optionally fail-flagged)")
    order.add_argument("--run-id", required=True)
    order.add_argument("--order-id", default=None)
    order.add_argument("--product", default="margherita", choices=PRODUCTS)
    order.add_argument("--qty", type=_qty, default=1)
    order.add_argument("--due-in-s", type=_positive_float, default=600.0)
    order.add_argument("--fail-step", choices=STATIONS, default=None)
    order.add_argument("--fail-times", type=_non_negative_int, default=None)
    order.add_argument("--poison", action="store_true")

    brk = sub.add_parser("break", help="break a station down")
    brk.add_argument("--station", choices=STATIONS, required=True)
    brk.add_argument("--run-id", required=True)
    brk.add_argument("--duration-s", type=_positive_float, default=None)

    rep = sub.add_parser("repair", help="repair a broken station")
    rep.add_argument("--station", choices=STATIONS, required=True)
    rep.add_argument("--run-id", required=True)

    bank = sub.add_parser("bank", help="bank an order's lot")
    bank.add_argument("--order-id", required=True)
    bank.add_argument("--run-id", required=True)
    bank.add_argument("--reason", choices=["capacity", "customer", "schedule"], default="customer")

    rel = sub.add_parser("release", help="release a banked lot")
    rel.add_argument("--order-id", required=True)
    rel.add_argument("--run-id", required=True)

    _add_job_args(parser)  # bare job flags without the subcommand
    return parser


_JOB_DEFAULTS = {
    "order_id": None,
    "qty": 1,
    "recipe": None,
    "unit_time_s": None,
    "visit": 1,
    "event_id": None,
    "product": None,
    "fail_times": None,
    "poison": False,
}


def build_job(args: argparse.Namespace) -> JobCommand:
    values = {**_JOB_DEFAULTS, **vars(args)}
    station = values["station"]
    unit_time_s = values["unit_time_s"]
    if unit_time_s is None and values["recipe"] is None:
        unit_time_s = 5.0  # Phase 2 fallback
    return JobCommand(
        event_id=values["event_id"] or new_event_id(),
        run_id=values["run_id"],
        line_id=LINE_ID,
        machine_id=station,
        ts=utc_now(),
        schema_version=1,
        order_id=values["order_id"] or f"o-{uuid.uuid4().hex[:8]}",
        step=station,
        visit=values["visit"],
        qty=values["qty"],
        recipe=values["recipe"],
        unit_time_s=unit_time_s,
        product=values["product"],
        fail_times=values["fail_times"],
        poison=values["poison"],
    )


def build_run_started(
    station: str,
    run_id: str,
    event_id: uuid.UUID | None = None,
    params: RunParams | None = None,
    ts: datetime | None = None,
) -> RunStarted:
    return RunStarted(
        event_id=event_id or new_event_id(),
        run_id=run_id,
        line_id=LINE_ID,
        machine_id=station,
        ts=ts or utc_now(),
        schema_version=1,
        params=params,
    )


def build_params(args: argparse.Namespace) -> RunParams | None:
    """RunParams from start-run flags; None when no flag was given."""
    fields = {
        "seed": args.seed,
        "arrival_rate": args.arrival_rate,
        "weights": args.weights,
        "k": args.k,
        "k_max": args.k_max,
        "max_orders": args.max_orders,
        "random_breakdowns": True if args.random_breakdowns else None,
    }
    given = {k: v for k, v in fields.items() if v is not None}
    return RunParams(**given) if given else None


def run_targets(station: str) -> list[tuple[str, str]]:
    """(routing_key, machine_id) pairs; generator last so scheduler/stations are ready."""
    if station == "scheduler":
        return [("scheduler.run", "scheduler")]
    if station != "all":
        return [(f"station.{station}.run", station)]
    return [
        ("scheduler.run", "scheduler"),
        *((f"station.{s}.run", s) for s in STATIONS),
        ("kpi.run", "kpi-service"),
        ("generator.run", "generator"),
    ]


def _trunc_ms(value: datetime) -> datetime:
    return value.replace(microsecond=value.microsecond // 1000 * 1000)


def build_order(args: argparse.Namespace) -> tuple[OrderLifecycle, OrderSubmitted]:
    """(created fact, submitted command) sharing one payload; ValidationError if invalid."""
    created_ts = _trunc_ms(utc_now())
    payload = {
        "order_id": args.order_id or f"o-{uuid.uuid4().hex[:8]}",
        "product": args.product,
        "qty": args.qty,
        "due_ts": _trunc_ms(created_ts + timedelta(seconds=args.due_in_s)),
        "created_ts": created_ts,
        "fail_step": args.fail_step,
        "fail_times": args.fail_times,
        "poison": args.poison,
    }
    fact = OrderLifecycle.make(
        run_id=args.run_id, machine_id=MACHINE_ID, state="created", source="manual", **payload
    )
    command = OrderSubmitted.make(
        run_id=args.run_id, machine_id=MACHINE_ID, source="manual", **payload
    )
    return fact, command


def build_break(station: str, run_id: str, duration_s: float | None = None) -> MachineBreak:
    return MachineBreak.make(run_id=run_id, machine_id=station, duration_s=duration_s)


def build_repair(station: str, run_id: str) -> MachineRepair:
    return MachineRepair.make(run_id=run_id, machine_id=station)


def build_bank(order_id: str, reason: str, run_id: str) -> LotBank:
    return LotBank.make(run_id=run_id, machine_id=SCHEDULER_ID, order_id=order_id, reason=reason)


def build_release(order_id: str, run_id: str) -> LotRelease:
    return LotRelease.make(run_id=run_id, machine_id=SCHEDULER_ID, order_id=order_id)


def control_target(args: argparse.Namespace) -> tuple[str, str]:
    """(routing_key, machine_id) for break/repair/bank/release."""
    if args.mode == "break":
        return f"station.{args.station}.break", args.station
    if args.mode == "repair":
        return f"station.{args.station}.repair", args.station
    if args.mode == "bank":
        return "scheduler.bank", SCHEDULER_ID
    if args.mode == "release":
        return "scheduler.release", SCHEDULER_ID
    raise ValueError(f"no control target for mode {args.mode!r}")


def build_control(args: argparse.Namespace) -> Command:
    if args.mode == "break":
        return build_break(args.station, args.run_id, args.duration_s)
    if args.mode == "repair":
        return build_repair(args.station, args.run_id)
    if args.mode == "bank":
        return build_bank(args.order_id, args.reason, args.run_id)
    return build_release(args.order_id, args.run_id)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(list(argv))
    if args.mode in (None, "job"):
        if not hasattr(args, "station") or not hasattr(args, "run_id"):
            parser.error("--station and --run-id are required")
        args.mode = "job"
    elif args.mode == "start-run":
        try:
            args.params = build_params(args)
        except ValidationError as exc:
            parser.error(f"invalid run parameters: {exc.errors()[0]['msg']}")
    elif args.mode == "order":
        try:
            build_order(args)
        except ValidationError as exc:
            parser.error(f"invalid order: {exc.errors()[0]['msg']}")
    return args


async def _publish(args: argparse.Namespace) -> None:
    connection = await connect(get_settings().rabbitmq_url)
    try:
        channel = await connection.channel()
        if args.mode == "start-run":
            exchange = await channel.get_exchange(EX_CONTROL)
            ts = utc_now()  # one ts so generator t0 and scheduler agree
            targets = run_targets(args.station)
            for routing_key, machine_id in targets:
                msg = build_run_started(
                    machine_id,
                    args.run_id,
                    args.event_id if len(targets) == 1 else None,
                    params=args.params,
                    ts=ts,
                )
                await publish_command(exchange, routing_key, msg)  # generator confirmed last
                print(f"run.started {routing_key} event_id={msg.event_id}")
            print(f"start-run run_id={args.run_id} targets={len(targets)}")
        elif args.mode == "order":
            created, submitted = build_order(args)
            producer = make_producer(get_settings().kafka_bootstrap)
            # Kafka "created" first, then the work order (same order as the dashboard).
            await asyncio.to_thread(
                produce_fact, producer, TOPIC_ORDER_LIFECYCLE, created, created.order_id
            )
            exchange = await channel.get_exchange(EX_WORKORDERS)
            await publish_command(exchange, INTAKE_KEY, submitted)
            print(f"order order_id={submitted.order_id} run_id={submitted.run_id}")
        elif args.mode in CONTROL_MODES:
            routing_key, _ = control_target(args)
            control = build_control(args)
            exchange = await channel.get_exchange(EX_CONTROL)
            await publish_command(exchange, routing_key, control)
            print(f"{args.mode} {routing_key} event_id={control.event_id}")
        else:
            job = build_job(args)
            exchange = await channel.get_exchange(EX_DISPATCH)
            await publish_command(exchange, f"station.{args.station}", job)
            print(f"job order_id={job.order_id} event_id={job.event_id}")
    finally:
        await connection.close()


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.mode == "job":
        build_job(args)  # validate before connecting
    elif args.mode in CONTROL_MODES:
        build_control(args)
    asyncio.run(_publish(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
