"""Replay recorder: rebuilds the completed set from station.events (D-12)."""

import argparse
import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal

from confluent_kafka import Consumer

from pizza.common.config import get_settings
from pizza.common.dedupe import Deduper
from pizza.common.envelope import Message, OpCompleted
from pizza.common.kafka_io import parse_or_skip
from pizza.common.topology import TOPIC_STATION_EVENTS


@dataclass
class RecordResult:
    status: Literal["RECORDED", "DUPLICATE"]
    order_id: str
    step: str
    visit: int
    command_event_id: uuid.UUID
    run_id: str


def record(
    events: Iterable[Message], deduper: Deduper, run_id: str | None = None
) -> list[RecordResult]:
    results: list[RecordResult] = []
    for ev in events:
        if not isinstance(ev, OpCompleted):
            continue
        if run_id is not None and ev.run_id != run_id:
            continue
        status: Literal["RECORDED", "DUPLICATE"] = (
            "RECORDED" if deduper.first_time_op(ev) else "DUPLICATE"
        )
        results.append(
            RecordResult(status, ev.order_id, ev.step, ev.visit, ev.command_event_id, ev.run_id)
        )
    return results


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m pizza.recorder")
    p.add_argument("--run-id", default=None)
    p.add_argument("--idle-timeout-s", type=float, default=0.0)
    p.add_argument("--bootstrap", default=None)
    args = p.parse_args(argv)

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap or get_settings().kafka_bootstrap,
            "group.id": f"recorder-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    deduper = Deduper()
    consumer.subscribe([TOPIC_STATION_EVENTS])
    last_msg = time.monotonic()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                if args.idle_timeout_s and time.monotonic() - last_msg > args.idle_timeout_s:
                    break
                continue
            last_msg = time.monotonic()
            parsed = parse_or_skip(
                msg.value(), topic=msg.topic(), partition=msg.partition(), offset=msg.offset()
            )
            if parsed is None:
                continue
            for r in record([parsed], deduper, args.run_id):
                label = "RECORDED" if r.status == "RECORDED" else "DUPLICATE ignored"
                print(
                    f"{label} order={r.order_id} step={r.step} visit={r.visit} "
                    f"command_event_id={r.command_event_id}"
                )
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
