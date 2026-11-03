"""Builders for dashboard state/hub tests (not a test module).

Reuses the read-only tandem-line builder from ``kpi_fixtures``; pause/resume
facts are constructed directly from the envelope classes.
"""

from datetime import datetime

from pizza.common.envelope import (
    Message,
    OpCompleted,
    OpDispatched,
    OpStarted,
    OrderLifecycle,
    RunPaused,
    RunResumed,
    StationStatus,
)
from tests.unit.kpi_fixtures import RUN, T0, at, shuffled, simulate_line, with_duplicates

__all__ = [
    "RUN",
    "T0",
    "at",
    "full_timeline",
    "lifecycle",
    "pause_fact",
    "resume_fact",
    "shuffled",
    "status",
    "with_duplicates",
]


def full_timeline(n: int, *, run_id: str = RUN, t0: datetime = T0) -> list[Message]:
    """All facts of ``n`` lots through the seven stations (distinct ts per status)."""
    products = ("margherita", "pepperoni", "veggie")
    orders = [(f"o{i + 1}", products[i % 3], 1 + i % 3, 20.0 * i) for i in range(n)]
    return simulate_line(orders, run_id=run_id, t0=t0, track_in_s=0.5).messages


def lifecycle(
    state: str,
    order_id: str = "o1",
    *,
    run_id: str = RUN,
    offset: float = 0.0,
    product: str = "margherita",
    qty: int = 1,
    priority: int = 50,
    source: str | None = None,
) -> OrderLifecycle:
    return OrderLifecycle.make(
        run_id=run_id,
        machine_id="scheduler",
        ts=at(offset),
        state=state,
        order_id=order_id,
        product=product,
        qty=qty,
        due_ts=at(3600.0),
        created_ts=at(0.0),
        priority=priority,
        source=source,
    )


def op(cls: type, order_id: str, step: str, offset: float, **extra: object) -> Message:
    import uuid

    fields: dict[str, object] = {
        "order_id": order_id,
        "step": step,
        "visit": 1,
        "qty": 1,
        "product": "margherita",
    }
    machine = "scheduler"
    if cls is OpDispatched:
        fields["command_event_id"] = uuid.UUID(int=hash((order_id, step)) & ((1 << 128) - 1))
        fields["ready_ts"] = at(offset)
    else:
        machine = step
        fields["attempt"] = 1
        fields["command_event_id"] = uuid.UUID(int=hash((order_id, step)) & ((1 << 128) - 1))
        if cls is OpStarted:
            fields["redelivered"] = False
        elif cls is OpCompleted:
            fields["duration_s"] = 1.0
    fields.update(extra)
    return cls.make(run_id=RUN, machine_id=machine, ts=at(offset), **fields)


def status(station: str, value: str, offset: float, *, run_id: str = RUN) -> StationStatus:
    return StationStatus.make(run_id=run_id, machine_id=station, ts=at(offset), status=value)


def pause_fact(offset: float, *, run_id: str = RUN) -> RunPaused:
    return RunPaused.make(run_id=run_id, machine_id="scheduler", ts=at(offset))


def resume_fact(offset: float, *, run_id: str = RUN) -> RunResumed:
    return RunResumed.make(run_id=run_id, machine_id="scheduler", ts=at(offset))
