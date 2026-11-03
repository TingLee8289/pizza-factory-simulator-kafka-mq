"""Declarative broker topology (D-12, D-14).

This module is data only: it performs no broker I/O and is the single source
of truth for every Kafka topic, RabbitMQ exchange, queue and binding.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from pizza.common.constants import STATIONS

RETRY_TTL_MS = 5000
"""Retry delay in ms.

WARNING: this is baked into the retry queues' ``x-message-ttl`` argument.
Quorum queue arguments cannot be changed in place, so changing this value
requires deleting and recreating the retry queues (D-14).

The per-station rework hold queues (``scheduler.rework.<station>.hold``) bake
the same TTL. The retry queues remain the jobs-queue crash/delivery-limit path.
"""

KAFKA_RETENTION_MS = 2_592_000_000  # 30 days; D-12 requires >= 7 days.

TOPIC_STATION_EVENTS = "station.events"  # message key = machine_id
TOPIC_ORDER_LIFECYCLE = "order.lifecycle"  # message key = order_id
TOPIC_WIP_SNAPSHOTS = "wip.snapshots"
TOPIC_KPI_SNAPSHOTS = "kpi.snapshots"
# Simulation speed changes, key "line". cleanup.policy=delete (not compact): the KPI
# service replays the whole scale history to convert old wall intervals to sim seconds.
TOPIC_TIMESCALE = "sim.timescale"

EX_WORKORDERS = "workorders"
EX_DISPATCH = "dispatch"
EX_DISPATCH_RETRY = "dispatch.retry"
EX_DLX = "dlx"
EX_CONTROL = "control"
EX_SCHEDULER_REWORK = "scheduler.rework"

Q_WORKORDERS_INTAKE = "workorders.intake"
Q_DISPATCH_DLQ = "dispatch.dlq"
Q_SCHEDULER_CONTROL = "scheduler.control"
Q_GENERATOR_CONTROL = "generator.control"
Q_KPI_CONTROL = "kpi.control"
Q_REWORK_RETURN = "scheduler.rework.return"

# Routing keys on the topic exchange EX_CONTROL. The scheduler control
# queue binds ``scheduler.#`` so ``scheduler.priority`` (Phase 07.2 D-11
# LotSetPriority) needs no new binding; the constant is the single source
# of truth for the string.
ROUTING_SCHEDULER_PRIORITY = "scheduler.priority"


def jobs_queue_name(station: str) -> str:
    return f"station.{station}.jobs"


def retry_queue_name(station: str) -> str:
    return f"station.{station}.retry"


def rework_hold_queue_name(station: str) -> str:
    return f"scheduler.rework.{station}.hold"


def control_queue_name(station: str) -> str:
    return f"station.{station}.control"


@dataclass(frozen=True)
class TopicSpec:
    """Kafka topic; replication factor is fixed at 1 (single broker)."""

    name: str
    partitions: int
    config: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ExchangeSpec:
    """Durable RabbitMQ exchange; type is direct, fanout or topic."""

    name: str
    type: str


@dataclass(frozen=True)
class QueueSpec:
    """Durable quorum queue with (exchange, routing_key) bindings."""

    name: str
    arguments: Mapping[str, int | str]
    bindings: tuple[tuple[str, str], ...]


def _ro(d: dict[str, str]) -> Mapping[str, str]:
    return MappingProxyType(d)


_DELETE_CFG = {"cleanup.policy": "delete", "retention.ms": str(KAFKA_RETENTION_MS)}
_COMPACT_CFG = {"cleanup.policy": "compact"}

KAFKA_TOPICS: tuple[TopicSpec, ...] = (
    TopicSpec(TOPIC_STATION_EVENTS, 3, _ro(_DELETE_CFG)),
    TopicSpec(TOPIC_ORDER_LIFECYCLE, 3, _ro(_DELETE_CFG)),
    TopicSpec(TOPIC_WIP_SNAPSHOTS, 1, _ro(_COMPACT_CFG)),
    TopicSpec(TOPIC_KPI_SNAPSHOTS, 1, _ro(_COMPACT_CFG)),
    TopicSpec(TOPIC_TIMESCALE, 1, _ro(_DELETE_CFG)),
)

EXCHANGES: tuple[ExchangeSpec, ...] = (
    ExchangeSpec(EX_WORKORDERS, "direct"),
    ExchangeSpec(EX_DISPATCH, "direct"),
    ExchangeSpec(EX_DISPATCH_RETRY, "direct"),
    ExchangeSpec(EX_DLX, "fanout"),
    ExchangeSpec(EX_CONTROL, "topic"),
    ExchangeSpec(EX_SCHEDULER_REWORK, "direct"),
)

_QUORUM = {"x-queue-type": "quorum"}


def _jobs_queue(s: str) -> QueueSpec:
    return QueueSpec(
        jobs_queue_name(s),
        {
            **_QUORUM,
            "x-dead-letter-exchange": EX_DISPATCH_RETRY,
            "x-dead-letter-routing-key": f"station.{s}",
        },
        ((EX_DISPATCH, f"station.{s}"),),
    )


def _retry_queue(s: str) -> QueueSpec:
    return QueueSpec(
        retry_queue_name(s),
        {
            **_QUORUM,
            "x-message-ttl": RETRY_TTL_MS,
            "x-dead-letter-exchange": EX_DISPATCH,
            "x-dead-letter-routing-key": f"station.{s}",
        },
        ((EX_DISPATCH_RETRY, f"station.{s}"),),
    )


def _rework_hold_queue(s: str) -> QueueSpec:
    return QueueSpec(
        rework_hold_queue_name(s),
        {
            **_QUORUM,
            "x-message-ttl": RETRY_TTL_MS,
            "x-dead-letter-exchange": EX_SCHEDULER_REWORK,
            "x-dead-letter-routing-key": "return",
        },
        ((EX_SCHEDULER_REWORK, f"hold.{s}"),),
    )


def _control_queue(s: str) -> QueueSpec:
    return QueueSpec(control_queue_name(s), dict(_QUORUM), ((EX_CONTROL, f"station.{s}.#"),))


QUEUES: tuple[QueueSpec, ...] = (
    QueueSpec(Q_WORKORDERS_INTAKE, dict(_QUORUM), ((EX_WORKORDERS, "intake"),)),
    *(_jobs_queue(s) for s in STATIONS),
    *(_retry_queue(s) for s in STATIONS),
    *(_rework_hold_queue(s) for s in STATIONS),
    QueueSpec(Q_REWORK_RETURN, dict(_QUORUM), ((EX_SCHEDULER_REWORK, "return"),)),
    QueueSpec(Q_DISPATCH_DLQ, dict(_QUORUM), ((EX_DLX, ""),)),
    QueueSpec(Q_SCHEDULER_CONTROL, dict(_QUORUM), ((EX_CONTROL, "scheduler.#"),)),
    QueueSpec(Q_GENERATOR_CONTROL, dict(_QUORUM), ((EX_CONTROL, "generator.#"),)),
    QueueSpec(Q_KPI_CONTROL, dict(_QUORUM), ((EX_CONTROL, "kpi.#"),)),
    *(_control_queue(s) for s in STATIONS),
)
