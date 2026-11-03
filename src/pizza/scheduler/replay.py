"""Kafka replay to high watermark and a thread-based tail (D-14, Pitfall 5).

confluent-kafka polls block, so replay runs in a worker thread (the caller wraps
it in ``asyncio.to_thread``) and the live tail runs in its own daemon thread,
handing messages to the event loop only through run_coroutine_threadsafe.
"""

import asyncio
import threading
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any

import structlog
from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

from pizza.common.envelope import Message
from pizza.common.kafka_io import parse_or_skip

_log = structlog.get_logger()

TP = tuple[str, int]


def is_caught_up(
    positions: Mapping[TP, int],
    end_offsets: Mapping[TP, int],
    low_offsets: Mapping[TP, int] | None = None,
) -> bool:
    """True when every non-empty partition has been read up to its end offset."""
    for tp, end in end_offsets.items():
        low = (low_offsets or {}).get(tp, 0)
        if end <= low:
            continue
        if positions.get(tp, low) < end:
            return False
    return True


def _partitions(consumer: Any, topics: list[str]) -> list[TP]:
    md = consumer.list_topics(timeout=10)
    out: list[TP] = []
    for topic in topics:
        meta = md.topics.get(topic)
        if meta is None:
            continue
        out.extend((topic, p) for p in sorted(meta.partitions))
    return out


def replay_topics(
    bootstrap: str,
    topics: list[str],
    timeout_s: float,
    group_id: str = "scheduler",
    consumer_factory: Callable[[dict[str, Any]], Any] = Consumer,
) -> tuple[list[Message], dict[TP, int]]:
    """Read every topic from the beginning up to the high watermarks seen at start."""
    consumer = consumer_factory(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        tps = _partitions(consumer, topics)
        consumer.assign([TopicPartition(t, p, OFFSET_BEGINNING) for t, p in tps])
        low: dict[TP, int] = {}
        high: dict[TP, int] = {}
        for t, p in tps:
            lo, hi = consumer.get_watermark_offsets(TopicPartition(t, p), timeout=10)
            low[(t, p)], high[(t, p)] = lo, hi
        positions: dict[TP, int] = dict(low)
        messages: list[Message] = []
        deadline = time.monotonic() + timeout_s
        while not is_caught_up(positions, high, low):
            if time.monotonic() > deadline:
                raise TimeoutError(f"replay did not reach high watermarks in {timeout_s}s")
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            key = (msg.topic(), msg.partition())
            positions[key] = msg.offset() + 1
            if key in high and msg.offset() >= high[key]:
                continue  # beyond the captured watermark: belongs to the tail
            parsed = parse_or_skip(
                msg.value(), topic=msg.topic(), partition=msg.partition(), offset=msg.offset()
            )
            if parsed is not None:
                messages.append(parsed)
        return messages, high
    finally:
        consumer.close()


class KafkaTail:
    """Poll a topic from exact offsets in a thread; deliver to an async callback."""

    def __init__(
        self,
        bootstrap: str,
        group_id: str,
        start_offsets: Mapping[TP, int],
        topic: str | Sequence[str],
        callback: Callable[[Message], Awaitable[None]],
        loop: asyncio.AbstractEventLoop,
        consumer_factory: Callable[[dict[str, Any]], Any] = Consumer,
        *,
        thread_name: str = "scheduler-tail",
        commit_interval_s: float = 2.0,
    ) -> None:
        self._bootstrap = bootstrap
        self._group_id = group_id
        self._start = dict(start_offsets)
        self._topics: list[str] = [topic] if isinstance(topic, str) else list(topic)
        self._thread_name = thread_name
        self._commit_interval_s = commit_interval_s
        self._callback = callback
        self._loop = loop
        self._factory = consumer_factory
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name=self._thread_name, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=10)

    def _commit(self, consumer: Any, offsets: Mapping[TP, int], *, asynchronous: bool) -> None:
        """Best-effort informational commit so AdminClient lag is non-empty (D-08)."""
        commit = getattr(consumer, "commit", None)
        if commit is None or not offsets:
            return
        try:
            commit(
                offsets=[TopicPartition(t, p, o) for (t, p), o in offsets.items()],
                asynchronous=asynchronous,
            )
        except Exception:  # noqa: BLE001 - never stop the tail on a commit failure
            _log.exception(f"{self._thread_name.replace('-', '_')}_commit_failed")

    def _run(self) -> None:
        consumer = self._factory(
            {
                "bootstrap.servers": self._bootstrap,
                "group.id": self._group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        try:
            tps = _partitions(consumer, self._topics)
            consumer.assign(
                [TopicPartition(t, p, self._start.get((t, p), OFFSET_BEGINNING)) for t, p in tps]
            )
            # Commit numeric start offsets once so an idle caught-up tail shows lag 0.
            positions: dict[TP, int] = {
                (t, p): self._start[(t, p)]
                for t, p in tps
                if self._start.get((t, p), OFFSET_BEGINNING) >= 0
            }
            self._commit(consumer, positions, asynchronous=True)
            dirty = False
            next_commit = time.monotonic() + self._commit_interval_s
            while not self._stop.is_set():
                if dirty and time.monotonic() >= next_commit:
                    self._commit(consumer, positions, asynchronous=True)
                    dirty = False
                    next_commit = time.monotonic() + self._commit_interval_s
                msg = consumer.poll(0.5)
                if msg is None or msg.error():
                    continue
                parsed = parse_or_skip(
                    msg.value(), topic=msg.topic(), partition=msg.partition(), offset=msg.offset()
                )
                if parsed is not None:
                    try:
                        fut = asyncio.run_coroutine_threadsafe(self._callback(parsed), self._loop)
                        fut.result()  # preserve order and apply back-pressure
                    except Exception:  # noqa: BLE001 - loop closed during shutdown
                        if self._stop.is_set():
                            break
                        _log.exception(f"{self._thread_name.replace('-', '_')}_callback_failed")
                        continue
                positions[(msg.topic(), msg.partition())] = msg.offset() + 1
                dirty = True
            if positions:
                self._commit(consumer, positions, asynchronous=False)
        finally:
            consumer.close()
