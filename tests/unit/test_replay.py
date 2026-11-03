"""Unit tests for scheduler replay helpers (no broker)."""

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from pizza.common.envelope import OrderLifecycle, encode
from pizza.scheduler import replay
from pizza.scheduler.replay import is_caught_up, replay_topics
from pizza.scheduler.settings import SchedulerSettings


def test_is_caught_up_rules() -> None:
    end = {("t", 0): 5, ("t", 1): 0}
    assert not is_caught_up({("t", 0): 3}, end, {("t", 0): 0, ("t", 1): 0})
    assert is_caught_up({("t", 0): 5}, end, {("t", 0): 0, ("t", 1): 0})
    # empty range counts as caught up
    assert is_caught_up({}, {("t", 0): 4}, {("t", 0): 4})


def _fact(i: int) -> bytes:
    ts = datetime(2026, 1, 1, tzinfo=UTC)
    return encode(
        OrderLifecycle.make(
            run_id="r",
            machine_id="scheduler",
            state="released",
            order_id=f"o{i}",
            product="margherita",
            qty=1,
            due_ts=ts,
            created_ts=ts,
        )
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
    def __init__(self, msgs: list[FakeMsg], low: int, high: int) -> None:
        self.msgs = list(msgs)
        self.low, self.high = low, high
        self.closed = False
        self.polls = 0

    def list_topics(self, timeout=None):
        return SimpleNamespace(topics={"t": SimpleNamespace(partitions={0: None})})

    def assign(self, tps):
        self.assigned = tps

    def get_watermark_offsets(self, tp, timeout=None):
        return self.low, self.high

    def poll(self, timeout=None):
        self.polls += 1
        return self.msgs.pop(0) if self.msgs else None

    def close(self):
        self.closed = True


def test_replay_stops_at_high_watermark_and_skips_invalid() -> None:
    msgs = [
        FakeMsg("t", 0, 0, _fact(0)),
        FakeMsg("t", 0, 1, b"garbage"),
        FakeMsg("t", 0, 2, _fact(2)),
        FakeMsg("t", 0, 3, _fact(3)),  # beyond watermark
    ]
    fake = FakeConsumer(msgs, 0, 3)
    out, ends = replay_topics("x", ["t"], 5.0, consumer_factory=lambda cfg: fake)
    assert [m.order_id for m in out] == ["o0", "o2"]
    assert ends == {("t", 0): 3}
    assert fake.closed
    assert len(fake.msgs) == 1


def test_replay_empty_returns_immediately() -> None:
    fake = FakeConsumer([], 0, 0)
    out, ends = replay_topics("x", ["t"], 5.0, consumer_factory=lambda cfg: fake)
    assert out == []
    assert ends == {("t", 0): 0}
    assert fake.polls == 0


def test_replay_timeout() -> None:
    fake = FakeConsumer([], 0, 2)
    with pytest.raises(TimeoutError):
        replay_topics("x", ["t"], 0.0, consumer_factory=lambda cfg: fake)


def test_settings_defaults() -> None:
    s = SchedulerSettings()
    assert s.nack_delay_s == 1.0
    assert s.dispatch_retry_s == 1.0
    assert s.intake_defer_max == 10
    assert s.resume_on_start is True
    assert s.replay_timeout_s == 60.0
    assert s.kafka_group_id == "scheduler"


def test_module_exports() -> None:
    assert hasattr(replay, "KafkaTail")


def test_kafka_tail_multi_topic_assigns_both_and_delivers() -> None:
    import asyncio

    class TwoTopicConsumer(FakeConsumer):
        def list_topics(self, timeout=None):
            return SimpleNamespace(
                topics={
                    "a": SimpleNamespace(partitions={0: None}),
                    "b": SimpleNamespace(partitions={0: None}),
                }
            )

    async def scenario() -> tuple[list[str], list]:
        loop = asyncio.get_running_loop()
        got: list[str] = []
        done = asyncio.Event()
        fake = TwoTopicConsumer([FakeMsg("b", 0, 5, _fact(7))], 0, 0)

        async def cb(msg) -> None:
            got.append(msg.order_id)
            done.set()

        tail = replay.KafkaTail(
            "x",
            "g",
            {("b", 0): 5},
            ["a", "b"],
            cb,
            loop,
            consumer_factory=lambda cfg: fake,
            thread_name="kpi-tail",
        )
        tail.start()
        await asyncio.wait_for(done.wait(), 5)
        await asyncio.to_thread(tail.stop)
        return got, fake.assigned

    got, assigned = asyncio.run(scenario())
    assert got == ["o7"]
    assert sorted((tp.topic, tp.partition) for tp in assigned) == [("a", 0), ("b", 0)]
    assert {tp.topic: tp.offset for tp in assigned}["b"] == 5


class CommitConsumer(FakeConsumer):
    def __init__(self, *a, fail: bool = False, **kw) -> None:
        super().__init__(*a, **kw)
        self.commits: list[tuple[list[tuple[int, int]], bool]] = []
        self.fail = fail

    def commit(self, offsets=None, asynchronous=True):
        self.commits.append(([(tp.partition, tp.offset) for tp in offsets], asynchronous))
        if self.fail:
            raise RuntimeError("boom")


def _run_tail(fake, start, *, interval: float, n_expected: int = 0):
    import asyncio

    async def scenario() -> list[str]:
        loop = asyncio.get_running_loop()
        got: list[str] = []
        done = asyncio.Event()

        async def cb(msg) -> None:
            got.append(msg.order_id)
            if len(got) >= n_expected:
                done.set()

        tail = replay.KafkaTail(
            "x",
            "g",
            start,
            "t",
            cb,
            loop,
            consumer_factory=lambda cfg: fake,
            commit_interval_s=interval,
        )
        tail.start()
        if n_expected:
            await asyncio.wait_for(done.wait(), 5)
            await asyncio.sleep(0.2)  # let one interval elapse (poll returns None)
        else:
            await asyncio.sleep(0.2)
        await asyncio.to_thread(tail.stop)
        return got

    return asyncio.run(scenario())


def test_tail_commit_on_interval_and_at_stop() -> None:
    fake = CommitConsumer([FakeMsg("t", 0, 5, _fact(1)), FakeMsg("t", 0, 6, _fact(2))], 0, 0)
    got = _run_tail(fake, {("t", 0): 5}, interval=0.0, n_expected=2)
    assert got == ["o1", "o2"]
    assert fake.commits[0] == ([(0, 5)], True)  # startup commit
    assert ([(0, 7)], True) in fake.commits  # interval commit
    assert fake.commits[-1] == ([(0, 7)], False)  # final synchronous commit


def test_tail_startup_commit_skips_beginning() -> None:
    fake = CommitConsumer([], 0, 0)
    _run_tail(fake, {}, interval=100.0)
    assert fake.commits == []
    fake = CommitConsumer([], 0, 0)
    _run_tail(fake, {("t", 0): 3}, interval=100.0)
    assert fake.commits[0] == ([(0, 3)], True)


def test_tail_no_commit_attribute_still_works() -> None:
    fake = FakeConsumer([FakeMsg("t", 0, 0, _fact(1))], 0, 0)
    assert _run_tail(fake, {("t", 0): 0}, interval=0.0, n_expected=1) == ["o1"]
    assert fake.closed


def test_tail_commit_failure_does_not_stop_tail() -> None:
    fake = CommitConsumer(
        [FakeMsg("t", 0, 0, _fact(1)), FakeMsg("t", 0, 1, _fact(2))], 0, 0, fail=True
    )
    assert _run_tail(fake, {("t", 0): 0}, interval=0.0, n_expected=2) == ["o1", "o2"]
    assert fake.closed
