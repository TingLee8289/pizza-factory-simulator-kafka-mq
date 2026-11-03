"""Unit tests for dashboard settings and the panel poller (no network)."""

import asyncio
from concurrent.futures import Future
from types import SimpleNamespace

import httpx
import pytest
from confluent_kafka import TopicPartition
from pydantic import ValidationError

from pizza.dashboard.panel import PanelPoller, compute_lag, parse_queues, summarize_rabbit
from pizza.dashboard.settings import DashboardSettings


def _settings(**kw) -> DashboardSettings:
    return DashboardSettings(_env_file=None, **kw)


def test_validators_reject_out_of_range():
    for kwargs in (
        {"panel_interval_s": 0.5},
        {"panel_interval_s": 3.0},
        {"state_interval_s": 0.1},
        {"state_interval_s": 2.0},
        {"kpi_ring_size": 601},
        {"kpi_ring_size": 0},
    ):
        with pytest.raises(ValidationError):
            _settings(**kwargs)


def test_defaults_and_secret_not_in_repr():
    s = _settings(rabbitmq_mgmt_pass="hunter2")
    assert s.panel_interval_s == 2.0
    assert s.lag_groups == ["scheduler", "kpi", "dashboard"]
    assert "hunter2" not in repr(s)
    assert "hunter2" not in str(s)


def test_parse_queues_filters_and_defaults():
    rows = [
        {"name": "workorders.intake", "messages_ready": 3, "messages_unacknowledged": 1},
        {"name": "station.bake.jobs", "messages_ready": 2},
        {"name": "some.other.queue", "messages_ready": 9},
        "garbage",
        None,
    ]
    out = parse_queues(rows)
    assert out == {"workorders.intake": (3, 1), "station.bake.jobs": (2, 0)}
    assert parse_queues({"not": "a list"}) == {}


def test_summarize_rabbit_extracts_dlq():
    rows = [{"name": "dispatch.dlq", "messages_ready": 4, "messages_unacknowledged": 0}]
    out = summarize_rabbit(rows)
    assert out["dlq"] == 4
    assert out["queues"]["dispatch.dlq"] == {"ready": 4, "unacked": 0}
    assert summarize_rabbit([])["dlq"] == 0


def test_compute_lag():
    committed = {("t", 0): 5, ("t", 1): 10, ("t", 2): -1}
    ends = {("t", 0): 8, ("t", 1): 9, ("t", 2): 100}
    assert compute_lag(committed, ends) == 3
    assert compute_lag({("t", 0): -1}, {("t", 0): 5}) is None
    assert compute_lag({}, {}) is None


class FakeAdmin:
    def __init__(self, committed=None, ends=None, fail=False):
        self.committed = committed or {}
        self.ends = ends or {}
        self.fail = fail

    @staticmethod
    def _fut(value):
        fut: Future = Future()
        fut.set_result(value)
        return fut

    def list_consumer_group_offsets(self, requests):
        if self.fail:
            raise RuntimeError("kafka down")
        group = requests[0].group_id
        tps = [TopicPartition(t, p, o) for (t, p), o in self.committed.get(group, {}).items()]
        return {group: self._fut(SimpleNamespace(topic_partitions=tps))}

    def list_offsets(self, spec):
        return {
            tp: self._fut(SimpleNamespace(offset=self.ends[(tp.topic, tp.partition)]))
            for tp in spec
        }


def _http(handler):
    return httpx.AsyncClient(
        base_url="http://rabbit.invalid", transport=httpx.MockTransport(handler)
    )


ROWS = [{"name": "dispatch.dlq", "messages_ready": 2, "messages_unacknowledged": 0}]


async def test_poll_once_ok():
    settings = _settings(lag_groups=["kpi", "scheduler"])
    admin = FakeAdmin(
        committed={"kpi": {("station.events", 0): 5}},
        ends={("station.events", 0): 9},
    )
    poller = PanelPoller(
        settings, http_client=_http(lambda r: httpx.Response(200, json=ROWS)), admin=admin
    )
    panel = await poller.poll_once()
    assert panel["error"] is False
    assert panel["as_of"]
    assert panel["rabbit"]["dlq"] == 2
    assert panel["kafka"]["lag"] == {"kpi": 4, "scheduler": None}


async def test_http_failure_retains_last_good():
    settings = _settings(lag_groups=[])
    state = {"ok": True}

    def handler(request):
        if state["ok"]:
            return httpx.Response(200, json=ROWS)
        return httpx.Response(500)

    poller = PanelPoller(settings, http_client=_http(handler), admin=FakeAdmin())
    first = await poller.poll_once()
    state["ok"] = False
    second = await poller.poll_once()
    assert second["error"] is True
    assert second["rabbit"] == first["rabbit"]


async def test_http_exception_does_not_raise_or_leak():
    settings = _settings(lag_groups=[], rabbitmq_mgmt_pass="hunter2")

    def handler(request):
        raise httpx.ConnectError("boom http://user:hunter2@rabbit")

    poller = PanelPoller(settings, http_client=_http(handler), admin=FakeAdmin())
    panel = await poller.poll_once()
    assert panel["error"] is True
    assert "hunter2" not in repr(panel)
    assert "rabbit" in panel and "invalid" not in repr(panel)


async def test_admin_failure_gives_empty_lag():
    settings = _settings(lag_groups=["kpi"])
    poller = PanelPoller(
        settings,
        http_client=_http(lambda r: httpx.Response(200, json=ROWS)),
        admin=FakeAdmin(fail=True),
    )
    panel = await poller.poll_once()
    assert panel["error"] is True
    assert panel["kafka"]["lag"] == {}
    assert panel["rabbit"]["dlq"] == 2


async def test_run_survives_callback_exception_and_stops():
    settings = _settings(lag_groups=[])
    poller = PanelPoller(
        settings, http_client=_http(lambda r: httpx.Response(200, json=ROWS)), admin=FakeAdmin()
    )
    poller.settings = SimpleNamespace(**{**settings.model_dump(), "panel_interval_s": 0.01})
    stop = asyncio.Event()
    calls: list[int] = []

    def on_update(panel):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("callback boom")
        if len(calls) >= 3:
            stop.set()

    await asyncio.wait_for(poller.run(on_update, stop), timeout=5)
    assert len(calls) >= 3
    await poller.aclose()
