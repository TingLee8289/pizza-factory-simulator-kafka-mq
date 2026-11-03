from datetime import UTC, datetime, timedelta

from pizza.scheduler.stocker import (
    classify_backlog,
    due_for_down_bank,
    should_wait_bank,
    wait_release_count,
)


def test_overflow():
    ids = ["a", "b", "c", "d"]
    assert classify_backlog(ids, "ok", 2) == {"c": "overflow", "d": "overflow"}
    assert classify_backlog(ids, "ok", None) == {}
    assert classify_backlog(["a"], "ok", 2) == {}


def test_down_offline():
    ids = ["a", "b"]
    assert classify_backlog(ids, "down", 1) == {"a": "station_down", "b": "station_down"}
    assert classify_backlog(ids, "offline", 8) == {
        "a": "station_offline",
        "b": "station_offline",
    }


def test_due_for_down_bank():
    now = datetime(2026, 1, 1, tzinfo=UTC)
    ids = ["a", "b"]
    assert due_for_down_bank(ids, now - timedelta(seconds=60), now, 60) == ids
    assert due_for_down_bank(ids, now - timedelta(seconds=59), now, 60) == []
    assert due_for_down_bank(ids, None, now, 60) == []


def test_wait_bank():
    assert should_wait_bank(5, None) is False
    assert should_wait_bank(8, 8) is True
    assert should_wait_bank(7, 8) is False


def test_release_count_deterministic():
    assert wait_release_count(5, 6, 8) == 2
    assert wait_release_count(1, 0, 8) == 1
    assert wait_release_count(5, 9, 8) == 0
    assert wait_release_count(5, 0, None) == 0
    assert wait_release_count(5, 6, 8) == wait_release_count(5, 6, 8)
