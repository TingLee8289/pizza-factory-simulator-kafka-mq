"""Unit-test defaults: never open a real Kafka consumer for the time-scale feed."""

import pytest

from pizza.common import timescale


@pytest.fixture(autouse=True)
def _no_timescale_feed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(timescale, "FEED_ENABLED", False)
