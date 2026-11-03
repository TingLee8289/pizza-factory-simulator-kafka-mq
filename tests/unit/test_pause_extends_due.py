"""A pause extends every in-flight lot's effective Due (KPI tardiness + dashboard payload)."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from pizza.dashboard.state import fold, line_snapshot, new_line_state
from pizza.kpi.fold import OrderRec, new_state
from pizza.kpi.frozen import ActiveClock
from pizza.kpi.metrics import KpiConfig, _tardiness, advance
from tests.unit.dashboard_fixtures import (
    RUN,
    at,
    full_timeline,
    lifecycle,
    pause_fact,
    resume_fact,
)

T0 = datetime(2026, 1, 1, tzinfo=UTC)


def s(n: float) -> datetime:
    return T0 + timedelta(seconds=n)


# -- KPI tardiness ------------------------------------------------------------


def test_pause_only_lateness_vanishes():
    clock = ActiveClock([(s(2), s(7))])
    assert _tardiness(s(0), s(10), s(12), clock) == pytest.approx(0.0)
    assert _tardiness(s(0), s(10), s(12), [(s(2), s(7))]) == pytest.approx(0.0)


def test_no_pause_unchanged():
    assert _tardiness(s(0), s(10), s(12), ActiveClock()) == pytest.approx(2.0)


def test_pause_between_due_and_completed_not_double_deducted():
    clock = ActiveClock([(s(11), s(13))])
    # active(0,15)=13 minus unpaused(0,10)=10 -> 3 (old formula: active(10,15)=3)
    assert _tardiness(s(0), s(10), s(15), clock) == pytest.approx(3.0)


def test_kpi_pause_before_created_no_effect():
    clock = ActiveClock([(s(-10), s(-5))])
    assert _tardiness(s(0), s(10), s(12), clock) == pytest.approx(2.0)


def test_time_scale_segments_consistent():
    segs = [(s(-100), s(1000), 2.0)]
    clock = ActiveClock([(s(2), s(7))], segs)
    # active(0,12)=(12-5)*2=14; unpaused(0,10)=20 -> not late
    assert _tardiness(s(0), s(10), s(12), clock) == 0.0
    # no pause: 2 wall s late -> 4 sim s
    assert _tardiness(s(0), s(10), s(12), ActiveClock((), segs)) == pytest.approx(4.0)


def test_due_before_created_falls_back():
    assert _tardiness(s(5), s(3), s(8), ActiveClock()) == pytest.approx(5.0)


def test_advance_counts_pause_only_late_order_as_on_time():
    st = new_state(RUN)
    rec = OrderRec("margherita", 1, s(0))
    rec.due_ts = s(10)
    rec.completed_ts = s(12)
    st.orders["o1"] = rec
    st.pause_marks[uuid.UUID(int=1)] = (s(2), True)
    st.pause_marks[uuid.UUID(int=2)] = (s(7), False)
    snap = advance(st, s(20), {}, KpiConfig(warmup_orders=0))
    assert snap.due.n_completed == 1
    assert snap.due.late_count == 0


# -- dashboard payload --------------------------------------------------------


def _state(msgs):
    st = new_line_state(RUN)
    for m in msgs:
        st = fold(st, m)
    return st


def _lot(snap):
    return next(lt for lt in snap["lots"] if lt["order_id"] == "o1")


def _iso(n: float) -> str:
    return at(n).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def test_closed_pause_extends_due():
    st = _state([lifecycle("created"), pause_fact(2), resume_fact(7)])
    lot = _lot(line_snapshot(st, at(8)))
    assert lot["due_ts"] == _iso(3600)
    assert lot["due_eff_ts"] == _iso(3605)


def test_open_pause_grows_with_now():
    st = _state([lifecycle("created"), pause_fact(2)])
    assert _lot(line_snapshot(st, at(8)))["due_eff_ts"] == _iso(3606)
    assert _lot(line_snapshot(st, at(9)))["due_eff_ts"] == _iso(3607)


def test_pause_before_created_no_effect():
    st = _state([lifecycle("created"), pause_fact(-5), resume_fact(-1)])
    lot = _lot(line_snapshot(st, at(8)))
    assert lot["due_eff_ts"] == lot["due_ts"]


def test_done_lot_clamps_at_completed():
    msgs = [*full_timeline(1), pause_fact(10_000), resume_fact(10_050)]
    lot = _lot(line_snapshot(_state(msgs), at(20_000)))
    assert lot["state"] == "done"
    assert lot["due_eff_ts"] == lot["due_ts"]
