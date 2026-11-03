"""KPI simulation-time conversion: scale marks, sim_seconds, 1x vs 8x equivalence (quick-et6)."""

import uuid
from datetime import timedelta

import pytest

from pizza.common.constants import STATIONS
from pizza.common.envelope import RunStarted, TimeScaleChanged
from pizza.kpi.fold import fold, new_state
from pizza.kpi.frozen import (
    ActiveClock,
    active_seconds,
    frozen_intervals,
    scale_segments,
    sim_seconds,
)
from pizza.kpi.metrics import KpiConfig, _secs, advance
from tests.unit.kpi_fixtures import RUN, T0, at, pause_fact, resume_fact, shuffled, simulate_line
from tests.unit.test_kpi_pause import _close, _fields

ID = [uuid.UUID(int=i + 1) for i in range(8)]
CFG = KpiConfig(warmup_orders=0)
RECIPES = {s: {"margherita": 1.0, "pepperoni": 1.0, "veggie": 1.0} for s in STATIONS}
ORDERS = [("o1", "margherita", 2, 0.0), ("o2", "pepperoni", 1, 5.0), ("o3", "veggie", 3, 40.0)]


def _t(s: float):
    return T0 + timedelta(seconds=s)


def _scale(scale: int, offset_s: float) -> TimeScaleChanged:
    return TimeScaleChanged.make(
        run_id="unknown", machine_id="dashboard", scale=scale, ts=at(offset_s)
    )


# -- pure functions -----------------------------------------------------------


def test_no_segments_equals_active_seconds() -> None:
    iv = [(_t(2), _t(5))]
    for a, b in ((0, 10), (3, 4), (4, 8), (8, 4)):
        assert sim_seconds(_t(a), _t(b), iv, []) == active_seconds(_t(a), _t(b), iv)


def test_single_segment_eight_x() -> None:
    segs = scale_segments({ID[0]: (_t(0), 8.0)}, _t(100))
    assert sim_seconds(_t(0), _t(10), [], segs) == pytest.approx(80)


def test_two_segments_and_pre_mark_is_1x() -> None:
    marks = {ID[0]: (_t(10), 1.0), ID[1]: (_t(15), 4.0)}
    segs = scale_segments(marks, _t(100))
    # [10,15] at 1x = 5, [15,20] at 4x = 20
    assert sim_seconds(_t(10), _t(20), [], segs) == pytest.approx(25)
    # [0,10] before the first mark counts 1x
    assert sim_seconds(_t(0), _t(20), [], segs) == pytest.approx(35)
    # beyond the last mark stays 4x
    assert sim_seconds(_t(15), _t(25), [], segs) == pytest.approx(40)


def test_frozen_overlap_not_counted() -> None:
    segs = scale_segments({ID[0]: (_t(0), 8.0)}, _t(100))
    assert sim_seconds(_t(0), _t(10), [(_t(2), _t(4))], segs) == pytest.approx(64)


def test_reverse_interval_keeps_raw_difference() -> None:
    segs = scale_segments({ID[0]: (_t(0), 8.0)}, _t(100))
    assert sim_seconds(_t(8), _t(4), [], segs) == -4.0


def test_segments_ignore_marks_after_as_of_and_order() -> None:
    marks = {ID[0]: (_t(10), 2.0), ID[1]: (_t(50), 16.0)}
    assert scale_segments(marks, _t(20)) == scale_segments({ID[0]: (_t(10), 2.0)}, _t(20))
    rev = dict(reversed(list(marks.items())))
    assert scale_segments(marks, _t(60)) == scale_segments(rev, _t(60))


def test_active_clock_matches_legacy_secs() -> None:
    iv = [(_t(2), _t(5))]
    assert _secs(_t(0), _t(10), iv) == _secs(_t(0), _t(10), ActiveClock(iv, ()))


def test_frozen_helper_still_importable_for_legacy_callers() -> None:
    assert frozen_intervals({}, _t(1)) == []


# -- fold ---------------------------------------------------------------------


def test_fold_scale_global_idempotent_order_insensitive() -> None:
    facts = [_scale(2, 1), _scale(8, 5), _scale(1, 9)]
    base = None
    for seed in range(5):
        st = new_state(RUN)
        for f in shuffled(facts, seed):
            st = fold(st, f)
        assert len(st.scale_marks) == 3
        base = base or dict(st.scale_marks)
        assert st.scale_marks == base
    st = new_state(RUN)
    for f in facts + facts:
        st = fold(st, f)
    assert len(st.scale_marks) == 3
    assert st.last_event_ts is None  # does not pollute the run range


def test_scale_marks_survive_run_start() -> None:
    st = fold(new_state(RUN), _scale(8, 0))
    st = fold(st, RunStarted.make(run_id="r2", machine_id="cli", ts=at(1)))
    assert st.run_id == "r2" and len(st.scale_marks) == 1


# -- metrics ------------------------------------------------------------------


def _compress(messages, factor: int):
    base = at(0)
    out = []
    for m in messages:
        upd = {"ts": base + (m.ts - base) / factor}
        for name in ("created_ts", "ready_ts", "due_ts"):
            v = getattr(m, name, None)
            if v is not None:
                upd[name] = base + (v - base) / factor
        out.append(m.model_copy(update=upd))
    return out


def _snapshot(facts, as_of):
    st = new_state(RUN)
    for f in facts:
        st = fold(st, f)
    return advance(st, as_of, RECIPES, CFG)


def test_8x_kpis_equal_1x() -> None:
    sim = simulate_line(ORDERS, RECIPES, track_in_s=0.5)
    end = max(m.ts for m in sim.messages) + timedelta(seconds=8)
    one = _snapshot(sim.messages, end)
    fast = _snapshot([*_compress(sim.messages, 8), _scale(8, -1.0)], at(0) + (end - at(0)) / 8)
    _close(_fields(one), _fields(fast))
    assert one.little.status == fast.little.status
    assert one.orders_completed == fast.orders_completed > 0


def test_8x_equals_1x_with_pause_and_shuffle() -> None:
    sim = simulate_line(ORDERS, RECIPES, track_in_s=0.5)
    end = max(m.ts for m in sim.messages) + timedelta(seconds=8)
    base = [*sim.messages, pause_fact(10.0), resume_fact(12.0)]
    one = _snapshot(base, end)
    fast_facts = [*_compress(base, 8), _scale(8, -1.0)]
    fast = _snapshot(shuffled(fast_facts, 3), at(0) + (end - at(0)) / 8)
    _close(_fields(one), _fields(fast))
