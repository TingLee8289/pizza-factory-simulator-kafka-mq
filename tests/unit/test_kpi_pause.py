"""Active-time transform: pause marks, frozen intervals, KPI equivalence (D-04)."""

import uuid
from datetime import timedelta

from pizza.common.constants import STATIONS
from pizza.kpi.fold import KpiState, fold, new_state
from pizza.kpi.frozen import active_seconds, frozen_intervals
from pizza.kpi.metrics import KpiConfig, advance
from tests.unit.kpi_fixtures import RUN, T0, at, pause_fact, resume_fact, shuffled, simulate_line

ID = [uuid.UUID(int=i + 1) for i in range(8)]


def _t(s: float):
    return T0 + timedelta(seconds=s)


# -- frozen module -----------------------------------------------------------


def test_intervals_basic_and_ignores_strays() -> None:
    marks = {
        ID[0]: (_t(1), False),  # stray resume
        ID[1]: (_t(2), True),
        ID[2]: (_t(3), True),  # double pause
        ID[3]: (_t(5), False),
        ID[4]: (_t(8), True),  # open
    }
    assert frozen_intervals(marks, _t(10)) == [(_t(2), _t(5)), (_t(8), _t(10))]
    assert frozen_intervals({}, _t(10)) == []


def test_active_seconds() -> None:
    iv = [(_t(2), _t(5))]
    assert active_seconds(_t(0), _t(10), iv) == 7.0
    assert active_seconds(_t(3), _t(4), iv) == 0.0
    assert active_seconds(_t(4), _t(8), iv) == 3.0
    assert active_seconds(_t(8), _t(4), iv) == -4.0


# -- fold ----------------------------------------------------------------------


def _marks_state(facts) -> KpiState:
    st = new_state(RUN)
    for f in facts:
        st = fold(st, f)
    return st


def test_fold_pause_idempotent_and_order_insensitive() -> None:
    facts = [pause_fact(2), resume_fact(5), pause_fact(8), resume_fact(9)]
    base = _marks_state(facts).pause_marks
    assert len(base) == 4
    for seed in range(5):
        assert _marks_state(shuffled(facts, seed)).pause_marks == base
    assert _marks_state(facts + facts).pause_marks == base


def test_fold_ignores_other_run_and_new_state_empty() -> None:
    assert new_state(RUN).pause_marks == {}
    st = _marks_state([pause_fact(1, run_id="other")])
    assert st.pause_marks == {}


def test_pause_marks_bounded() -> None:
    st = new_state(RUN)
    import pizza.kpi.fold as fold_mod

    old = fold_mod.MAX_SEEN_IDS_CAP
    fold_mod.MAX_SEEN_IDS_CAP = 3
    try:
        for i in range(10):
            st = fold(st, pause_fact(i))
    finally:
        fold_mod.MAX_SEEN_IDS_CAP = old
    assert len(st.pause_marks) == 3


# -- metrics -------------------------------------------------------------------

CFG = KpiConfig(warmup_orders=0)
ORDERS = [("o1", "margherita", 2, 0.0), ("o2", "pepperoni", 1, 5.0)]
RECIPES = {s: {"margherita": 1.0, "pepperoni": 1.0, "veggie": 1.0} for s in STATIONS}


def _gap_pair(p: float, r: float):
    """Timeline with a real gap [p, r] vs the same timeline with the gap removed."""
    sim = simulate_line(ORDERS, RECIPES, track_in_s=0.5)
    gap = r - p
    base = at(0)
    with_gap, removed = [], []
    for m in sim.messages:
        off = (m.ts - base).total_seconds()
        if off > p:
            upd = {"ts": m.ts + timedelta(seconds=gap)}
            for name in ("created_ts", "ready_ts", "due_ts"):
                v = getattr(m, name, None)
                if v is not None and (v - base).total_seconds() > p:
                    upd[name] = v + timedelta(seconds=gap)
            m_gap = m.model_copy(update=upd)
            with_gap.append(m_gap)
            removed.append(m)
        else:
            with_gap.append(m)
            removed.append(m)
    return with_gap, removed, sim


def _fields(snap):
    st = {s: (v.utilization, v.wait_mean_s, v.track_in_mean_s) for s, v in snap.stations.items()}
    return (
        snap.window_s,
        snap.throughput_per_s,
        snap.pizzas_per_s,
        snap.wip_avg,
        snap.cycle_time.mean_s,
        snap.cycle_time.p50_s,
        snap.cycle_time.p95_s,
        snap.little.l,
        snap.little.lambda_per_s,
        snap.little.w_s,
        snap.little.rel_error,
        st,
    )


def _close(a, b) -> None:
    if isinstance(a, dict):
        assert a.keys() == b.keys()
        for k in a:
            _close(a[k], b[k])
    elif isinstance(a, tuple):
        assert len(a) == len(b)
        for x, y in zip(a, b, strict=True):
            _close(x, y)
    elif a is None or b is None:
        assert a is b
    else:
        assert abs(a - b) < 1e-9


def _run(facts, as_of):
    st = new_state(RUN)
    for f in facts:
        st = fold(st, f)
    return advance(st, as_of, RECIPES, CFG)


def test_equivalence_with_gap_removed() -> None:
    p, r = 10.25, 30.25
    with_gap, removed, sim = _gap_pair(p, r)
    end = max(m.ts for m in sim.messages) + timedelta(seconds=1)
    gapped = _run([*with_gap, pause_fact(p), resume_fact(r)], end + timedelta(seconds=r - p))
    plain = _run(removed, end)
    _close(_fields(gapped), _fields(plain))
    assert gapped.little.status == plain.little.status


def test_busy_spanning_counts_only_unfrozen_part() -> None:
    sim = simulate_line([("o1", "margherita", 1, 0.0)], RECIPES, track_in_s=0.0)
    # every station busy 1s each; freeze in the middle of the first station's busy
    first = [m for m in sim.messages if getattr(m, "machine_id", "") == STATIONS[0]]
    s_ts = min(m.ts for m in first)
    p = (s_ts - at(0)).total_seconds() + 0.25
    facts = [*sim.messages, pause_fact(p), resume_fact(p + 0.5)]
    end = max(m.ts for m in sim.messages)
    gapped = _run(facts, end)
    plain = _run(sim.messages, end)
    u_g = gapped.stations[STATIONS[0]].utilization
    u_p = plain.stations[STATIONS[0]].utilization
    assert u_g is not None and u_p is not None
    # window shrinks by 0.5; busy shrinks by 0.5 -> 0.5s less busy of 0.5s less window
    busy_p = u_p * plain.window_s
    busy_g = u_g * gapped.window_s
    assert abs((busy_p - busy_g) - 0.5) < 1e-9


def test_open_pause_freezes_window() -> None:
    sim = simulate_line(ORDERS, RECIPES, track_in_s=0.5)
    p = 12.0
    facts = [m for m in sim.messages if (m.ts - at(0)).total_seconds() <= p]
    facts.append(pause_fact(p))
    at_p = _run(facts, at(p))
    later = _run(facts, at(p + 50))
    assert at_p.window_s == later.window_s
    assert at_p.throughput_per_s == later.throughput_per_s
    _close(_fields(at_p), _fields(later))


def test_pause_covering_whole_window_is_invalid_not_nan() -> None:
    st = new_state(RUN)
    for f in simulate_line([("o1", "margherita", 1, 0.0)], RECIPES).messages[:1]:
        st = fold(st, f)
    st = fold(st, pause_fact(0.0))
    snap = advance(st, at(5), RECIPES, CFG)
    assert snap.window_s is None
    assert snap.throughput_per_s is None
