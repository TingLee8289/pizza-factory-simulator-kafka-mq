import dataclasses

import pytest

from pizza.common.constants import STATIONS
from pizza.kpi.bottleneck import BottleneckCfg, BottleneckState, update_bottleneck

CFG = BottleneckCfg(margin=0.05, confirm_n=3, min_util=0.80)


def util(**kw):
    d = dict.fromkeys(STATIONS, 0.1)
    d.update(kw)
    return d


def run(series, start=None):
    st = start or BottleneckState()
    for u in series:
        st = update_bottleneck(st, u, CFG)
    return st


BAKE = BottleneckState(bottleneck="bake")


def test_within_margin_no_switch():
    st = run([util(bake=0.9, toppings=0.93)] * 5 + [util(bake=0.9, toppings=0.5)], BAKE)
    assert st.bottleneck == "bake"
    assert st.candidate is None and st.candidate_count == 0


def test_two_ticks_no_switch_and_counter_resets():
    hi = util(bake=0.90, toppings=0.97)
    st = run([hi, hi], BAKE)
    assert st.bottleneck == "bake" and st.candidate == "toppings" and st.candidate_count == 2
    st = run([util(bake=0.88, toppings=0.80)], st)
    assert st.bottleneck == "bake" and st.candidate_count == 0


def test_three_ticks_switch():
    hi = util(bake=0.90, toppings=0.97)
    st = run([hi, hi, hi], BAKE)
    assert st.bottleneck == "toppings"
    assert st.candidate is None and st.candidate_count == 0


def test_alternating_challengers_never_accumulate():
    a = util(bake=0.80, toppings=0.95)
    b = util(bake=0.80, slice=0.95)
    st = run([a, b, a, b, a, b], BAKE)
    assert st.bottleneck == "bake"
    assert st.candidate_count == 1


def test_below_min_util_keeps_incumbent():
    st = run([util(bake=0.7)], BottleneckState("bake", "toppings", 2))
    assert st == BottleneckState("bake")


def test_below_min_util_without_incumbent_is_empty():
    st = run([util(bake=0.7)], BottleneckState(None, "bake", 2))
    assert st == BottleneckState()


def test_dip_between_challenger_ticks_resets_count():
    hi = util(bake=0.90, toppings=0.97)
    st = run([hi, hi, dict.fromkeys(STATIONS, 0.1)], BAKE)
    assert st == BottleneckState("bake")
    st = run([hi], st)
    assert st.bottleneck == "bake" and st.candidate_count == 1
    st = run([hi, hi], st)
    assert st.bottleneck == "toppings"


def test_first_adoption_needs_confirm_n():
    u = util(bake=0.9)
    st = run([u, u])
    assert st.bottleneck is None and st.candidate == "bake" and st.candidate_count == 2
    st = run([u], st)
    assert st.bottleneck == "bake"


def test_first_adoption_candidate_change_resets():
    st = run([util(bake=0.9), util(bake=0.9), util(toppings=0.9)])
    assert st.bottleneck is None and st.candidate == "toppings" and st.candidate_count == 1


def test_none_utils_and_ties():
    assert run([dict.fromkeys(STATIONS)]).bottleneck is None
    tie = util(spin=0.9, bake=0.9)
    assert run([tie] * 3).bottleneck == "spin"


def test_missing_incumbent_util_satisfies_gap():
    u = util(toppings=0.9)
    u["bake"] = None
    assert run([u] * 3, BAKE).bottleneck == "toppings"


def test_immutability():
    prev = BottleneckState("bake")
    u = util(bake=0.9, toppings=0.97)
    new = update_bottleneck(prev, u, CFG)
    assert prev == BottleneckState("bake")
    assert new is not prev
    with pytest.raises(dataclasses.FrozenInstanceError):
        new.bottleneck = "x"  # type: ignore[misc]
