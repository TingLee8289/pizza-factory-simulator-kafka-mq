"""Unit tests for the pure ETA recurrence (Phase 7.1, D-01)."""

import math
import re
from pathlib import Path

import pytest

import pizza.scheduler.eta as eta_mod
from pizza.common.constants import STATIONS
from pizza.common.recipes import theoretical_total_s
from pizza.scheduler.eta import (
    MAX_ETA_SIM_S,
    EtaEstimate,
    build_estimate,
    finish_offsets,
)

PROC = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0]
ZEROS = [0.0] * 7


def test_empty_line_cumulative_sums():
    assert finish_offsets(ZEROS, PROC) == [10, 30, 60, 100, 150, 210, 280]


def test_empty_line_lead_equals_theoretical_total():
    recipes = {s: {"margherita": 5.0 + i * 3.0} for i, s in enumerate(STATIONS)}
    qty = 3
    proc = [recipes[s]["margherita"] * qty for s in STATIONS]
    est = build_estimate([0.0] * len(STATIONS), proc)
    assert est.lead_sim_s == pytest.approx(theoretical_total_s(recipes, "margherita", qty))
    assert est.degraded is False


def test_bottleneck_dominates():
    proc = [10.0, 10.0, 10.0, 10.0, 100.0, 10.0, 10.0]
    out = finish_offsets(ZEROS, proc)
    assert out[:4] == [10, 20, 30, 40]
    assert out[4] == 140
    assert out[5] == 150
    assert out[6] == 160


def test_queue_wait_and_max_semantics():
    proc = [10.0] * 7
    free = [0.0, 0.0, 0.0, 0.0, 500.0, 0.0, 0.0]
    out = finish_offsets(free, proc)
    assert out[3] == 40
    assert out[4] == 510
    assert out[5] == 520
    assert out[6] == 530
    # free_at smaller than prior finish has no effect
    free2 = [0.0, 5.0, 0.0, 0.0, 0.0, 0.0, 0.0]
    assert finish_offsets(free2, proc) == [10, 20, 30, 40, 50, 60, 70]


def test_start_index():
    out = finish_offsets(ZEROS, PROC, start_index=3)
    assert out[:3] == [None, None, None]
    assert out[3:] == [40, 90, 150, 220]
    assert len(out) == 7


def test_length_mismatch_raises():
    with pytest.raises(ValueError):
        finish_offsets([0.0] * 6, PROC)


def test_non_finite_clamped_and_degraded():
    proc = list(PROC)
    proc[2] = math.inf
    out = finish_offsets(ZEROS, proc)
    assert out[2] == MAX_ETA_SIM_S
    assert out[6] == MAX_ETA_SIM_S
    est = build_estimate(ZEROS, proc)
    assert est.degraded is True
    assert est.lead_sim_s == MAX_ETA_SIM_S
    nan_free = list(ZEROS)
    nan_free[0] = math.nan
    assert build_estimate(nan_free, PROC).degraded is True


def test_negative_treated_as_zero():
    proc = [-5.0] + PROC[1:]
    out = finish_offsets([-3.0] + ZEROS[1:], proc)
    assert out[0] == 0
    assert out[1] == 20


def test_caller_degraded_flag_and_estimate_shape():
    est = build_estimate(ZEROS, PROC, degraded=True)
    assert isinstance(est, EtaEstimate)
    assert est.degraded is True
    assert est.finish_sim_s[-1] == est.lead_sim_s == 280


def test_estimate_with_start_index_lead_is_last():
    est = build_estimate(ZEROS, PROC, start_index=5)
    assert est.finish_sim_s[:5] == (None,) * 5
    assert est.lead_sim_s == 130


def test_module_is_pure_by_source_inspection():
    src = Path(eta_mod.__file__).read_text(encoding="utf-8")
    imports = re.findall(r"^\s*(?:import|from)\s+(\S+)", src, flags=re.MULTILINE)
    banned = ("aio_pika", "confluent_kafka", "asyncio", "pizza.scheduler.core")
    for name in imports:
        assert not any(name == b or name.startswith(b + ".") for b in banned), name
