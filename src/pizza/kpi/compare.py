"""Pure KPI comparison module: per-round summary and replay verification (KPI-06, COMP-02, COMP-03).

``summarize_round`` produces a comparison table row (RoundSummary).
``recompute`` rebuilds a KPI snapshot from facts with stored parameters.
``compare_fields`` compares two snapshots field-by-field with tolerance and exclusions.

All functions are pure: no I/O, no clock, no randomness. Wall-clock time is passed in.
Pause/resume intervals are excluded from durations, making makespan "active time".

Tolerance and field list are documented constants so the UI can report them.
"""

import dataclasses
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pizza.common.envelope import KpiSnapshot, Message, TimeScaleChanged
from pizza.kpi.fold import KpiState, apply_run_params, fold, new_state
from pizza.kpi.frozen import ActiveClock, frozen_intervals, scale_segments
from pizza.kpi.metrics import KpiConfig, _secs, advance

# Tolerance for float comparison: 1e-6 absolute or relative.
DEFAULT_TOLERANCE = 1e-6


@dataclass(frozen=True)
class RoundSummary:
    """Per-round comparison table row.

    Captures the essential KPIs for a single round:
    - Counts: n_orders (created), n_completed, n_failed, late_count
    - Durations: mean_cycle_time_s, mean_tardiness_s, max_tardiness_s, makespan_s
    - Rates: pct_late (late_count / n_completed)
    - Metadata: partial (marker for incomplete rounds)

    All durations are in seconds; None means no data was available.
    Makespan is last_completed_ts - first_created_ts over active time (pauses excluded).
    """

    n_orders: int
    n_completed: int
    n_failed: int
    late_count: int
    mean_cycle_time_s: float | None
    mean_tardiness_s: float | None
    max_tardiness_s: float | None
    pct_late: float | None
    makespan_s: float | None
    partial: bool


def summarize_round(
    state: KpiState,
    as_of: datetime,
    recipes: Mapping[str, Mapping[str, str]],
    cfg: KpiConfig,
    *,
    partial: bool = False,
) -> RoundSummary:
    """Summarize a round of orders for comparison reporting.

    Calls ``advance`` with warmup_orders=0 (forcing all orders into the
    calculation regardless of ``cfg.warmup_orders``), then extracts KPI metrics
    and computes makespan as the span from the first created order to the last
    completed, excluding pause intervals (active time).

    Args:
        state: The KPI state accumulated over the run.
        as_of: The timestamp to snapshot at (typically last_event_ts).
        recipes: Station -> product -> unit_time_s.
        cfg: KPI configuration (warmup_orders will be overridden to 0).
        partial: If True, marks this round as incomplete (is echoed in output).

    Returns:
        RoundSummary with n_orders, n_completed, late metrics, cycle times, makespan.
        If no orders have completed, makespan_s and all duration means are None.
    """
    # Force warmup_orders=0 (D-09, D-10)
    cfg_no_warmup = dataclasses.replace(cfg, warmup_orders=0)

    # Call advance with the overridden config
    snapshot = advance(state, as_of, recipes, cfg_no_warmup)

    # Extract counts and basic metrics
    n_orders = snapshot.orders_total
    n_completed = snapshot.orders_completed
    late_count = snapshot.due.late_count if snapshot.due and snapshot.due.late_count else 0
    n_failed = snapshot.due.n_failed if snapshot.due and snapshot.due.n_failed else 0
    pct_late = snapshot.due.late_pct if snapshot.due else None
    mean_cycle_time_s = snapshot.cycle_time.per_pizza_mean_s if snapshot.cycle_time else None
    mean_tardiness_s = snapshot.due.tardiness_mean_s if snapshot.due else None
    max_tardiness_s = snapshot.due.tardiness_max_s if snapshot.due else None

    # Compute makespan = last_completed_ts - first_created_ts (active time)
    makespan_s: float | None = None
    if n_completed > 0:
        # Find first created and last completed over ALL orders (no warmup filter)
        first_created = None
        last_completed = None
        for order in state.orders.values():
            if first_created is None or order.created_ts < first_created:
                first_created = order.created_ts
            if order.completed_ts is not None:
                if last_completed is None or order.completed_ts > last_completed:
                    last_completed = order.completed_ts

        if first_created is not None and last_completed is not None:
            frozen = ActiveClock(
                frozen_intervals(state.pause_marks, as_of),
                scale_segments(state.scale_marks, as_of),
            )
            makespan_s = _secs(first_created, last_completed, frozen)

    return RoundSummary(
        n_orders=n_orders,
        n_completed=n_completed,
        n_failed=n_failed,
        late_count=late_count,
        mean_cycle_time_s=mean_cycle_time_s,
        mean_tardiness_s=mean_tardiness_s,
        max_tardiness_s=max_tardiness_s,
        pct_late=pct_late,
        makespan_s=makespan_s,
        partial=partial,
    )


# Comparison fields: scalar and nested attributes from KpiSnapshot to compare.
# Excludes bottleneck*, ts, and event_id; includes cycle_time.* and due.* metrics.
# This is a tuple of dotted paths and special markers for dynamic expansion.
COMPARE_FIELDS: tuple[str, ...] = (
    "orders_total",
    "orders_completed",
    "pizzas_per_s",
    "capacity_ratio",
    "wip_now",
    "wip_pizzas",
    "wip_pre_release",
    "wip_bank",
    "wip_bank_capacity",
    "wip_failed",
    "wip_avg",
    "cycle_time.n",
    "cycle_time.mean_s",
    "cycle_time.p50_s",
    "cycle_time.p95_s",
    "cycle_time.per_pizza_mean_s",
    "cycle_time.per_pizza_p50_s",
    "cycle_time.per_pizza_p95_s",
    "due.late_count",
    "due.n_completed",
    "due.n_failed",
    "due.late_pct",
    "due.tardiness_mean_s",
    "due.tardiness_max_s",
    # Station utilization is added dynamically per stored snapshot's keys
    "__stations_utilization__",
    "little.status",
    "little.l",
    "little.lambda_per_s",
    "little.w_s",
    "little.lambda_w",
    "little.rel_error",
    "little.n_completed",
    "little.n_in_system",
)


@dataclass(frozen=True)
class FieldDiff:
    """Result of comparing a single field between two snapshots.

    Attributes:
        field: Dotted path to the field (e.g., "cycle_time.mean_s").
        stored: Value from the stored (reference) snapshot.
        recomputed: Value from the recomputed snapshot.
        diff: Absolute difference (for numeric fields) or None.
        ok: True if values match within tolerance.
    """

    field: str
    stored: Any
    recomputed: Any
    diff: float | None
    ok: bool


def _get_nested(obj: Any, path: str) -> Any:
    """Get a value from an object via dotted path (e.g., 'cycle_time.mean_s')."""
    for part in path.split("."):
        if obj is None:
            return None
        obj = getattr(obj, part, None)
    return obj


def _tolerance_match(stored: Any, recomputed: Any, tol: float) -> bool:
    """Check if two values match within tolerance.

    Handles None, float, and other comparisons.
    For floats, uses absolute tolerance if abs(stored) < 1, else relative.
    """
    if stored is None and recomputed is None:
        return True
    if stored is None or recomputed is None:
        return False
    if isinstance(stored, float) and isinstance(recomputed, float):
        if math.isnan(stored) or math.isnan(recomputed):
            return False
        if math.isinf(stored) or math.isinf(recomputed):
            return False
        abs_diff = abs(stored - recomputed)
        # Absolute tolerance for small values, relative for large
        if abs(stored) < 1:
            return abs_diff <= tol
        else:
            return abs_diff <= tol * abs(stored)
    return stored == recomputed


def compare_fields(
    stored: KpiSnapshot,
    recomputed: KpiSnapshot,
    tol: float = DEFAULT_TOLERANCE,
) -> list[FieldDiff]:
    """Compare stored and recomputed snapshots field by field.

    Returns a list of FieldDiff for EVERY compared field (not just mismatches).
    Tolerance is applied to numeric fields. Exclusions include bottleneck*,
    ts, event_id, and bottleneck_candidate_count.

    Args:
        stored: The reference snapshot (as stored, possibly tampered).
        recomputed: The newly computed snapshot from facts.
        tol: Tolerance for float comparisons (default 1e-6).

    Returns:
        list[FieldDiff] with one entry per compared field, ok=True iff match.
    """
    diffs: list[FieldDiff] = []

    # Expand station fields from stored snapshot's keys
    fields_to_compare = list(COMPARE_FIELDS)
    if "__stations_utilization__" in fields_to_compare:
        fields_to_compare.remove("__stations_utilization__")
        for station_name in sorted(stored.stations.keys()):
            fields_to_compare.append(f"stations.{station_name}.utilization")

    for field_path in fields_to_compare:
        stored_val = _get_nested(stored, field_path)
        recomputed_val = _get_nested(recomputed, field_path)
        match = _tolerance_match(stored_val, recomputed_val, tol)

        # Compute diff for numeric fields
        diff: float | None = None
        if isinstance(stored_val, float) and isinstance(recomputed_val, float):
            diff = abs(stored_val - recomputed_val)

        diffs.append(
            FieldDiff(
                field=field_path,
                stored=stored_val,
                recomputed=recomputed_val,
                diff=diff,
                ok=match,
            )
        )

    return diffs


def all_match(diffs: Sequence[FieldDiff]) -> bool:
    """Return True iff all fields match (all diffs are ok=True)."""
    return all(d.ok for d in diffs)


def recompute(
    facts: Sequence[Message],
    stored: KpiSnapshot,
    recipes: Mapping[str, Mapping[str, float]],
    cfg: KpiConfig,
) -> KpiSnapshot:
    """Recompute a KPI snapshot from facts using stored parameters.

    Mirrors kpi/service.py::recover(). Folds only facts with:
    - run_id matching stored.run_id
    - ts <= stored.ts

    Restores run parameters (weights, run_started_ts) from the stored
    snapshot. Advances to stored.ts with stored.warmup_orders.

    Args:
        facts: Sequence of messages to fold (from Kafka replay).
        stored: The snapshot to replay towards (contains run_id, ts, warmup_orders).
        recipes: Station -> product -> unit_time_s.
        cfg: KPI configuration.

    Returns:
        KpiSnapshot computed at stored.ts with the same parameters.
    """
    run_id = stored.run_id
    state = new_state(run_id, None, "observed")

    # Fold facts with matching run_id and ts <= stored.ts
    for fact in facts:
        in_run = getattr(fact, "run_id", None) == run_id or isinstance(fact, TimeScaleChanged)
        if in_run and fact.ts <= stored.ts:
            state = fold(state, fact)

    # Restore run parameters if available
    if stored.run_weights is not None:
        apply_run_params(state, stored.run_weights, stored.run_started_ts)

    # Advance to stored.ts with stored.warmup_orders
    cfg_with_warmup = dataclasses.replace(cfg, warmup_orders=stored.warmup_orders)
    recomputed = advance(state, stored.ts, recipes, cfg_with_warmup)

    return recomputed
