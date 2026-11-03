"""Pure KPI recompute function and CLI for replay verification (COMP-03, D-13, D-14).

This module provides read-only replay and recomputation of KPI snapshots.
No producers are created; the module only reads from Kafka.
"""

import argparse
import json
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from uuid import uuid4

import structlog

from pizza.common.constants import PRODUCTS, STATIONS
from pizza.common.envelope import KpiSnapshot, Message, RecipeChanged, RecipeSnapshot
from pizza.common.recipes import load_all_recipes, validate_recipes
from pizza.common.topology import (
    TOPIC_KPI_SNAPSHOTS,
    TOPIC_ORDER_LIFECYCLE,
    TOPIC_STATION_EVENTS,
    TOPIC_TIMESCALE,
)
from pizza.kpi.compare import DEFAULT_TOLERANCE, all_match, compare_fields
from pizza.kpi.compare import recompute as kpi_recompute
from pizza.kpi.metrics import KpiConfig
from pizza.kpi.settings import get_kpi_settings
from pizza.scheduler.replay import replay_topics

_log = structlog.get_logger()

# Strict run_id format validation (matches _mint_run_id from dashboard/control.py)
RUN_ID_RE = re.compile(r"^r-[0-9]{8}-[0-9]{6}-[0-9a-f]{4}$")


class NoStoredSnapshot(ValueError):
    """Raised when no stored snapshot is found for the given run_id."""

    pass


def build_cfg_and_recipes() -> tuple[KpiConfig, Mapping[str, Mapping[str, float]]]:
    """Build KpiConfig and load recipes for recompute (shared with dashboard route)."""
    settings = get_kpi_settings()
    recipes = load_all_recipes(settings.recipes_dir)
    validate_recipes(recipes)
    cfg = KpiConfig(
        warmup_orders=settings.warmup_orders,
        little_tolerance=settings.little_tolerance,
        bottleneck_margin=settings.bottleneck_margin,
        bottleneck_confirm_n=settings.bottleneck_confirm_n,
        bottleneck_min_util=settings.bottleneck_min_util,
    )
    return cfg, recipes


def recipes_from_events(
    events: Sequence[Message],
    run_id: str,
    as_of: datetime,
    fallback: Mapping[str, Mapping[str, float]],
) -> tuple[dict[str, dict[str, float]], str, int]:
    """Rebuild the in-effect recipe table for one run from its facts (Phase 8 D-11, D-12).

    Returns ``(table, source, changes_applied)``. ``source`` is ``"events"`` when a
    RecipeSnapshot for ``run_id`` exists at or before ``as_of``, its structure is
    intact, and the table is rebuilt from it plus later RecipeChanged facts. Otherwise
    ``source`` is ``"disk"`` and the table is a copy of ``fallback``.

    ``fallback`` is never mutated and ``base.recipes`` is never aliased. This performs
    a structural check only; it deliberately skips the full recipe-invariant validator.
    """

    def _from_disk() -> tuple[dict[str, dict[str, float]], str, int]:
        return {station: dict(values) for station, values in fallback.items()}, "disk", 0

    snapshots = [
        ev
        for ev in events
        if isinstance(ev, RecipeSnapshot) and ev.run_id == run_id and ev.ts <= as_of
    ]
    if not snapshots:
        return _from_disk()

    base = max(snapshots, key=lambda ev: ev.ts)
    table = {station: dict(values) for station, values in base.recipes.items()}
    if set(table) != set(STATIONS) or any(
        set(values) != set(PRODUCTS) for values in table.values()
    ):
        return _from_disk()

    changes = [
        ev
        for ev in events
        if isinstance(ev, RecipeChanged) and ev.run_id == run_id and base.ts <= ev.ts <= as_of
    ]
    changes.sort(key=lambda ev: ev.ts)
    for ev in changes:
        table.setdefault(ev.station, {})[ev.product] = ev.new_unit_time_s
    return table, "events", len(changes)


def run_recompute(
    bootstrap: str,
    run_id: str,
    recipes: Mapping[str, Mapping[str, float]],
    cfg: KpiConfig,
    *,
    timeout_s: float = 60.0,
    tolerance: float = DEFAULT_TOLERANCE,
    replay: Callable = replay_topics,
    group_id: str | None = None,
) -> dict:
    """Recompute a KPI snapshot from facts on a fresh consumer group (read-only, D-13).

    Validates run_id format strictly before any I/O.
    Reads KpiSnapshot with max-ts for the run_id (ignoring ComparisonResult records).
    Reads station.events and order.lifecycle facts only for this run_id and ts <= stored.ts.
    Calls the pure compare.recompute() and compare.fields() to produce a report.

    Args:
        bootstrap: Kafka bootstrap servers.
        run_id: The run to recompute (must match RUN_ID_RE or ValueError is raised).
        recipes: Fallback Station -> product -> unit_time_s table used when the run
            has no ``recipe.snapshot``.
        cfg: KPI configuration.
        timeout_s: Timeout for each replay.
        tolerance: Float tolerance for field comparison.
        replay: Callable for replay_topics (injectable for testing).
        group_id: Optional override for consumer group (testing only).

    Returns:
        dict with:
        - run_id: The run_id
        - matched: bool, True iff all compared fields match within tolerance
        - tolerance: The tolerance value used
        - fields: list of FieldDiff dicts (field, stored, recomputed, diff, ok)
        - stored_ts: The timestamp of the stored snapshot
        - n_events: Number of events folded
        - recipes_source: ``"events"`` when the table came from the run's snapshot plus
          later changes, ``"disk"`` when no usable snapshot existed
        - recipe_changes_applied: int count of applied ``recipe.changed`` facts

    The derived table is structurally checked by ``recipes_from_events`` but is never
    passed through ``validate_recipes``, so a live edit cannot fail recompute.

    Raises:
        ValueError: If run_id does not match RUN_ID_RE
        NoStoredSnapshot: If no stored snapshot found for run_id
    """
    # Validate run_id format before any I/O (T-07-25)
    if not RUN_ID_RE.match(run_id):
        raise ValueError(f"invalid run_id format: {run_id}")

    # Generate unique replay group ids (T-07-26: never kpi/dashboard/scheduler)
    snap_group_id = group_id or f"replay-{uuid4().hex}"
    facts_group_id = f"replay-{uuid4().hex}"

    # Replay stored snapshot (max-ts KpiSnapshot for this run_id)
    snap_facts, _ = replay(
        bootstrap,
        [TOPIC_KPI_SNAPSHOTS],
        timeout_s,
        group_id=snap_group_id,
    )

    stored_snaps = [
        f for f in snap_facts
        if isinstance(f, KpiSnapshot) and f.run_id == run_id
    ]
    if not stored_snaps:
        raise NoStoredSnapshot(f"no stored snapshot for run_id {run_id}")

    stored = max(stored_snaps, key=lambda s: s.ts)

    # Replay facts for recomputation
    events, _ = replay(
        bootstrap,
        [TOPIC_STATION_EVENTS, TOPIC_ORDER_LIFECYCLE, TOPIC_TIMESCALE],
        timeout_s,
        group_id=facts_group_id,
    )

    # Derive the in-effect recipe table from the run's facts (D-11, D-12)
    derived, source, applied = recipes_from_events(events, run_id, stored.ts, recipes)

    # Recompute using pure function
    recomputed = kpi_recompute(events, stored, derived, cfg)

    # Compare fields
    diffs = compare_fields(stored, recomputed, tolerance)

    # Build report
    return {
        "run_id": run_id,
        "matched": all_match(diffs),
        "tolerance": tolerance,
        "fields": [
            {
                "field": d.field,
                "stored": d.stored,
                "recomputed": d.recomputed,
                "diff": d.diff,
                "ok": d.ok,
            }
            for d in diffs
        ],
        "stored_ts": stored.ts.isoformat() if hasattr(stored.ts, "isoformat") else str(stored.ts),
        "n_events": len(events),
        "recipes_source": source,
        "recipe_changes_applied": applied,
    }


def main(argv: list[str] | None = None, runner: Callable = run_recompute) -> int:
    """CLI entry point for run_recompute.

    Arguments:
        argv: Command-line arguments (default: sys.argv[1:])
        runner: Callable to run recompute (injectable for testing)

    Returns:
        0 if recomputed snapshot matches stored (matched=True)
        1 if mismatch (matched=False)
        2 on error (NoStoredSnapshot or other exception)
    """
    parser = argparse.ArgumentParser(
        description="Recompute a KPI snapshot from Kafka facts for verification",
        prog="python -m pizza.kpi.recompute",
    )
    parser.add_argument(
        "--run-id",
        required=True,
        help="Run ID to recompute (format: r-YYYYMMDD-HHMMSS-xxxx)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output report as JSON",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="Replay timeout in seconds (default: 60.0)",
    )

    try:
        args = parser.parse_args(argv)
    except SystemExit:
        raise

    try:
        cfg, recipes = build_cfg_and_recipes()
        report = runner(
            bootstrap=_get_kafka_bootstrap(),
            run_id=args.run_id,
            recipes=recipes,
            cfg=cfg,
            timeout_s=args.timeout,
        )

        matched = report.get("matched", False)

        if args.json:
            # JSON output
            print(json.dumps(report, indent=2, default=str))
        else:
            # Human-readable output
            print(f"Run ID:           {report['run_id']}")
            print(f"Matched:          {matched}")
            print(f"Tolerance:        {report['tolerance']}")
            print(f"Stored timestamp: {report['stored_ts']}")
            print(f"Events folded:    {report['n_events']}")
            print(f"Recipes source:   {report.get('recipes_source', 'unknown')}")
            print(f"Recipe changes:   {report.get('recipe_changes_applied', 0)}")
            print()
            print("Field Comparison:")
            for field in report["fields"]:
                status = "✓" if field["ok"] else "✗"
                print(f"  {status} {field['field']}")
                print(f"      stored:     {field['stored']}")
                print(f"      recomputed: {field['recomputed']}")
                if field["diff"] is not None:
                    print(f"      diff:       {field['diff']}")

        return 0 if matched else 1

    except NoStoredSnapshot as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        _log.exception("recompute_failed")
        return 2


def _get_kafka_bootstrap() -> str:
    """Get Kafka bootstrap from settings (helper for main)."""
    settings = get_kpi_settings()
    return settings.kafka_bootstrap


if __name__ == "__main__":
    raise SystemExit(main())
