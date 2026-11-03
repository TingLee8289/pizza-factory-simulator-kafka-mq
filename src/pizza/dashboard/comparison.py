"""Comparison runner: orchestrate FIFO/EDD/SPT rounds on identical seeded orders."""

import asyncio
import csv
import io
import secrets
from collections.abc import Awaitable, Callable
from typing import Any

import structlog
from pydantic import BaseModel, ConfigDict, Field

from pizza.common.constants import PRODUCTS
from pizza.common.envelope import ComparisonResult
from pizza.dashboard.control import (
    MAX_ARRIVAL_PER_MIN,
    MAX_SEED,
    ConflictError,
    RunController,
    StartRequest,
)

_log = structlog.get_logger()

# Configuration constants
ROUND_RULES = ("fifo", "edd", "spt")
DEFAULT_N = 30
DEFAULT_K = 1.5
DEFAULT_RATE_PER_MIN = 2.0
MAX_COMPARISONS = 50
CSV_COLUMNS = "rule,run_id,seed,n_orders,mean_cycle_time,mean_tardiness,pct_late,makespan"


def _safe_cell(value: Any) -> str:
    """Safely format a cell value for CSV, preventing injection."""
    s = str(value) if not isinstance(value, (int, float)) else str(value)
    if s and s[0] in "=+-@":
        # Return the value; the CSV writer will properly quote it
        return s
    return s


class ComparisonStartRequest(BaseModel):
    """Request to start a comparison run with identical seeded orders."""

    model_config = ConfigDict(extra="forbid")

    n_orders: int = Field(default=DEFAULT_N, ge=5, le=99)
    arrival_rate_per_min: float = Field(default=DEFAULT_RATE_PER_MIN, gt=0, le=MAX_ARRIVAL_PER_MIN)
    k: float = Field(default=DEFAULT_K, ge=1.0)
    seed: int | None = Field(default=None, ge=0, le=MAX_SEED)
    weights: dict[str, float] | None = None
    random_breakdowns: bool = True


class ResultStore(dict[tuple[str, int], ComparisonResult]):
    """Store comparison results, capped at MAX_COMPARISONS, oldest first."""

    def add(self, result: ComparisonResult) -> None:
        """Add a result, evicting oldest if necessary."""
        key = (result.comparison_id, result.round)
        # Dedup: replace if already exists
        if key in self:
            del self[key]
        # Evict oldest if at capacity
        if len(self) >= MAX_COMPARISONS:
            oldest_key = next(iter(self))
            del self[oldest_key]
        self[key] = result


class ComparisonRunner:
    """State machine for running FIFO/EDD/SPT comparisons sequentially."""

    def __init__(
        self,
        controller: RunController,
        on_change: Callable[[str, dict[str, Any]], Awaitable[None]] | None = None,
    ) -> None:
        self.controller = controller
        self._on_change = on_change
        self._lock = asyncio.Lock()
        self._state = "idle"
        self._comparison_id: str | None = None
        self._round = 1
        self._seed: int | None = None
        self._n_orders: int | None = None
        self._rate_per_min: float | None = None
        self._k: float | None = None
        self._weights: dict[str, float] | None = None
        self._random_breakdowns = True
        self._closing = False
        self._results = ResultStore()

    @property
    def state(self) -> str:
        """Current state machine value (``idle``/``round_running``/.../``error``)."""
        return self._state

    @property
    def results(self) -> ResultStore:
        """All stored results across every comparison (read-only view, D-12)."""
        return self._results

    def status(self, runs: dict[str, Any] | None = None) -> dict[str, Any]:
        """Return the current status.

        ``runs`` is the dashboard's run history (plan 08); when given, it lets
        ``interrupted`` detect a comparison that started (seen in ``runs``) but
        never finished all 3 rounds and is not the active comparison.
        """
        rows = comparison_rows(self._results)
        return {
            "state": self._state,
            "comparison_id": self._comparison_id,
            "round": self._round,
            "rule": ROUND_RULES[self._round - 1] if self._round <= 3 else None,
            "n_orders": self._n_orders,
            "seed": self._seed,
            "k": self._k,
            "arrival_rate_per_min": self._rate_per_min,
            "random_breakdowns": self._random_breakdowns,
            "weights": dict(self._weights) if self._weights else None,
            "run_id": self.controller.run_info().get("run_id"),
            "results": rows,
            "best": best_by_metric(rows),
            "interrupted": detect_interrupted(runs or {}, self._results, self._comparison_id)
            is not None,
        }

    async def _notify(self, event: str, status: dict[str, Any]) -> None:
        if self._on_change is not None:
            await self._on_change(event, status)

    async def start(self, request: ComparisonStartRequest) -> dict[str, Any]:
        """Start a new comparison with FIFO round 1."""
        async with self._lock:
            if self._state != "idle":
                raise ConflictError("a comparison is already running")
            if self.controller.state != "none":
                raise ConflictError("a run is already active")

            # Mint comparison_id and seed
            self._comparison_id = f"c-{secrets.token_hex(8)}"
            self._seed = request.seed if request.seed is not None else secrets.randbelow(2**31)
            self._n_orders = request.n_orders
            self._rate_per_min = request.arrival_rate_per_min
            self._k = request.k
            self._weights = request.weights or dict.fromkeys(PRODUCTS, 1.0)
            self._random_breakdowns = request.random_breakdowns
            self._round = 1
            self._closing = False
            self._state = "round_running"

            # Set the lock on the controller
            self.controller.comparison_active = True

            try:
                # Start round 1 with FIFO
                await self._start_round()
            except Exception as e:
                self._state = "error"
                self.controller.comparison_active = False
                raise RuntimeError("could not start round 1") from e

            status = self.status()
        await self._notify("started", status)
        return status

    async def next_round(self) -> dict[str, Any]:
        """Advance to the next round."""
        async with self._lock:
            if self._state != "round_done":
                raise ConflictError("cannot advance: not round_done")
            if self._round >= 3:
                raise ConflictError("cannot advance past round 3")

            self._round += 1
            self._closing = False
            self._state = "round_running"

            try:
                await self._start_round()
            except Exception as e:
                self._state = "error"
                self.controller.comparison_active = False
                raise RuntimeError(f"could not start round {self._round}") from e

            status = self.status()
        await self._notify("next_round", status)
        return status

    async def end_round(self) -> None:
        """End the current round early."""
        async with self._lock:
            if self._state not in ("round_running", "round_done"):
                raise ConflictError("no round is active")

            # Stop generation and close the round with partial=True
            await self.controller.stop_generation()
            await self.controller.send_round_close(partial=True)

    async def cancel(self) -> None:
        """Cancel the comparison."""
        async with self._lock:
            if self.controller.state != "none":
                await self.controller.reset(_internal=True)
            self._state = "cancelled"
            self.controller.comparison_active = False
            status = self.status()
        await self._notify("cancelled", status)

    async def on_progress(self, run_id: str, completed: int, failed: int) -> None:
        """Called when a progress update arrives."""
        async with self._lock:
            if self._state != "round_running":
                return
            if run_id != self.controller.run_info().get("run_id"):
                return

            # Check if round is complete
            n = self._n_orders
            if completed + failed >= n and not self._closing:
                self._closing = True
                try:
                    await self.controller.send_round_close(partial=False)
                except Exception:
                    _log.warning("failed_to_send_round_close")

    async def on_result(self, result: ComparisonResult) -> None:
        """Called when a ComparisonResult fact arrives."""
        status: dict[str, Any] | None = None
        async with self._lock:
            # Always store results for any comparison
            self._results.add(result)

            if result.comparison_id == self._comparison_id and result.round == self._round:
                # Store and transition to round_done
                self._state = "round_done"

                if self._round >= 3:
                    # All rounds complete
                    self._state = "finished"
                    self.controller.comparison_active = False

                status = self.status()
        if status is not None:
            await self._notify("result", status)

    async def _start_round(self) -> None:
        """Internal: start a round (reset if needed, then start)."""
        # If controller has a run, reset it
        if self.controller.state != "none":
            await self.controller.reset(_internal=True)

        # Start the new run
        rule = ROUND_RULES[self._round - 1]
        req = StartRequest(
            seed=self._seed,
            arrival_rate_per_min=self._rate_per_min,
            weights=self._weights,
            random_breakdowns=self._random_breakdowns,
            k=self._k,
            max_orders=self._n_orders,
            rule=rule,
            comparison_id=self._comparison_id,
            round=self._round,
        )
        await self.controller.start(req, _internal=True)


def comparison_rows(results: dict[tuple[str, int], ComparisonResult]) -> list[dict[str, Any]]:
    """Extract rows from stored ComparisonResults for display/CSV."""
    if not results:
        return []

    # Group by comparison_id and sort by round
    by_comp = {}
    for (comp_id, round_num), result in results.items():
        if comp_id not in by_comp:
            by_comp[comp_id] = []
        by_comp[comp_id].append((round_num, result))

    # Return the newest comparison's rows, sorted by round
    newest_comp_id = max(by_comp.keys()) if by_comp else None
    if not newest_comp_id:
        return []

    rows = []
    for _round_num, result in sorted(by_comp[newest_comp_id]):
        rows.append({
            "rule": result.rule,
            "run_id": result.run_id,
            "seed": result.seed,
            "n_orders": result.n_orders,
            "mean_cycle_time": result.mean_cycle_time_s,
            "mean_tardiness": result.mean_tardiness_s,
            "pct_late": result.pct_late,
            "makespan": result.makespan_s,
            "partial": result.partial,
            "n_failed": result.n_failed,
            "max_tardiness": result.max_tardiness_s,
        })
    return rows


def comparison_csv(rows: list[dict[str, Any]]) -> str:
    """Generate CSV from comparison rows, with injection protection."""
    output = io.StringIO()
    # Use QUOTE_NONNUMERIC to quote all non-numeric fields
    writer = csv.writer(output, lineterminator="\n", quoting=csv.QUOTE_NONNUMERIC)

    # Write header (all strings, will be quoted)
    writer.writerow(CSV_COLUMNS.split(","))

    # Write rows (mix of strings and numbers; dangerous strings will be quoted)
    for row in rows:
        writer.writerow([
            _safe_cell(row.get("rule", "")),
            _safe_cell(row.get("run_id", "")),
            row.get("seed") or 0,
            row.get("n_orders") or 0,
            row.get("mean_cycle_time") or 0.0,
            row.get("mean_tardiness") or 0.0,
            row.get("pct_late") or 0.0,
            row.get("makespan") or 0.0,
        ])

    return output.getvalue()


def csv_filename(rows: list[dict[str, Any]]) -> str:
    """Generate a safe filename for the CSV download."""
    if not rows:
        return "comparison-0.csv"
    seed = rows[0].get("seed", 0)
    return f"comparison-{int(seed)}.csv"


def best_by_metric(rows: list[dict[str, Any]]) -> dict[str, set[str]]:
    """Return the set of rules with the best (lowest) value per metric."""
    if len(rows) < 2:
        return {}

    metrics = ["mean_cycle_time", "mean_tardiness", "pct_late", "makespan"]
    best = {}

    for metric in metrics:
        values = [(row.get("rule"), row.get(metric)) for row in rows if metric in row]
        numeric = [(rule, v) for rule, v in values if v is not None and isinstance(v, (int, float))]
        if len(numeric) < 2:
            best[metric] = set()
            continue
        min_val = min(v for _, v in numeric)
        best[metric] = {rule for rule, v in numeric if v == min_val}

    return best


def detect_interrupted(
    runs: dict[str, Any],
    results: dict[tuple[str, int], ComparisonResult],
    active_comparison_id: str | None,
) -> str | None:
    """Detect if a comparison was interrupted: it started (seen in ``runs``, which
    carries ``comparison_id`` from run params) or already produced a result, has
    fewer than 3 stored results, and is not the active comparison (Open Question 4:
    runs without run params, i.e. without a ``comparison_id``, are ignored).
    """
    # Count results per comparison_id.
    by_comp: dict[str, int] = {}
    for comp_id, _round in results:
        by_comp[comp_id] = by_comp.get(comp_id, 0) + 1

    # Comparisons known to have started, from the dashboard's run history.
    started = {
        info.get("comparison_id")
        for info in runs.values()
        if isinstance(info, dict) and info.get("comparison_id")
    }

    # Find the newest non-active comparison with fewer than 3 results.
    for comp_id in sorted(started | set(by_comp), reverse=True):
        if comp_id is None or comp_id == active_comparison_id:
            continue
        if by_comp.get(comp_id, 0) < 3:
            return comp_id

    return None
