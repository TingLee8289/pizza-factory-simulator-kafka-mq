"""Offline rule-differentiation oracle (RESEARCH Pitfall 8, Wave 0 gap).

A single-file, pure-Python discrete-event approximation of the seven-station
serial pizza line: one server per station, infinite buffers, no SimPy
dependency. It reuses the real seeded order generator
(``pizza.generator.plan.plan_orders``), the shipped recipe tables
(``pizza.common.recipes.load_all_recipes``), and the scheduler's exact
``ordering_key`` semantics (priority from the planned order) to pick the next job at
each station under FIFO / EDD / SPT.

This is a sanity check only -- it is NOT part of the product runtime and is
not shipped in any container (T-07-45). It exists to confirm, before any
50-minute manual run, that the shipped comparison defaults (``DEFAULT_K``,
``DEFAULT_RATE_PER_MIN``, ``DEFAULT_N`` in ``src/pizza/dashboard/comparison.py``)
actually make FIFO / EDD / SPT differ.

Run directly: ``uv run python scripts/rule_oracle.py``
"""

import heapq
import itertools
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from statistics import mean

from pizza.common.constants import PRODUCTS, STATIONS
from pizza.common.envelope import RunParams
from pizza.common.recipes import load_all_recipes
from pizza.dashboard.comparison import DEFAULT_K, DEFAULT_N, DEFAULT_RATE_PER_MIN
from pizza.generator.plan import PlannedOrder, plan_orders
from pizza.scheduler.core import BacklogEntry, OrderContext, ordering_key

# Fixed simulation epoch; only relative timestamps matter.
T0 = datetime(2024, 1, 1, tzinfo=UTC)

# Seed used for the default-parameter differentiation check (Test 3 / main()).
# Chosen arbitrarily but fixed so the printed table is reproducible.
DEFAULT_SEED = 42

RULES: tuple[str, ...] = ("fifo", "edd", "spt")


def simulate_line_queue(
    orders: Sequence[PlannedOrder],
    recipes: Mapping[str, Mapping[str, float]],
    rule: str,
    route: tuple[str, ...] = STATIONS,
) -> dict[str, datetime]:
    """Discrete-event simulation of one server per station in series.

    Each station has an infinite FIFO-ish buffer; at each station, whenever
    the server is free and jobs are waiting, the next job is chosen with the
    scheduler's real ``ordering_key(rule, entry, ctx)`` (priority comes from
    each planned order). Returns the absolute completion datetime at the
    last station in ``route``, keyed by ``order_id``.
    """
    by_id = {o.order_id: o for o in orders}
    counter = itertools.count()

    def proc_s(station: str, order_id: str) -> float:
        o = by_id[order_id]
        return recipes[station][o.product] * o.qty

    def ctx(station: str, order_id: str) -> OrderContext:
        o = by_id[order_id]
        return OrderContext(due_ts=o.due_ts, priority=o.priority, proc_s=proc_s(station, order_id))

    def key_fn(station: str):
        def key(entry: BacklogEntry) -> tuple:
            return ordering_key(rule, entry, ctx(station, entry.order_id))

        return key

    # Heap events: (time, seq, kind, station, order_id); kind in {"arrive", "free"}.
    events: list[tuple[datetime, int, str, str, str]] = []
    for o in orders:
        heapq.heappush(events, (o.created_ts, next(counter), "arrive", route[0], o.order_id))

    station_current: dict[str, str | None] = dict.fromkeys(route)
    queues: dict[str, list[BacklogEntry]] = {s: [] for s in route}
    completion: dict[str, datetime] = {}

    def start_service(station: str, now: datetime) -> None:
        entry = min(queues[station], key=key_fn(station))
        queues[station].remove(entry)
        station_current[station] = entry.order_id
        finish = now + timedelta(seconds=proc_s(station, entry.order_id))
        heapq.heappush(events, (finish, next(counter), "free", station, entry.order_id))

    while events:
        now, _seq, kind, station, order_id = heapq.heappop(events)
        idx = route.index(station)
        if kind == "arrive":
            queues[station].append(BacklogEntry(order_id, idx, now))
            if station_current[station] is None:
                start_service(station, now)
        else:  # "free"
            station_current[station] = None
            if idx + 1 < len(route):
                nxt = route[idx + 1]
                heapq.heappush(events, (now, next(counter), "arrive", nxt, order_id))
            else:
                completion[order_id] = now
            if queues[station]:
                start_service(station, now)

    return completion


def compare_rules(
    seed: int,
    n: int,
    rate_per_min: float,
    k: float,
    weights: Mapping[str, float] | None = None,
) -> dict[str, dict[str, float]]:
    """Run FIFO/EDD/SPT on the identical seeded order set; return per-rule metrics.

    Each rule's dict has ``mean_tardiness_s``, ``pct_late``, ``makespan_s`` and
    ``mean_cycle_s``. Deterministic: same arguments always give the same result.
    """
    recipes = load_all_recipes()
    resolved_weights = dict(weights) if weights else dict.fromkeys(PRODUCTS, 1.0)
    params = RunParams(
        seed=seed,
        arrival_rate=rate_per_min / 60.0,
        k=k,
        max_orders=n,
        weights=resolved_weights,
    )
    orders = list(plan_orders(params, T0, recipes))

    results: dict[str, dict[str, float]] = {}
    for rule in RULES:
        completion = simulate_line_queue(orders, recipes, rule)
        cycle_times: list[float] = []
        tardiness: list[float] = []
        late = 0
        for o in orders:
            done = completion[o.order_id]
            cycle_times.append((done - o.created_ts).total_seconds())
            tard = max(0.0, (done - o.due_ts).total_seconds())
            tardiness.append(tard)
            if tard > 0:
                late += 1
        results[rule] = {
            "mean_tardiness_s": mean(tardiness),
            "pct_late": 100.0 * late / len(orders),
            "makespan_s": (max(completion.values()) - T0).total_seconds(),
            "mean_cycle_s": mean(cycle_times),
        }
    return results


def main() -> None:
    results = compare_rules(DEFAULT_SEED, DEFAULT_N, DEFAULT_RATE_PER_MIN, DEFAULT_K)
    print(
        f"defaults: seed={DEFAULT_SEED} n_orders={DEFAULT_N} "
        f"rate_per_min={DEFAULT_RATE_PER_MIN} k={DEFAULT_K}\n"
    )
    header = f"{'rule':6} {'mean_tardiness_s':>18} {'pct_late':>10} {'makespan_s':>12} {'mean_cycle_s':>12}"
    print(header)
    for rule in RULES:
        r = results[rule]
        print(
            f"{rule:6} {r['mean_tardiness_s']:18.2f} {r['pct_late']:10.2f} "
            f"{r['makespan_s']:12.2f} {r['mean_cycle_s']:12.2f}"
        )


if __name__ == "__main__":
    main()
