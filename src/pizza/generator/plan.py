"""Pure seeded order planner (ORDER-04, D-01, D-08, D-09, D-10).

No broker, asyncio or wall-clock imports: ``t0`` is a parameter so tests control it.
"""

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta

from pizza.common.constants import (
    DEFAULT_PRIORITY_HIGH_RATIO,
    DEFAULT_PRIORITY_HIGH_VALUE,
    PRIORITY_DEFAULT,
    PRIORITY_MAX,
    PRIORITY_MIN,
    QTY_MAX,
    QTY_MIN,
)
from pizza.common.envelope import RunParams
from pizza.common.ids import make_order_id
from pizza.common.recipes import theoretical_total_s
from pizza.common.rng import stream_rng


@dataclass(frozen=True)
class PlannedOrder:
    seq: int
    order_id: str
    offset_s: float
    created_ts: datetime
    product: str
    qty: int
    due_ts: datetime
    source: str | None = None
    priority: int = PRIORITY_DEFAULT
    eta_ts: datetime | None = None
    station_eta: dict[str, datetime] | None = None


def _trunc_ms(value: datetime) -> datetime:
    return value.replace(microsecond=value.microsecond // 1000 * 1000)


def plan_orders(
    params: RunParams,
    t0: datetime,
    recipes: Mapping[str, Mapping[str, float]],
    *,
    rate_source: Callable[[], float] | None = None,
    priority_high_ratio: float = DEFAULT_PRIORITY_HIGH_RATIO,
    priority_high_value: int = DEFAULT_PRIORITY_HIGH_VALUE,
) -> Iterator[PlannedOrder]:
    """Yield the deterministic order sequence for (params, t0).

    ``rate_source`` (optional) returns the live arrival rate (orders/s) consulted
    before each inter-arrival gap; the uniform draw sequence is unchanged, only
    its scale (D-06).

    Validation happens eagerly (before the first ``next``) so callers can catch
    ValueError at call time.
    """
    products = sorted(params.weights)
    known = set(next(iter(recipes.values()), {})) if recipes else set()
    for product in products:
        if params.weights[product] > 0 and (
            product not in known or any(product not in table for table in recipes.values())
        ):
            raise ValueError(f"weights name product {product!r} absent from recipes")
    if not 0.0 <= priority_high_ratio <= 1.0:
        raise ValueError(f"priority_high_ratio must be in [0, 1], got {priority_high_ratio!r}")
    if not PRIORITY_MIN <= priority_high_value <= PRIORITY_MAX:
        raise ValueError(
            f"priority_high_value must be in [{PRIORITY_MIN}, {PRIORITY_MAX}], "
            f"got {priority_high_value!r}"
        )
    return _generate(
        params, t0, recipes, products, rate_source, priority_high_ratio, priority_high_value
    )


def _generate(
    params: RunParams,
    t0: datetime,
    recipes: Mapping[str, Mapping[str, float]],
    products: list[str],
    rate_source: Callable[[], float] | None = None,
    priority_high_ratio: float = DEFAULT_PRIORITY_HIGH_RATIO,
    priority_high_value: int = DEFAULT_PRIORITY_HIGH_VALUE,
) -> Iterator[PlannedOrder]:
    weights = [params.weights[p] for p in products]
    arrival_rng = stream_rng(params.seed, "arrival")
    mix_rng = stream_rng(params.seed, "mix")
    qty_rng = stream_rng(params.seed, "qty")
    k_rng = stream_rng(params.seed, "k")
    prio_rng = stream_rng(params.seed, "priority")
    offset_s = 0.0
    seq = 0
    while params.max_orders is None or seq < params.max_orders:
        seq += 1
        rate = params.arrival_rate
        if rate_source is not None:
            live = rate_source()
            if live > 0:
                rate = live
        offset_s += arrival_rng.expovariate(1.0) / rate
        product = mix_rng.choices(products, weights=weights)[0]
        qty = qty_rng.randint(QTY_MIN, QTY_MAX)
        if params.k_max is None:
            k_eff = params.k
        else:
            k_eff = k_rng.uniform(params.k, params.k_max)
        # Exactly one draw per order, unconditionally, from its own stream (D-08).
        is_high = prio_rng.random() < priority_high_ratio
        priority = priority_high_value if is_high else PRIORITY_DEFAULT
        created_ts = _trunc_ms(t0 + timedelta(seconds=offset_s))
        total = theoretical_total_s(recipes, product, qty)  # type: ignore[arg-type]
        due_ts = _trunc_ms(created_ts + timedelta(seconds=total * k_eff))
        yield PlannedOrder(
            seq=seq,
            order_id=make_order_id(params.seed, seq),
            offset_s=offset_s,
            created_ts=created_ts,
            product=product,
            qty=qty,
            due_ts=due_ts,
            priority=priority,
        )
