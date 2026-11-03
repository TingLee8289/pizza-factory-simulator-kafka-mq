"""Shared constants for the pizza line."""

# Station slugs in process order: dough, sauce, cheese, toppings, bake, slice, package.
# In v2, parallel machines will use machine ids such as `bake-1`, `bake-2`.
STATIONS: tuple[str, ...] = (
    "spin",
    "sauce",
    "cheese",
    "toppings",
    "bake",
    "slice",
    "package",
)

LINE_ID = "pizza-1"

# Phase 3: product catalogue (D-01) and input bounds (T-03-01).
PRODUCTS: tuple[str, ...] = ("margherita", "pepperoni", "veggie")
MAX_ORDER_QTY = 20
MAX_ORDERS_CAP = 10000
SLUG_PATTERN = r"^[a-z][a-z0-9_-]{0,31}$"

# Generator draws qty uniformly in [QTY_MIN, QTY_MAX]; KPI theory uses
# E[qty] = (QTY_MIN + QTY_MAX) / 2 (Pitfall 8) -- keep both in sync via these.
QTY_MIN: int = 1
QTY_MAX: int = 3

# Phase 07.2 D-05: numeric priority replaces the rush/normal boolean.
# Smaller numbers are more urgent; 1..99 inclusive, default 50.
PRIORITY_MIN: int = 1
PRIORITY_MAX: int = 99
PRIORITY_DEFAULT: int = 50
# ETA lead multiplier for auto due dates (due = created + lead * buffer_k). 1.0 puts due exactly
# at the estimate, so any queueing variance makes the lot late; 1.5 leaves slack.
BUFFER_K_DEFAULT: float = 1.5
# D-15: single "high priority" boundary shared by the UI chip, 3D scale-up and
# any danger-highlight rule. Priority <= this value counts as high priority.
PRIORITY_HIGH_THRESHOLD: int = 10
# D-06: legacy `rush=true` maps to this priority; `rush=false`/missing -> DEFAULT.
PRIORITY_LEGACY_RUSH: int = 10

# Phase 07.2 D-08/D-09: generator auto orders give a fraction of lots the high
# priority value; the rest get PRIORITY_DEFAULT. Drawn from an independent
# sha256-seeded RNG stream so existing streams are not perturbed.
DEFAULT_PRIORITY_HIGH_RATIO: float = 0.10
DEFAULT_PRIORITY_HIGH_VALUE: int = 10
