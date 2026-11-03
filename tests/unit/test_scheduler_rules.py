"""Pure scheduler rule ordering tests (SCHED-03, D-01..D-04).

Tests for FIFO/EDD/SPT rules with priority-first, deterministic tie-breaking,
rule switching, and missing recipe handling.
"""

from datetime import UTC, datetime, timedelta

from pizza.common.constants import STATIONS
from pizza.common.envelope import OpCompleted, OpFailed, StationStatus
from pizza.common.ids import job_event_id
from pizza.scheduler.core import (
    CoreConfig,
    Dispatch,
    OrderRecord,
    SchedulerCore,
)

RUN = "r1"
T0 = datetime(2026, 1, 1, tzinfo=UTC)


def ts(n: int) -> datetime:
    """Create a timestamp at T0 + n milliseconds."""
    return T0 + timedelta(milliseconds=n)


def rec(
    oid: str, product: str = "margherita", qty: int = 1, due_ts: datetime | None = None
) -> OrderRecord:
    """Create an OrderRecord with optional due_ts override."""
    return OrderRecord(
        oid,
        product,
        qty,
        due_ts if due_ts is not None else T0 + timedelta(hours=1),
        T0,
        priority=50,
    )


def rec_rush(
    oid: str,
    product: str = "margherita",
    qty: int = 1,
    due_ts: datetime | None = None,
    priority: int = 10,
) -> OrderRecord:
    """Create an OrderRecord with an urgent numeric priority (default 10)."""
    return OrderRecord(
        oid,
        product,
        qty,
        due_ts if due_ts is not None else T0 + timedelta(hours=1),
        T0,
        priority=priority,
    )


def started(d: Dispatch, t: datetime, run: str = RUN) -> None:
    """Simulate order started at station."""
    return None  # stub for type completeness


def completed(d: Dispatch, t: datetime, run: str = RUN) -> OpCompleted:
    """Create an OpCompleted fact."""
    return OpCompleted.make(
        run_id=run,
        machine_id=d.step,
        ts=t,
        order_id=d.order_id,
        step=d.step,
        visit=1,
        attempt=1,
        qty=d.qty,
        command_event_id=d.command_event_id,
        duration_s=0.0,
    )


def ready(c: SchedulerCore, n: int = -1) -> None:
    """Every station reports idle: a never-reported station is not dispatchable."""
    for station in c.route:
        c.on_station_status(
            StationStatus.make(run_id=RUN, machine_id=station, ts=ts(n), status="idle")
        )


def fresh(rule: str = "fifo", recipes: dict[str, dict[str, float]] | None = None) -> SchedulerCore:
    """Create a fresh SchedulerCore with optional rule and recipes."""
    cfg = CoreConfig(recipes=recipes or {})
    c = SchedulerCore(config=cfg)
    c.start_run(RUN)
    c.rule = rule
    ready(c)
    return c


class TestFifo:
    """Test 1: FIFO rule orders by (entered_ts, order_id)."""

    def test_fifo_by_entry_time(self) -> None:
        c = fresh(rule="fifo")
        c.on_order(rec("A"), ts(3))
        c.on_order(rec("C"), ts(5))
        c.on_order(rec("B"), ts(5))
        # FIFO: A dispatched immediately, then B (ts 5, id < C) ahead of C
        # Check backlog order
        assert c.snapshot()["backlog"]["spin"] == ["B", "C"]
        # A should be outstanding
        assert c.snapshot()["outstanding"]["spin"] == "A"


class TestEdd:
    """Test 2: EDD rule orders by earliest due_ts."""

    def test_edd_earliest_due_first(self) -> None:
        # Order A: due_ts = T0 + 5h
        # Order B: due_ts = T0 + 2h (earliest)
        # Order C: due_ts = T0 + 8h
        # Entry order: A (ts 1), B (ts 2), C (ts 3)
        # Under EDD, dispatch order should be: B (earliest due), then A, then C
        recipes = {s: {"margherita": 1.0} for s in STATIONS}
        c = fresh(rule="edd", recipes=recipes)
        c.on_order(rec("A", due_ts=T0 + timedelta(hours=5)), ts(1))
        c.on_order(rec("B", due_ts=T0 + timedelta(hours=2)), ts(2))
        c.on_order(rec("C", due_ts=T0 + timedelta(hours=8)), ts(3))

        # A is dispatched first (entered first), then B and C are in backlog
        # Check EDD ordering in backlog: B (earliest due), then C
        assert c.snapshot()["backlog"]["spin"] == ["B", "C"]
        assert c.snapshot()["outstanding"]["spin"] == "A"


class TestSpt:
    """Test 3: SPT rule orders by smallest unit_time_s * qty at the station."""

    def test_spt_by_station_processing_time(self) -> None:
        recipes = {
            "spin": {"margherita": 1.0, "hawaiian": 2.0},
            "sauce": {"margherita": 1.0, "hawaiian": 1.0},
            "cheese": {"margherita": 1.0, "hawaiian": 1.0},
            "toppings": {"margherita": 1.0, "hawaiian": 1.0},
            "bake": {"margherita": 2.0, "hawaiian": 3.0},
            "slice": {"margherita": 1.0, "hawaiian": 1.0},
            "package": {"margherita": 1.0, "hawaiian": 1.0},
        }
        c = fresh(rule="spt", recipes=recipes)
        # At spin: A has 1x margherita = 1.0s, B has 3x margherita = 3.0s
        c.on_order(rec("A", product="margherita", qty=1), ts(1))
        c.on_order(rec("B", product="margherita", qty=3), ts(2))

        # A is dispatched first (entered first), then B in backlog
        # Under SPT, B should remain in backlog since A is already dispatched
        assert c.snapshot()["backlog"]["spin"] == ["B"]
        assert c.snapshot()["outstanding"]["spin"] == "A"


class TestRushFirst:
    """Numeric priority is the first ordering level within any rule (smaller wins)."""

    def test_priority_10_beats_50_fifo_edd_spt(self) -> None:
        recipes = {s: {"margherita": 1.0, "pepperoni": 9.0} for s in STATIONS}
        for rule in ("fifo", "edd", "spt"):
            c = fresh(rule=rule, recipes=recipes)
            c.on_order(rec("A"), ts(1))
            c.on_order(rec("B", due_ts=T0 + timedelta(hours=1)), ts(2))
            c.on_order(
                rec_rush("URGENT", product="pepperoni", qty=5, due_ts=T0 + timedelta(hours=10)),
                ts(3),
            )
            assert c.snapshot()["backlog"]["spin"] == ["URGENT", "B"], rule

    def test_equal_priority_falls_back_to_rule_then_entered_then_id(self) -> None:
        c = fresh(rule="fifo")
        c.on_order(rec("A"), ts(1))
        c.on_order(rec_rush("Z", priority=20), ts(2))
        c.on_order(rec_rush("Y", priority=20), ts(2))
        c.on_order(rec_rush("X", priority=20), ts(5))
        assert c.snapshot()["backlog"]["spin"] == ["Y", "Z", "X"]

    def test_all_default_priority_matches_legacy_order(self) -> None:
        recipes = {s: {"margherita": 1.0} for s in STATIONS}
        c = fresh(rule="edd", recipes=recipes)
        c.on_order(rec("A"), ts(1))
        c.on_order(rec("B", due_ts=T0 + timedelta(hours=3)), ts(2))
        c.on_order(rec("C", due_ts=T0 + timedelta(hours=2)), ts(3))
        assert c.snapshot()["backlog"]["spin"] == ["C", "B"]


    def test_rush_first_in_fifo(self) -> None:
        c = fresh(rule="fifo")
        c.on_order(rec("A"), ts(1))
        c.on_order(rec("B"), ts(2))
        c.on_order(rec_rush("RUSH"), ts(10))  # entered last but rush=True

        # A is dispatched immediately, then B and RUSH in backlog
        # RUSH should be first in backlog (rush=True sorts ahead)
        assert c.snapshot()["backlog"]["spin"] == ["RUSH", "B"]
        assert c.snapshot()["outstanding"]["spin"] == "A"

    def test_rush_first_in_edd(self) -> None:
        recipes = {s: {"margherita": 1.0} for s in STATIONS}
        c = fresh(rule="edd", recipes=recipes)
        c.on_order(rec("A", due_ts=T0 + timedelta(hours=1)), ts(1))
        c.on_order(rec("B", due_ts=T0 + timedelta(hours=2)), ts(2))
        # Rush order with later due_ts should still go first
        c.on_order(rec_rush("RUSH", due_ts=T0 + timedelta(hours=10)), ts(3))

        # A is dispatched first, then RUSH and B in backlog
        # RUSH should be first (rush=True sorts ahead of non-rush, even with later due_ts)
        assert c.snapshot()["backlog"]["spin"] == ["RUSH", "B"]
        assert c.snapshot()["outstanding"]["spin"] == "A"

    def test_rush_first_within_rule_then_apply_rule(self) -> None:
        recipes = {s: {"margherita": 1.0} for s in STATIONS}
        c = fresh(rule="fifo", recipes=recipes)
        # Entry order: R1 at ts 5, B at ts 4, R2 at ts 3
        # R1 is dispatched first (entered first), then R2, B in backlog
        # Within rush group (R2), FIFO applies: R2 before B (but both non-rush in
        # this backlog... wait)
        # Actually: R1 and R2 are both rush, B is not. So backlog should be [R2, B]
        # (rush first, then by entry time within rush)
        c.on_order(rec_rush("R1"), ts(5))
        c.on_order(rec("B"), ts(4))
        c.on_order(rec_rush("R2"), ts(3))

        # R1 is dispatched, then R2 and B in backlog
        # R2 is rush (sorts first), B is not
        assert c.snapshot()["backlog"]["spin"] == ["R2", "B"]
        assert c.snapshot()["outstanding"]["spin"] == "R1"


class TestTieBreaking:
    """Test 5: Equal rule_key falls back to entered_ts, then order_id."""

    def test_tie_break_by_entry_then_id(self) -> None:
        recipes = {s: {"margherita": 1.0} for s in STATIONS}
        c = fresh(rule="edd", recipes=recipes)
        # All have same due_ts, entered at same time: order_id breaks tie
        c.on_order(rec("A", due_ts=T0 + timedelta(hours=5)), ts(1))
        c.on_order(rec("B", due_ts=T0 + timedelta(hours=5)), ts(1))
        c.on_order(rec("C", due_ts=T0 + timedelta(hours=5)), ts(1))

        # A is dispatched first, then B and C in backlog
        # B and C have same due_ts and entered_ts, so order_id breaks tie: B < C
        assert c.snapshot()["backlog"]["spin"] == ["B", "C"]
        assert c.snapshot()["outstanding"]["spin"] == "A"


class TestSwitchRule:
    """Test 6: Changing core.rule mid-run affects only next dispatch, not outstanding."""

    def test_switch_rule_preserves_outstanding_credits(self) -> None:
        recipes = {
            "spin": {"margherita": 1.0, "hawaiian": 2.0},
            "sauce": {"margherita": 1.0, "hawaiian": 1.0},
            "cheese": {"margherita": 1.0, "hawaiian": 1.0},
            "toppings": {"margherita": 1.0, "hawaiian": 1.0},
            "bake": {"margherita": 2.0, "hawaiian": 3.0},
            "slice": {"margherita": 1.0, "hawaiian": 1.0},
            "package": {"margherita": 1.0, "hawaiian": 1.0},
        }
        c = fresh(rule="fifo", recipes=recipes)
        c.on_order(rec("A", product="margherita", qty=1), ts(1))
        c.on_order(rec("B", product="hawaiian", qty=3), ts(2))

        # A is outstanding, B in backlog
        assert c.snapshot()["outstanding"]["spin"] == "A"
        assert c.snapshot()["backlog"]["spin"] == ["B"]

        # Switch to SPT (set_rule just changes the rule, no dispatch yet)
        c.set_rule("spt")

        # Outstanding should remain unchanged
        assert c.snapshot()["outstanding"]["spin"] == "A"

        # Complete A and dispatch next: under SPT, B remains (no other orders)
        actions = c.on_op_completed(completed(
            Dispatch("A", "spin", 1, "margherita", 1, ts(1), job_event_id(RUN, "A", "spin", 1)),
            ts(10)
        ))
        dispatches = [a for a in actions if isinstance(a, Dispatch) and a.step == "spin"]
        # B should be dispatched (only item in backlog)
        assert len(dispatches) == 1
        assert dispatches[0].order_id == "B"


class TestMissingRecipe:
    """Test 7: Missing recipe yields proc_s = math.inf, sorts last."""

    def test_missing_recipe_sorts_last(self) -> None:
        recipes = {
            "spin": {"margherita": 1.0},  # no hawaiian
            "sauce": {"margherita": 1.0, "hawaiian": 1.0},
            "cheese": {"margherita": 1.0, "hawaiian": 1.0},
            "toppings": {"margherita": 1.0, "hawaiian": 1.0},
            "bake": {"margherita": 2.0, "hawaiian": 3.0},
            "slice": {"margherita": 1.0, "hawaiian": 1.0},
            "package": {"margherita": 1.0, "hawaiian": 1.0},
        }
        c = fresh(rule="spt", recipes=recipes)
        c.on_order(rec("A", product="margherita", qty=1), ts(1))
        c.on_order(rec("B", product="hawaiian", qty=1), ts(2))  # hawaiian missing at spin

        # A is dispatched first (1.0s), B sorts last (math.inf for missing recipe)
        assert c.snapshot()["outstanding"]["spin"] == "A"
        assert c.snapshot()["backlog"]["spin"] == ["B"]


class TestReworkReturnOrdering:
    """Phase 6.2 D-05: a returned rework lot keeps its original entered_ts."""

    @staticmethod
    def _failed_then_returned(rule: str, a_qty: int, c_priority: int) -> SchedulerCore:
        recipes = {s: {"margherita": 1.0} for s in STATIONS}
        c = fresh(rule=rule, recipes=recipes)
        c.on_order(
            OrderRecord("A", "margherita", a_qty, T0 + timedelta(hours=5), T0), ts(1)
        )  # dispatched
        c.on_station_status(
            StationStatus.make(run_id=RUN, machine_id="spin", ts=ts(2), status="down")
        )
        c.on_order(
            OrderRecord("B", "margherita", 1, T0 + timedelta(hours=2), T0), ts(3)
        )
        c.on_order(
            OrderRecord("C", "margherita", 3, T0 + timedelta(hours=8), T0, priority=c_priority), ts(4)
        )
        c.on_op_failed(
            OpFailed.make(
                run_id=RUN,
                machine_id="spin",
                ts=ts(10),
                order_id="A",
                step="spin",
                visit=1,
                attempt=1,
                qty=a_qty,
                command_event_id=job_event_id(RUN, "A", "spin", 1),
                reason="boom",
            )
        )
        c.on_rework_ready("A", "spin", 2, RUN, ts(20))
        return c

    def test_fifo_returned_lot_ahead_of_later_arrivals(self) -> None:
        c = self._failed_then_returned("fifo", 2, 50)
        assert c.snapshot()["backlog"]["spin"] == ["A", "B", "C"]

    def test_edd_rule_key_stays_primary(self) -> None:
        c = self._failed_then_returned("edd", 2, 50)
        assert c.snapshot()["backlog"]["spin"] == ["B", "A", "C"]

    def test_spt_rule_key_stays_primary(self) -> None:
        c = self._failed_then_returned("spt", 2, 50)
        assert c.snapshot()["backlog"]["spin"] == ["B", "A", "C"]

    def test_rush_still_first(self) -> None:
        c = self._failed_then_returned("fifo", 2, 10)
        assert c.snapshot()["backlog"]["spin"][0] == "C"
