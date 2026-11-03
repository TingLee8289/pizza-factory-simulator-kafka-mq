import uuid
from typing import Any

from pizza.common.constants import STATIONS
from pizza.common.envelope import (
    Message,
    OpCompleted,
    OpDispatched,
    OpStarted,
    OrderLifecycle,
    RunParams,
    RunStarted,
    StationStatus,
)
from pizza.kpi.fold import KpiState, fold, new_state, newest_run_id
from tests.unit.kpi_fixtures import (
    FIXTURE_PATH,
    RECORDED_ARGS,
    RUN,
    T0,
    at,
    from_jsonl,
    shuffled,
    simulate_line,
    with_duplicates,
)


def fold_all(msgs: list[Message], run_id: str | None = RUN) -> KpiState:
    state = new_state(run_id)
    for m in msgs:
        state = fold(state, m)
    return state


def canonical(state: KpiState) -> dict[str, Any]:
    return {
        "run_id": state.run_id,
        "weights": state.weights,
        "orders": {k: vars(v) for k, v in state.orders.items()},
        "ops": {k: vars(v) for k, v in state.ops.items()},
        "status": {s: sorted((t, st) for t, st, _ in log) for s, log in state.status_logs.items()},
        "last_event_ts": state.last_event_ts,
    }


def small() -> list[Message]:
    return simulate_line(RECORDED_ARGS["orders"], track_in_s=0.5).messages


def op_ids() -> dict[str, Any]:
    return dict(
        order_id="o1", step="spin", visit=1, attempt=1, qty=1, command_event_id=uuid.uuid4()
    )


def test_order_insensitive() -> None:
    msgs = small()
    base = canonical(fold_all(msgs))
    assert base["orders"] and base["ops"]
    for seed in range(20):
        assert canonical(fold_all(shuffled(msgs, seed))) == base


def test_duplicates_ignored() -> None:
    msgs = small()
    base = canonical(fold_all(msgs))
    assert canonical(fold_all(with_duplicates(msgs, 3, 5))) == base
    assert canonical(fold_all(with_duplicates(msgs, 4, 100))) == base


def test_recorded_jsonl_matches_simulation() -> None:
    recorded = from_jsonl(FIXTURE_PATH.read_text(encoding="utf-8"))
    sim = simulate_line(**RECORDED_ARGS)
    assert canonical(fold_all(recorded)) == canonical(fold_all(sim.messages))
    assert recorded == sim.messages


def test_fixture_has_bake_wait_gt_zero() -> None:
    state = fold_all(from_jsonl(FIXTURE_PATH.read_text(encoding="utf-8")))
    waits = [
        (r.dispatched_ts - r.ready_ts).total_seconds()
        for (_, step, _), r in state.ops.items()
        if step == "bake"
    ]
    assert len(waits) == 6
    assert max(waits) > 0


def test_three_dedupers() -> None:
    cmd = uuid.uuid4()
    common = dict(order_id="o1", step="bake", visit=1, qty=1, command_event_id=cmd)
    disp = OpDispatched.make(run_id=RUN, machine_id="scheduler", ts=at(1), ready_ts=at(0), **common)
    start = OpStarted.make(
        run_id=RUN, machine_id="bake", ts=at(2), attempt=1, redelivered=False, **common
    )
    done = OpCompleted.make(
        run_id=RUN, machine_id="bake", ts=at(3), attempt=1, duration_s=1.0, **common
    )
    state = fold_all([disp, start, done])
    rec = state.ops[("o1", "bake", 1)]
    assert (rec.dispatched_ts, rec.started_ts, rec.completed_ts) == (at(1), at(2), at(3))
    assert rec.ready_ts == at(0)


def test_first_started_only() -> None:
    ids = op_ids()
    early = OpStarted.make(run_id=RUN, machine_id="spin", ts=at(5), redelivered=False, **ids)
    late = OpStarted.make(
        run_id=RUN, machine_id="spin", ts=at(9), redelivered=True, **{**ids, "attempt": 2}
    )
    state = fold_all([early, late])
    assert state.ops[("o1", "spin", 1)].started_ts == at(5)


def test_redelivered_later_start_first() -> None:
    ids = op_ids()
    early = OpStarted.make(run_id=RUN, machine_id="spin", ts=at(5), redelivered=False, **ids)
    late = OpStarted.make(
        run_id=RUN, machine_id="spin", ts=at(9), redelivered=True, **{**ids, "attempt": 2}
    )
    state = fold_all([late, early])
    assert state.ops[("o1", "spin", 1)].started_ts == at(5)


def test_completed_before_created_and_unknown_step() -> None:
    life = dict(order_id="o9", product="veggie", qty=2, due_ts=at(3600), created_ts=at(10))
    done = OrderLifecycle.make(
        run_id=RUN, machine_id="scheduler", ts=at(50), state="completed", **life
    )
    made = OrderLifecycle.make(
        run_id=RUN, machine_id="scheduler", ts=at(10), state="created", **life
    )
    state = fold_all([done, made])
    rec = state.orders["o9"]
    assert (rec.created_ts, rec.completed_ts, rec.released_ts) == (at(10), at(50), None)
    bogus = OpStarted.make(
        run_id=RUN, machine_id="x", ts=at(1), redelivered=False, **{**op_ids(), "step": "nope"}
    )
    assert fold_all([bogus]).ops == {}


def test_station_status_log() -> None:
    a = StationStatus.make(run_id=RUN, machine_id="bake", ts=at(1), status="busy")
    b = StationStatus.make(run_id=RUN, machine_id="bake", ts=at(2), status="idle")
    state = fold_all([b, a, a])
    assert sorted((t, s) for t, s, _ in state.status_logs["bake"]) == [
        (at(1), "busy"),
        (at(2), "idle"),
    ]
    assert state.last_event_ts == at(2)
    assert "bake" in STATIONS


def test_foreign_run_ignored() -> None:
    msgs = small()
    state = fold_all(msgs)
    before = canonical(state)
    foreign = simulate_line([("z1", "veggie", 1, 0.0)], run_id="other").messages
    # older created_ts than last_event_ts, so it must not be adopted
    for m in foreign:
        state = fold(state, m)
    assert state.run_id == RUN
    assert canonical(state) == before


def test_newest_run_id() -> None:
    assert newest_run_id([]) is None
    old = simulate_line([("a", "veggie", 1, 0.0)], run_id="old").messages
    new = simulate_line([("b", "veggie", 1, 500.0)], run_id="new").messages
    assert newest_run_id(old + new) == "new"
    assert newest_run_id(m for m in old if not isinstance(m, OrderLifecycle)) is None


def run_started(run_id: str, when: float, weights: dict[str, float] | None = None) -> RunStarted:
    params = RunParams(weights=weights) if weights is not None else None
    return RunStarted.make(run_id=run_id, machine_id="scheduler", ts=at(when), params=params)


def created(run_id: str, created_s: float, oid: str = "n1") -> OrderLifecycle:
    return OrderLifecycle.make(
        run_id=run_id,
        machine_id="scheduler",
        ts=at(created_s),
        state="created",
        order_id=oid,
        product="veggie",
        qty=1,
        due_ts=at(created_s + 3600),
        created_ts=at(created_s),
    )


def test_run_started_resets() -> None:
    state = fold_all(small())
    assert state.orders
    new = fold(state, run_started("r2", 1000.0, {"margherita": 2.0}))
    assert new is not state
    assert new.run_id == "r2"
    assert new.orders == {} and new.ops == {} and new.status_logs == {}
    assert new.weights == {"margherita": 2.0}
    assert new.weights_source == "run_started"
    default = fold(state, run_started("r3", 1000.0))
    assert default.weights == {"margherita": 1.0, "pepperoni": 1.0, "veggie": 1.0}


def test_same_run_upgrades_weights() -> None:
    state = fold_all(small())
    n_orders = len(state.orders)
    same = fold(state, run_started(RUN, 1.0, {"veggie": 3.0}))
    assert same is state
    assert len(same.orders) == n_orders
    assert same.weights == {"veggie": 3.0}
    assert same.weights_source == "run_started"


def test_adopt_newer_run() -> None:
    state = fold_all(small())
    assert state.last_event_ts is not None
    later = (state.last_event_ts - T0).total_seconds() + 100.0
    adopted = fold(state, created("r9", later))
    assert adopted.run_id == "r9"
    assert adopted.weights_source == "observed"
    assert "n1" in adopted.orders and "o1" not in adopted.orders
    assert fold(new_state(None), created("r9", 5.0)).run_id == "r9"


def test_adopt_ignores_older_run() -> None:
    state = fold_all(small())
    assert fold(state, created("r9", 1.0)).run_id == RUN


def test_fresh_state_does_not_adopt() -> None:
    state = new_state(RUN)
    assert fold(state, created("r9", 500.0)).run_id == RUN


def test_snapshot_order_insensitive() -> None:
    from pizza.common.recipes import load_all_recipes
    from pizza.kpi.metrics import KpiConfig, advance

    sim = simulate_line(RECORDED_ARGS["orders"], track_in_s=0.5)
    recipes = load_all_recipes()
    cfg = KpiConfig(warmup_orders=1)
    as_of = at(400.0)
    base = advance(fold_all(sim.messages), as_of, recipes, cfg).model_dump(exclude={"event_id"})
    for seed in range(10):
        msgs = with_duplicates(shuffled(sim.messages, seed), seed, 15)
        got = advance(fold_all(msgs), as_of, recipes, cfg).model_dump(exclude={"event_id"})
        assert got == base


def test_apply_run_params_valid_and_invalid() -> None:
    from pizza.kpi.fold import apply_run_params

    state = new_state(RUN)
    assert apply_run_params(state, {"veggie": 2.0}, at(5.0)) is state
    assert state.weights == {"veggie": 2.0}
    assert state.weights_source == "run_started"
    assert state.run_started_ts == at(5.0)
    for bad in ({}, None, {"veggie": -1.0}, {"veggie": 0.0}):
        s = new_state(RUN)
        assert apply_run_params(s, bad, at(5.0)) is s  # type: ignore[arg-type]
        assert s.weights is None and s.weights_source == "observed"
        assert s.run_started_ts is None


def test_advance_emits_run_params_only_for_run_started() -> None:
    from pizza.common.recipes import load_all_recipes
    from pizza.kpi.metrics import KpiConfig, advance

    recipes = load_all_recipes()
    cfg = KpiConfig(warmup_orders=1)
    sim = simulate_line(RECORDED_ARGS["orders"], track_in_s=0.5)
    state = fold_all(sim.messages)
    snap = advance(state, at(400.0), recipes, cfg)
    assert snap.run_weights is None and snap.run_started_ts is None
    state = fold(state, run_started(RUN, 1.0, {"veggie": 3.0, "margherita": 1.0}))
    snap = advance(state, at(400.0), recipes, cfg)
    assert snap.run_weights == {"margherita": 1.0, "veggie": 3.0}
    assert snap.run_started_ts == at(1.0)
