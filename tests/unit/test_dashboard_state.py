from pizza.common.constants import PRODUCTS, STATIONS
from pizza.common.envelope import (
    KpiSnapshot,
    LotBanked,
    LotHold,
    LotReleased,
    LotStocked,
    LotUnstocked,
    OpCompleted,
    OpDispatched,
    OpFailed,
    OpStarted,
    OrderLifecycle,
    PriorityChanged,
    RecipeChanged,
    RecipeSnapshot,
    StationHeartbeat,
    StationStatus,
)
from pizza.dashboard import state as state_mod
from pizza.dashboard.state import (
    active_lots,
    event_payload,
    fold,
    line_snapshot,
    new_line_state,
    new_run_state,
)
from tests.unit.dashboard_fixtures import (
    RUN,
    at,
    full_timeline,
    lifecycle,
    op,
    pause_fact,
    resume_fact,
    shuffled,
    status,
    with_duplicates,
)
from tests.unit.test_kpi_snapshot_schema import _full


def _run(msgs, run_id=RUN, adopt=True):
    st = new_line_state(run_id, adopt=adopt)
    for m in msgs:
        st = fold(st, m)
    return st


def _lot(snap, oid="o1"):
    return next(lt for lt in snap["lots"] if lt["order_id"] == oid)


def test_recipe_facts_are_invisible():
    """Recipe facts stay out of _KINDS: no payload, and the line state is unchanged."""
    full = {station: {product: 1.0 for product in PRODUCTS} for station in STATIONS}
    snap = RecipeSnapshot.make(run_id=RUN, machine_id="scheduler", recipes=full)
    chg = RecipeChanged.make(
        run_id=RUN,
        machine_id="bake",
        station="bake",
        product="margherita",
        old_unit_time_s=1.0,
        new_unit_time_s=2.0,
    )
    state = new_line_state(RUN)
    before = line_snapshot(state)

    assert event_payload(snap) is None
    assert event_payload(chg) is None
    assert line_snapshot(fold(state, snap)) == before
    assert line_snapshot(fold(state, chg)) == before
    assert RecipeSnapshot not in state_mod._KINDS
    assert RecipeChanged not in state_mod._KINDS


def test_created_only_is_pre_release_and_in_no_queue():
    snap = line_snapshot(_run([lifecycle("created")]))
    assert _lot(snap)["state"] == "pre_release"
    assert all(s["queue"] == 0 for s in snap["stations"].values())
    assert snap["counts"]["wip"] == 0 and snap["counts"]["completed"] == 0


def test_released_is_queued_at_first_step():
    snap = line_snapshot(_run([lifecycle("created"), lifecycle("released", offset=1)]))
    lot = _lot(snap)
    assert lot["state"] == "queued"
    assert lot["station"] == "spin"
    assert snap["stations"]["spin"]["queue"] == 1
    assert snap["counts"]["wip"] == 1


def test_dispatched_still_queued_then_running_then_next_station():
    base = [lifecycle("created"), lifecycle("released", offset=1)]
    snap = line_snapshot(_run([*base, op(OpDispatched, "o1", "spin", 2)]))
    assert _lot(snap)["state"] == "queued"
    assert snap["stations"]["spin"]["queue"] == 1

    started = [*base, op(OpDispatched, "o1", "spin", 2), op(OpStarted, "o1", "spin", 3)]
    snap = line_snapshot(_run(started))
    assert _lot(snap)["state"] == "running"
    assert snap["stations"]["spin"]["current_lot"] == "o1"
    assert snap["stations"]["spin"]["queue"] == 0

    completed = [*started, op(OpCompleted, "o1", "spin", 4)]
    snap = line_snapshot(_run(completed))
    lot = _lot(snap)
    assert (lot["state"], lot["station"], lot["steps_done"]) == ("queued", "sauce", 1)
    assert snap["stations"]["sauce"]["queue"] == 1
    assert snap["stations"]["spin"]["current_lot"] is None


def test_package_completion_is_done():
    msgs = full_timeline(1)
    snap = line_snapshot(_run(msgs))
    lot = _lot(snap)
    assert lot["state"] == "done"
    assert lot["station"] is None
    assert lot["steps_done"] == len(STATIONS)
    assert snap["counts"]["wip"] == 0 and snap["counts"]["completed"] == 1
    assert active_lots(snap) == []


def test_station_status_latest_wins_default_offline_unknown_ignored():
    msgs = [
        status("bake", "busy", 5),
        status("bake", "idle", 9),
        status("bake", "busy", 7),
        status("oven", "busy", 1),
    ]
    snap = line_snapshot(_run(msgs))
    assert snap["stations"]["bake"]["status"] == "idle"
    assert snap["stations"]["bake"]["since_ts"] is not None
    assert snap["stations"]["slice"]["status"] == "offline"
    assert "oven" not in snap["stations"]


def test_paused_derived_from_newest_fact():
    assert line_snapshot(_run([]))["paused"] is False
    assert line_snapshot(_run([pause_fact(1)]))["paused"] is True
    assert line_snapshot(_run([pause_fact(1), resume_fact(2)]))["paused"] is False
    assert line_snapshot(_run([resume_fact(2), pause_fact(3), pause_fact(1)]))["paused"] is True
    # ties resolve to resumed
    assert line_snapshot(_run([pause_fact(2), resume_fact(2)]))["paused"] is False


def test_shuffle_and_duplicates_do_not_change_snapshot():
    msgs = full_timeline(8)
    msgs = [*msgs, pause_fact(30), resume_fact(40), pause_fact(50)]
    expected = line_snapshot(_run(msgs))
    for seed in range(5):
        assert line_snapshot(_run(shuffled(msgs, seed))) == expected
        assert line_snapshot(_run(with_duplicates(msgs, seed, 40))) == expected


def test_partial_prefix_is_shuffle_invariant():
    msgs = full_timeline(8)
    cut = msgs[: len(msgs) // 2]
    expected = line_snapshot(_run(cut))
    for seed in range(5):
        assert line_snapshot(_run(shuffled(cut, seed))) == expected


def test_other_run_facts_ignored():
    st = _run([lifecycle("created"), lifecycle("created", "x9", run_id="other", offset=5)])
    assert [lt["order_id"] for lt in line_snapshot(st)["lots"]] == ["o1"]


def test_adopt_true_follows_run_started_elsewhere():
    st = new_line_state(None, adopt=True)
    st = fold(st, lifecycle("released", run_id="r9"))  # not a created fact: ignored
    assert st.run_id is None
    st = fold(st, lifecycle("created", run_id="r9"))
    assert st.run_id == "r9"
    assert line_snapshot(st)["run_id"] == "r9"


def test_station_idle_of_new_run_adopts_it_before_any_order():
    # RunStarted makes each station report idle before the first order exists.
    st = fold(new_line_state(None, adopt=True), status("bake", "idle", 1, run_id="r9"))
    assert st.run_id == "r9"
    assert line_snapshot(st)["stations"]["bake"]["status"] == "idle"


def test_station_status_adoption_ignores_unknown_run_and_stale_reports():
    st = fold(new_line_state(None, adopt=True), status("bake", "idle", 1, run_id="unknown"))
    assert st.run_id is None
    st = fold(new_line_state("r1", adopt=True), lifecycle("created", "o1", run_id="r1", offset=10))
    st = fold(st, status("bake", "idle", 2, run_id="r0"))
    assert st.run_id == "r1"


def test_new_run_state_keeps_station_reports_that_arrived_before_start():
    # Station idle for run r9 can be folded before the start callback resets the state.
    early = [status("bake", "idle", 1, run_id="r9"), status("oven", "busy", 2, run_id="r8")]
    st = new_run_state("r9", early)
    snap = line_snapshot(st)
    assert st.run_id == "r9"
    assert snap["stations"]["bake"]["status"] == "idle"
    assert "oven" not in snap["stations"]


def test_adopt_false_never_adopts():
    st = new_line_state(None, adopt=False)
    st = fold(st, lifecycle("created", run_id="r9"))
    assert st.run_id is None
    assert line_snapshot(st)["lots"] == []
    st = new_line_state("r1", adopt=False)
    st = fold(st, lifecycle("created", run_id="r2", offset=100))
    assert st.run_id == "r1"


def test_completed_lots_pruned_and_counted(monkeypatch):
    unpruned = line_snapshot(_run(full_timeline(9)))["counts"]["completed_pizzas"]
    assert unpruned > 0
    monkeypatch.setattr(state_mod, "MAX_COMPLETED_LOTS", 5)
    st = _run(full_timeline(9))
    assert len(st.lots) == 5
    assert len(st.steps) == 5
    snap = line_snapshot(st)
    assert snap["counts"]["completed"] == 9
    assert snap["counts"]["completed_pizzas"] == unpruned
    assert st.pruned_completed_pizzas > 0
    kept = {lt["order_id"] for lt in snap["lots"]}
    assert kept == {f"o{i}" for i in range(5, 10)}
    # stragglers of a pruned lot do not resurrect it
    st = fold(st, lifecycle("created", "o1"))
    assert "o1" not in st.lots


def test_snapshot_limits_done_lots_to_newest_50():
    snap = line_snapshot(_run(full_timeline(60)))
    assert len(snap["lots"]) == 50
    assert snap["counts"]["completed"] == 60


def test_lot_cap(monkeypatch):
    monkeypatch.setattr(state_mod, "MAX_ORDERS_CAP", 3)
    st = _run([lifecycle("created", f"l{i}") for i in range(10)])
    assert len(st.lots) == 3


def test_seen_ids_capped(monkeypatch):
    monkeypatch.setattr(state_mod, "MAX_SEEN_IDS_CAP", 4)
    st = _run([status("bake", "busy", i) for i in range(10)])
    assert len(st.seen_event_ids) <= 4


def test_newest_lifecycle_priority_wins_and_source_kept():
    st = _run(
        [
            lifecycle("created", priority=50, source=None),
            lifecycle("released", offset=1, priority=10, source="manual"),
        ]
    )
    lot = _lot(line_snapshot(st))
    assert lot["priority"] == 10 and "rush" not in lot
    assert lot["source"] == "manual"


def _change(offset, new, order_id="o1", old=50):
    return PriorityChanged.make(
        run_id=RUN,
        machine_id="scheduler",
        ts=at(offset),
        order_id=order_id,
        previous_priority=old,
        new_priority=new,
        source="manual",
    )


def test_priority_change_overrides_and_stale_lifecycle_ignored():
    st = _run([lifecycle("created", priority=50), _change(5, 7)])
    assert _lot(line_snapshot(st))["priority"] == 7
    # A lifecycle fact older than the change (late/replayed) must not overwrite it.
    st = fold(st, lifecycle("released", offset=2, priority=50))
    assert _lot(line_snapshot(st))["priority"] == 7
    # A lifecycle fact newer than the change carries the current priority.
    st = fold(st, lifecycle("completed", offset=9, priority=7))
    assert _lot(line_snapshot(st))["priority"] == 7


def test_priority_order_insensitive():
    # Changes for a lot are only applied once the lot exists (same as other lot facts).
    st = _run([lifecycle("created", priority=50), _change(8, 3, old=7), _change(5, 7)])
    assert _lot(line_snapshot(st))["priority"] == 3


def test_priority_tie_goes_to_change_fact():
    st = _run([lifecycle("created", priority=50, offset=3.0), _change(3.0, 7)])
    assert _lot(line_snapshot(st))["priority"] == 7
    # A later duplicate-ts lifecycle (strictly-newer rule) cannot undo the change.
    st = fold(st, lifecycle("released", priority=50, offset=3.0))
    assert _lot(line_snapshot(st))["priority"] == 7


def test_priority_change_dedup_unknown_and_pruned():
    st = _run([lifecycle("created", priority=50)])
    ch = _change(5, 7)
    st = fold(st, ch)
    st = fold(st, ch)  # duplicate event_id: no second effect
    assert _lot(line_snapshot(st))["priority"] == 7
    # Unknown lot: ignored without creating a record.
    st = fold(st, _change(6, 4, order_id="ghost"))
    assert [lt["order_id"] for lt in line_snapshot(st)["lots"]] == ["o1"]
    # Pruned lot: ignored.
    st.pruned_ids.add("o1")
    st.lots.pop("o1")
    st = fold(st, _change(7, 4))
    assert line_snapshot(st)["lots"] == []


def test_priority_changed_has_no_live_feed_row():
    assert event_payload(_change(1, 7)) is None


def test_event_payload_per_type():
    d = event_payload(op(OpDispatched, "o1", "bake", 1))
    assert d["kind"] == "op_dispatched"
    assert (d["station"], d["order_id"], d["step"], d["visit"]) == ("bake", "o1", "bake", 1)
    assert d["ts"].endswith("Z")
    assert event_payload(op(OpStarted, "o1", "bake", 2))["kind"] == "op_started"
    assert event_payload(op(OpCompleted, "o1", "bake", 3))["kind"] == "op_completed"
    s = event_payload(status("bake", "busy", 1))
    assert (s["kind"], s["station"], s["status"]) == ("station_status", "bake", "busy")
    life = event_payload(lifecycle("released", priority=10))
    assert (life["kind"], life["state"]) == ("order_lifecycle", "released")
    assert "rush" not in life
    assert life["due_ts"].endswith("Z") and life["created_ts"].endswith("Z")
    assert life["station"] is None and life["status"] is None
    assert event_payload(pause_fact(1))["kind"] == "run_paused"
    assert event_payload(resume_fact(1))["kind"] == "run_resumed"


def test_event_payload_none_for_other_types():
    snap = _full()
    assert isinstance(snap, KpiSnapshot)
    assert event_payload(snap) is None


# -- Phase 6: failures, stocker, BANK ------------------------------------------


def _released(oid="o1"):
    return [lifecycle("created", oid), lifecycle("released", oid, offset=1)]


def _lotfact(cls, offset, oid="o1", station="spin", **kw):
    return cls.make(
        run_id=RUN, machine_id="scheduler", ts=at(offset), order_id=oid, station=station, **kw
    )


def test_station_down_cause_and_eta_then_idle_clears():
    down = StationStatus.make(
        run_id=RUN, machine_id="bake", ts=at(5), status="down", cause="manual", repair_in_s=30
    )
    snap = line_snapshot(_run([down]))
    st = snap["stations"]["bake"]
    assert st["status"] == "down" and st["cause"] == "manual"
    assert st["repair_eta_ts"] == "2026-01-01T00:00:35.000Z"
    assert snap["stations"]["spin"]["status"] == "offline"
    st = line_snapshot(_run([down, status("bake", "idle", 40)]))["stations"]["bake"]
    assert st["status"] == "idle" and st["cause"] is None and st["repair_eta_ts"] is None


def test_stocker_overflow_and_unstock():
    stocked = _lotfact(LotStocked, 3, kind="overflow")
    snap = line_snapshot(_run([*_released(), stocked]))
    lot = _lot(snap)
    assert lot["state"] == "queued" and lot["stocker"]["kind"] == "overflow"
    assert snap["counts"]["wip"] == 1 and snap["counts"]["stocker_overflow"] == 1
    assert snap["stations"]["spin"]["queue"] == 1
    snap = line_snapshot(_run([*_released(), stocked, _lotfact(LotUnstocked, 4)]))
    assert _lot(snap)["stocker"] is None and snap["stations"]["spin"]["queue"] == 1


def test_stocker_down_blocks_station():
    snap = line_snapshot(_run([*_released(), _lotfact(LotStocked, 3, kind="station_down")]))
    assert snap["stations"]["spin"]["blocked"] == 1 and snap["counts"]["stocker_down"] == 1
    assert snap["stations"]["spin"]["queue"] == 1
    assert snap["counts"]["wip"] == 1


def test_stocker_overflow_and_down_both_count_in_station_queue():
    msgs = [
        *_released("o1"),
        *_released("o2"),
        _lotfact(LotStocked, 3, oid="o1", kind="overflow"),
        _lotfact(LotStocked, 4, oid="o2", kind="station_down"),
    ]
    snap = line_snapshot(_run(msgs))
    assert snap["stations"]["spin"]["queue"] == 2
    assert snap["stations"]["spin"]["blocked"] == 1
    assert snap["counts"]["wip"] == 2
    assert snap["counts"]["stocker_overflow"] == 1 and snap["counts"]["stocker_down"] == 1


def test_bank_release_and_shuffle_insensitive():
    msgs = [
        *_released(),
        _lotfact(LotStocked, 2, kind="overflow"),
        _lotfact(LotBanked, 3, reason="capacity", source="manual"),
    ]
    snap = line_snapshot(_run(msgs))
    lot = _lot(snap)
    assert lot["state"] == "banked" and lot["bank"]["reason"] == "capacity"
    assert lot["stocker"] is None
    assert snap["counts"]["wip"] == 0 and snap["counts"]["bank"] == 1
    full = [*msgs, _lotfact(LotReleased, 4, by="manual")]
    snap = line_snapshot(_run(full))
    assert _lot(snap)["state"] == "queued" and snap["counts"]["bank"] == 0
    assert line_snapshot(_run(shuffled(full, 3))) == snap


def test_failed_lifecycle():
    failed = OrderLifecycle.make(
        run_id=RUN,
        machine_id="scheduler",
        ts=at(9),
        state="failed",
        order_id="o1",
        product="margherita",
        qty=1,
        due_ts=at(3600.0),
        created_ts=at(0.0),
        step="spin",
        visit=3,
    )
    snap = line_snapshot(_run([*_released(), failed]))
    lot = _lot(snap)
    assert lot["state"] == "failed" and lot["failed"]["station"] == "spin"
    assert lot["failed"]["visit"] == 3
    assert snap["counts"]["wip"] == 0 and snap["counts"]["failed"] == 1
    assert snap["counts"]["failed_pizzas"] == 1 and snap["counts"]["completed_pizzas"] == 0


def test_rework_visit_info():
    msgs = [
        *_released(),
        op(OpDispatched, "o1", "spin", 2),
        op(OpStarted, "o1", "spin", 3),
        op(OpFailed, "o1", "spin", 4, reason="boom"),
        op(OpDispatched, "o1", "spin", 5, visit=2),
    ]
    lot = _lot(line_snapshot(_run(msgs)))
    assert lot["visit"] == 2 and lot["dispatched"] is True
    assert lot["retry_since_ts"] == "2026-01-01T00:00:04.000Z"
    lot = _lot(line_snapshot(_run([*msgs, op(OpStarted, "o1", "spin", 6, visit=2)])))
    assert lot["dispatched"] is False and lot["state"] == "running"


def test_cooling_lot_derivation():
    base = [
        *_released(),
        op(OpDispatched, "o1", "spin", 2),
        op(OpStarted, "o1", "spin", 3),
        op(OpFailed, "o1", "spin", 4, reason="boom"),
    ]
    lot = _lot(line_snapshot(_run(base)))
    assert lot["cooling"] is True and lot["visit"] == 2
    assert lot["state"] == "queued" and lot["dispatched"] is False
    assert lot["retry_since_ts"] == "2026-01-01T00:00:04.000Z"
    assert line_snapshot(_run(base))["counts"]["wip"] == 1
    lot = _lot(line_snapshot(_run([*base, op(OpDispatched, "o1", "spin", 5, visit=2)])))
    assert lot["cooling"] is False and lot["dispatched"] is True
    failed = OrderLifecycle.make(
        run_id=RUN,
        machine_id="scheduler",
        ts=at(9),
        state="failed",
        order_id="o1",
        product="margherita",
        qty=1,
        due_ts=at(3600.0),
        created_ts=at(0.0),
        step="spin",
        visit=1,
    )
    assert _lot(line_snapshot(_run([*base, failed])))["cooling"] is False
    done = [
        *_released(),
        op(OpDispatched, "o1", "spin", 2),
        op(OpStarted, "o1", "spin", 3),
        op(OpCompleted, "o1", "spin", 4),
    ]
    assert _lot(line_snapshot(_run(done)))["cooling"] is False


def test_cooling_ends_after_hold_ttl_but_lot_stays_rework_wait():
    """WR-02: past TTL + margin the lot is back in the queue: still visit 2, BANK-able."""
    base = [
        *_released(),
        op(OpDispatched, "o1", "spin", 2),
        op(OpStarted, "o1", "spin", 3),
        op(OpFailed, "o1", "spin", 4, reason="boom"),
    ]
    window = state_mod.COOLING_WINDOW.total_seconds()
    # Other line activity advances the fact clock.
    early = _lot(line_snapshot(_run([*base, status("bake", "idle", 4 + window - 0.5)])))
    assert early["cooling"] is True
    late = _lot(line_snapshot(_run([*base, status("bake", "idle", 4 + window + 0.5)])))
    assert late["cooling"] is False
    assert late["visit"] == 2 and late["state"] == "queued" and late["dispatched"] is False
    assert late["retry_since_ts"] == "2026-01-01T00:00:04.000Z"
    # A stocker fact at this station after the failure proves the hold came back.
    stocked = _lotfact(LotStocked, 5, kind="overflow")
    lot = _lot(line_snapshot(_run([*base, stocked])))
    assert lot["cooling"] is False and lot["visit"] == 2 and lot["stocker"] is not None
    # A mark from before the failure does not.
    old = _lotfact(LotUnstocked, 2)
    assert _lot(line_snapshot(_run([*base, old])))["cooling"] is True


def _failed_msgs():
    return [
        *_released(),
        op(OpDispatched, "o1", "spin", 2),
        op(OpStarted, "o1", "spin", 3),
        op(OpFailed, "o1", "spin", 4, reason="boom", cause="breakdown"),
        op(OpDispatched, "o1", "spin", 5, visit=2),
        op(OpStarted, "o1", "spin", 6, visit=2),
        op(OpFailed, "o1", "spin", 7, reason="bad", visit=2),
    ]


def test_lot_rework_count_across_stations():
    msgs = [
        *_released(),
        op(OpDispatched, "o1", "spin", 2),
        op(OpDispatched, "o1", "spin", 3, visit=2),
        op(OpDispatched, "o1", "package", 4),
        op(OpDispatched, "o1", "package", 5, visit=2),
    ]
    assert _lot(line_snapshot(_run(msgs)))["rework_count"] == 2
    assert _lot(line_snapshot(_run(_released())))["rework_count"] == 0


def test_lot_failures_with_cause_and_default_quality():
    lot = _lot(line_snapshot(_run(_failed_msgs())))
    assert [(f["station"], f["visit"], f["cause"]) for f in lot["failures"]] == [
        ("spin", 1, "breakdown"),
        ("spin", 2, "quality"),
    ]
    assert lot["failures"][0]["ts"] == "2026-01-01T00:00:04.000Z"
    assert lot["rework_count"] == 1


def test_failure_cause_idempotent_under_replay():
    base = _failed_msgs()
    dup = op(OpFailed, "o1", "spin", 4, reason="boom", cause="breakdown")
    late_other = op(OpFailed, "o1", "spin", 9, reason="x", cause="quality")
    early_other = op(OpFailed, "o1", "spin", 3.5, reason="x", cause="quality")
    lot = _lot(line_snapshot(_run([*base, dup, late_other])))
    assert lot["failures"][0]["cause"] == "breakdown"
    lot = _lot(line_snapshot(_run([*base, early_other])))
    assert lot["failures"][0]["cause"] == "quality"


def test_failures_capped(monkeypatch):
    monkeypatch.setattr(state_mod, "MAX_LOT_FAILURES", 1)
    lot = _lot(line_snapshot(_run(_failed_msgs())))
    assert len(lot["failures"]) == 1 and lot["failures"][0]["visit"] == 2


def test_op_failed_event_payload_has_cause():
    p = event_payload(op(OpFailed, "o1", "spin", 4, reason="boom", cause="breakdown"))
    assert p["cause"] == "breakdown" and p["reason"] == "boom"
    p = event_payload(op(OpFailed, "o1", "spin", 4, reason="boom"))
    assert p["cause"] == "quality"


def test_scrapped_lot_keeps_failures():
    failed = OrderLifecycle.make(
        run_id=RUN,
        machine_id="scheduler",
        ts=at(9),
        state="failed",
        order_id="o1",
        product="margherita",
        qty=1,
        due_ts=at(3600.0),
        created_ts=at(0.0),
        step="spin",
        visit=2,
    )
    lot = _lot(line_snapshot(_run([*_failed_msgs(), failed])))
    assert lot["state"] == "failed" and len(lot["failures"]) == 2


def test_event_payload_new_kinds():
    p = event_payload(op(OpFailed, "o1", "spin", 4, reason="boom"))
    assert p["kind"] == "op_failed" and p["reason"] == "boom" and p["redelivered"] is None
    p = event_payload(op(OpStarted, "o1", "spin", 4, redelivered=True))
    assert p["redelivered"] is True
    assert event_payload(_lotfact(LotStocked, 1, kind="overflow"))["stock_kind"] == "overflow"
    assert event_payload(_lotfact(LotUnstocked, 1))["kind"] == "lot_unstocked"
    p = event_payload(_lotfact(LotBanked, 1, reason="customer", source="auto_wait"))
    assert p["kind"] == "lot_banked" and p["reason"] == "customer" and p["source"] == "auto_wait"
    assert event_payload(_lotfact(LotReleased, 1, by="auto"))["by"] == "auto"
    hb = StationHeartbeat.make(run_id=RUN, machine_id="spin", ts=at(1), status="idle")
    assert event_payload(hb) is None
    down = StationStatus.make(
        run_id=RUN, machine_id="bake", ts=at(5), status="down", cause="random", repair_in_s=12
    )
    p = event_payload(down)
    assert p["cause"] == "random" and p["repair_in_s"] == 12


def test_lot_facts_for_unknown_order_ignored_and_marks_capped():
    st = _run([_lotfact(LotBanked, 1, oid="ghost", reason="capacity", source="manual")])
    assert not st.lots
    msgs = [*_released(), *(_lotfact(LotStocked, 2 + i, kind="overflow") for i in range(40))]
    assert len(_run(msgs).lots["o1"].stock_marks) <= 16


# ---- Phase 7.1 plan 08: frozen ETA fold / derive ---------------------------


def _eta_fact(state, order_id="o1", *, offset=0.0, eta_off=100.0, station_eta=None, with_eta=True):
    return OrderLifecycle.make(
        run_id=RUN,
        machine_id="scheduler",
        ts=at(offset),
        state=state,
        order_id=order_id,
        product="margherita",
        qty=1,
        due_ts=at(3600.0),
        created_ts=at(0.0),
        eta_ts=at(eta_off) if with_eta else None,
        station_eta=station_eta,
    )


def test_eta_fields_fold_and_derive_in_progress():
    st = _run([_eta_fact("released", station_eta={"spin": at(10.0), "bake": at(50.0)})])
    lot = _lot(line_snapshot(st))
    assert lot["eta_ts"] == state_mod._iso(at(100.0))
    assert lot["station_eta"] == {
        "spin": state_mod._iso(at(10.0)),
        "bake": state_mod._iso(at(50.0)),
    }
    assert lot["eta_now_ts"] is None
    assert lot["eta_error_s"] is None


def test_eta_error_after_completed_and_missing_eta():
    st = _run([_eta_fact("released"), _eta_fact("completed", offset=130.0)])
    lot = _lot(line_snapshot(st))
    assert lot["state"] == "done"
    assert lot["eta_error_s"] == 30.0
    assert lot["completed_ts"] == state_mod._iso(at(130.0))
    st2 = _run(
        [
            _eta_fact("released", with_eta=False),
            _eta_fact("completed", offset=130.0, with_eta=False),
        ]
    )
    lot2 = _lot(line_snapshot(st2))
    assert lot2["eta_ts"] is None and lot2["station_eta"] is None
    assert lot2["eta_error_s"] is None


def test_eta_fold_shuffle_and_duplicates_identical():
    base = [
        _eta_fact("released", station_eta={"spin": at(10.0)}),
        _eta_fact("completed", offset=130.0, station_eta={"spin": at(10.0)}),
    ]
    ref = line_snapshot(_run(base))
    for seed in range(5):
        assert line_snapshot(_run(shuffled(base, seed))) == ref
        assert line_snapshot(_run(with_duplicates(base, seed, 4))) == ref
    again = [*base, _eta_fact("released", station_eta={"spin": at(10.0)})]
    assert line_snapshot(_run(again)) == ref


def test_eta_later_none_does_not_erase_and_min_kept():
    st = _run(
        [
            _eta_fact("released", eta_off=100.0, station_eta={"spin": at(10.0)}),
            _eta_fact("completed", offset=130.0, with_eta=False),
            _eta_fact("completed", offset=131.0, eta_off=90.0, station_eta={"spin": at(5.0)}),
        ]
    )
    lot = _lot(line_snapshot(st))
    assert lot["eta_ts"] == state_mod._iso(at(90.0))
    assert lot["station_eta"] == {"spin": state_mod._iso(at(5.0))}


def test_recent_done_ordering_and_exclusions():
    msgs = []
    for i in range(1, 4):
        oid = f"d{i}"
        msgs += [_eta_fact("released", oid), _eta_fact("completed", oid, offset=100.0 + i)]
    msgs += [_eta_fact("released", "e1"), _eta_fact("completed", "e1", offset=103.0)]
    msgs += [_eta_fact("released", "active1")]
    snap = line_snapshot(_run(msgs))
    ids = [lt["order_id"] for lt in state_mod.recent_done(snap, n=20)]
    assert "active1" not in ids
    assert ids == ["d3", "e1", "d2", "d1"]
    assert len(state_mod.recent_done(snap, n=2)) == 2


def test_recent_done_tolerates_summary_lots_without_completed_ts():
    snap = {"lots": [
        {"order_id": "a", "state": "done"},
        {"order_id": "b", "state": "done", "completed_ts": "2026-01-01T00:00:00Z"},
    ]}
    assert [lt["order_id"] for lt in state_mod.recent_done(snap)] == ["b", "a"]


def test_pre_phase_lots_derive_without_eta():
    st = _run([lifecycle("released"), lifecycle("completed", offset=5.0)])
    lot = _lot(line_snapshot(st))
    assert lot["eta_ts"] is None and lot["eta_error_s"] is None


# -- quick 261023-pfp: deferred hold ---------------------------------------------


def _holdfact(offset, state="requested", kind="bank", station="spin", oid="o1", reason="capacity"):
    return LotHold.make(
        run_id=RUN,
        machine_id="scheduler",
        ts=at(offset),
        order_id=oid,
        station=station,
        state=state,
        kind=kind,
        reason=reason,
    )


def _all_ops_done(oid="o1"):
    return [op(OpCompleted, oid, st, 3 + i) for i, st in enumerate(STATIONS)]


def test_hold_pending_then_cleared_and_tie_clears():
    base = [*_released(), op(OpStarted, "o1", "spin", 2)]
    lot = _lot(line_snapshot(_run([*base, _holdfact(3)])))
    assert lot["state"] == "running"
    assert lot["hold"] == {
        "kind": "bank",
        "reason": "capacity",
        "station": "spin",
        "since_ts": lot["hold"]["since_ts"],
    }
    for state in ("cancelled", "applied"):
        later = _run([*base, _holdfact(3), _holdfact(4, state)])
        assert _lot(line_snapshot(later))["hold"] is None
        tie = _run([*base, _holdfact(3), _holdfact(3, state)])
        assert _lot(line_snapshot(tie))["hold"] is None


def test_hold_none_when_banked_or_failed():
    base = [*_released(), _holdfact(2)]
    banked = _run(
        [*base, _lotfact(LotBanked, 3, station="sauce", reason="capacity", source="manual")]
    )
    lot = _lot(line_snapshot(banked))
    assert lot["state"] == "banked" and lot["hold"] is None
    failed = _run([*base, lifecycle("failed", "o1", offset=3)])
    assert _lot(line_snapshot(failed))["hold"] is None


def test_lot_banked_after_package_is_banked_not_done_and_not_pruned(monkeypatch):
    msgs = [
        *_released(),
        _holdfact(2.5, station=STATIONS[-1]),
        *_all_ops_done(),
        _holdfact(19, "applied", station=STATIONS[-1]),
        _lotfact(LotBanked, 20, station=STATIONS[-1], reason="capacity", source="manual"),
    ]
    snap = line_snapshot(_run(msgs))
    lot = _lot(snap)
    assert lot["state"] == "banked" and lot["station"] == STATIONS[-1]
    assert snap["counts"]["completed"] == 0 and snap["counts"]["bank"] == 1
    # pruning ignores it
    monkeypatch.setattr(state_mod, "MAX_COMPLETED_LOTS", 0)
    st = _run(msgs)
    assert "o1" in st.lots and st.pruned_completed == 0
    monkeypatch.undo()
    # release + completed lifecycle -> done
    done = [
        *msgs,
        _lotfact(LotReleased, 21, station=STATIONS[-1], by="manual"),
        lifecycle("completed", "o1", offset=22),
    ]
    assert _lot(line_snapshot(_run(done)))["state"] == "done"


def test_lot_scrapped_after_package_is_failed():
    msgs = [*_released(), *_all_ops_done(), lifecycle("failed", "o1", offset=20)]
    snap = line_snapshot(_run(msgs))
    assert _lot(snap)["state"] == "failed"
    assert snap["counts"]["completed"] == 0


def test_event_payload_lot_hold():
    p = event_payload(_holdfact(1, "applied", "scrap", "bake", reason="manual_scrap:quality"))
    assert p["kind"] == "lot_hold" and p["state"] == "applied" and p["station"] == "bake"
    assert p["order_id"] == "o1" and p["hold_kind"] == "scrap"
    assert p["reason"] == "manual_scrap:quality"


def test_lot_visits_per_visit_records():
    msgs = [
        *_released(),
        op(OpDispatched, "o1", "spin", 2),
        op(OpStarted, "o1", "spin", 3),
        op(OpFailed, "o1", "spin", 4, reason="boom"),
        op(OpDispatched, "o1", "spin", 5, visit=2),
        op(OpStarted, "o1", "spin", 6, visit=2),
        op(OpCompleted, "o1", "spin", 7, visit=2),
    ]
    lot = _lot(line_snapshot(_run(msgs)))
    assert [(v["station"], v["visit"]) for v in lot["visits"]] == [("spin", 1), ("spin", 2)]
    v1, v2 = lot["visits"]
    assert v1["failed_ts"] == "2026-01-01T00:00:04.000Z" and v1["completed_ts"] is None
    assert v1["started_ts"] == "2026-01-01T00:00:03.000Z"
    assert v2["failed_ts"] is None
    assert v2["started_ts"] == "2026-01-01T00:00:06.000Z"
    assert v2["completed_ts"] == "2026-01-01T00:00:07.000Z"
    assert set(v1) == {"station", "visit", "started_ts", "completed_ts", "failed_ts"}


def test_lot_visits_route_order_and_empty():
    assert _lot(line_snapshot(_run(_released())))["visits"] == []
    msgs = [
        *_released(),
        op(OpStarted, "o1", "sauce", 4),
        op(OpStarted, "o1", "spin", 2),
    ]
    lot = _lot(line_snapshot(_run(msgs)))
    assert [v["station"] for v in lot["visits"]] == ["spin", "sauce"]


def test_lot_priority_changes_recorded_sorted_and_deduped():
    ch = _change(8, 3, old=7)
    st = _run([lifecycle("created", priority=50), ch, _change(5, 7)])
    st = fold(st, ch)
    lot = _lot(line_snapshot(st))
    assert lot["priority_changes"] == [
        {"ts": "2026-01-01T00:00:05.000Z", "from": 50, "to": 7},
        {"ts": "2026-01-01T00:00:08.000Z", "from": 7, "to": 3},
    ]
    assert _lot(line_snapshot(_run(_released())))["priority_changes"] == []


# ---- summary vs detail split (quick 261025-gmm) -------------------------------


def _running_state():
    return _run(
        [
            lifecycle("created"),
            lifecycle("released", offset=1),
            op(OpDispatched, "o1", "spin", 2),
            op(OpStarted, "o1", "spin", 3),
        ]
    )


def test_summary_snapshot_drops_heavy_and_empty_keys():
    st = _running_state()
    full = line_snapshot(st)
    slim = line_snapshot(st, summary=True)
    lot = _lot(slim)
    for key in state_mod.LOT_DETAIL_KEYS:
        assert key not in lot
    assert all(v is not None and v is not False for v in lot.values())
    assert lot["step_started_ts"] == _lot(full)["steps"]["spin"]["started_ts"]
    assert lot["steps_done"] == 0
    assert slim["counts"] == full["counts"]
    assert slim["stations"] == full["stations"]
    assert "visits" in _lot(full)


def test_lot_detail_matches_full_derive_and_unknown_is_none():
    st = _running_state()
    full = _lot(line_snapshot(st))
    detail = state_mod.lot_detail(st, "o1")
    assert detail == {"order_id": "o1", **{k: full[k] for k in state_mod.LOT_DETAIL_KEYS}}
    assert state_mod.lot_detail(st, "nope") is None
