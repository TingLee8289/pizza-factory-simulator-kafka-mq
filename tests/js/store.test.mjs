import test from "node:test";
import assert from "node:assert/strict";
import { createStore, HISTORY_CAP, isComparisonLocked } from "../../src/pizza/dashboard/static/js/store.js";
import { feedLine } from "../../src/pizza/dashboard/static/js/feed.js";

const snap = (over = {}) => ({
  seq: 10,
  server_ts: 1,
  replaying: false,
  run: { state: "running", run_id: "r1", arrival_rate_per_min: 1 },
  line: {
    run_id: "r1",
    paused: false,
    stations: { bake: { status: "busy", queue: 2 } },
    lots: [
      { order_id: "a", state: "running" },
      { order_id: "d", state: "done" },
    ],
    counts: { wip: 1, completed: 1 },
  },
  kpi: { wip_now: 1 },
  history: Array.from({ length: 700 }, (_, i) => ({ t: i })),
  panel: { x: 1 },
  panel_history: { k: [1, null] },
  ...over,
});

test("replace sets state and trims history", () => {
  const s = createStore();
  s.replace(snap());
  const st = s.getState();
  assert.equal(st.seq, 10);
  assert.equal(st.connection, "live");
  assert.equal(st.history.length, HISTORY_CAP);
  assert.equal(st.history.at(-1).t, 699);
  assert.equal(st.line.stations.bake.queue, 2);
  assert.equal(st.dirty, true);
  assert.deepEqual(st.panelHistory, { k: [1, null] });
});

test("merge updates active set, keeps done lots, appends point", () => {
  const s = createStore();
  s.replace(snap({ history: [] }));
  s.merge({
    seq: 12,
    run: { state: "running", run_id: "r1" },
    paused: true,
    stations: { bake: { status: "idle", queue: 0 } },
    lots: [{ order_id: "b", state: "queued" }],
    kpi: { wip_now: 5 },
    kpi_point: { t: 1 },
    panel: { y: 2 },
  });
  const st = s.getState();
  assert.equal(st.seq, 12);
  assert.deepEqual(st.line.lots.map((l) => l.order_id).sort(), ["b", "d"]);
  assert.equal(st.line.paused, true);
  assert.equal(st.kpi.wip_now, 5);
  assert.equal(st.history.length, 1);
  assert.equal(st.panel.y, 2);
  s.merge({ seq: 5, lots: [], kpi_point: null });
  assert.equal(s.getState().seq, 12);
});

test("merge caps history at 600", () => {
  const s = createStore();
  s.replace(snap({ history: [] }));
  for (let i = 0; i < 700; i++) s.merge({ seq: 11 + i, lots: [], kpi_point: { t: i } });
  const h = s.getState().history;
  assert.equal(h.length, 600);
  assert.equal(h[0].t, 100);
  assert.equal(h.at(-1).t, 699);
});

test("applyFact filters by snapshot seq and duplicates", () => {
  const s = createStore();
  const seen = [];
  s.onFact((f) => seen.push(f.seq));
  s.replace(snap());
  assert.equal(s.applyFact({ seq: 10, kind: "x" }), false);
  assert.equal(s.applyFact({ seq: 9, kind: "x" }), false);
  assert.equal(s.applyFact({ seq: 11, kind: "op_started", station: "bake", order_id: "a" }), true);
  assert.equal(s.applyFact({ seq: 11, kind: "op_started" }), false);
  assert.deepEqual(seen, [11]);
  assert.equal(s.getState().feed.length, 1);
});

test("feed capped at 5", () => {
  const s = createStore();
  s.replace(snap());
  for (let i = 0; i < 9; i++) s.applyFact({ seq: 11 + i, kind: "k" });
  const f = s.getState().feed;
  assert.equal(f.length, 5);
  assert.equal(f.at(-1).seq, 19);
});

test("selection notifies and clears on new run id", () => {
  const s = createStore();
  s.replace(snap());
  let calls = 0;
  const un = s.subscribe(() => calls++);
  s.select({ kind: "station", id: "bake" });
  assert.equal(s.getState().selection.id, "bake");
  assert.equal(calls, 1);
  s.select(null);
  assert.equal(s.getState().selection, null);
  assert.equal(calls, 2);
  s.select({ kind: "lot", id: "a" });
  s.replace(snap({ run: { state: "running", run_id: "r1" } }));
  assert.notEqual(s.getState().selection, null);
  s.replace(snap({ run: { state: "running", run_id: "r2" }, line: { run_id: "r2", stations: {}, lots: [], counts: {} } }));
  assert.equal(s.getState().selection, null);
  un();
  s.select(null);
  assert.ok(calls >= 2);
});

test("connection and staleness", () => {
  const s = createStore();
  s.replace(snap());
  s.setConnection("reconnecting");
  assert.equal(s.getState().connection, "reconnecting");
  assert.equal(s.getState().kpi.wip_now, 1);
  s.noteData();
  const t = s.getState().lastDataAt;
  assert.equal(s.isStale(t + 5000), false);
  assert.equal(s.isStale(t + 5001), true);
});

test("merge carries counts, setFailureConfig stores config, feed keeps new keys", () => {
  const s = createStore();
  s.replace(snap());
  s.merge({ seq: 11, counts: { wip: 2, completed: 0, bank: 1, failed: 0, stocker_overflow: 0, stocker_down: 0 } });
  assert.equal(s.getState().line.counts.bank, 1);
  s.setFailureConfig({ mtbf_s: 5 });
  assert.equal(s.getState().failureConfig.mtbf_s, 5);
  s.applyFact({ seq: 50, kind: "lot_banked", reason: "capacity", source: "manual", visit: 2 });
  const e = s.getState().feed.at(-1);
  assert.equal(e.reason, "capacity");
  assert.equal(e.source, "manual");
  assert.equal(e.visit, 2);
  assert.equal(createStore().getState().line.counts.stocker_down, 0);
});

test("replace sets rule/comparison/runs from the snapshot, null-safe", () => {
  const s = createStore();
  s.replace(snap({ line: { ...snap().line, rule: "edd" }, comparison: { state: "idle", results: [] }, runs: [{ run_id: "r1" }] }));
  const st = s.getState();
  assert.equal(st.rule, "edd");
  assert.deepEqual(st.comparison, { state: "idle", rows: [] });
  assert.deepEqual(st.runs, [{ run_id: "r1" }]);

  const s2 = createStore();
  s2.replace(snap());
  const st2 = s2.getState();
  assert.equal(st2.rule, null);
  assert.equal(st2.comparison, null);
  assert.deepEqual(st2.runs, []);
});

test("fact rule_changed sets state.rule, clears to it even without snapshot.line.rule, and pushes a feed entry rendered as RULE SPT", () => {
  const s = createStore();
  s.onFact(() => {});
  s.replace(snap());
  assert.equal(s.applyFact({ seq: 11, kind: "rule_changed", rule: "spt" }), true);
  const st = s.getState();
  assert.equal(st.rule, "spt");
  const entry = st.feed.at(-1);
  assert.equal(entry.kind, "rule_changed");
  assert.equal(entry.rule, "spt");
  assert.match(feedLine(entry), /RULE SPT$/);
});

test("fact listeners fire for rule_changed (controls.js toast/pending hook)", () => {
  const s = createStore();
  const seen = [];
  s.onFact((f) => seen.push(f));
  s.replace(snap());
  s.applyFact({ seq: 11, kind: "rule_changed", rule: "edd" });
  assert.equal(seen.length, 1);
  assert.equal(seen[0].rule, "edd");
});

test("tick merge updates rule only when different", () => {
  const s = createStore();
  s.replace(snap({ line: { ...snap().line, rule: "fifo" } }));
  s.merge({ seq: 11, lots: [], rule: "fifo" });
  assert.equal(s.getState().rule, "fifo");
  s.merge({ seq: 12, lots: [], rule: "edd" });
  assert.equal(s.getState().rule, "edd");
});

test("isComparisonLocked true only for round_running/round_done", () => {
  assert.equal(isComparisonLocked({ comparison: { state: "round_running" } }), true);
  assert.equal(isComparisonLocked({ comparison: { state: "round_done" } }), true);
  assert.equal(isComparisonLocked({ comparison: { state: "idle" } }), false);
  assert.equal(isComparisonLocked({ comparison: { state: "finished" } }), false);
  assert.equal(isComparisonLocked({ comparison: { state: "cancelled" } }), false);
  assert.equal(isComparisonLocked({ comparison: { state: "error" } }), false);
  assert.equal(isComparisonLocked({ comparison: null }), false);
  assert.equal(isComparisonLocked({}), false);
});

test("comparison event replaces state.comparison (results -> rows); comparison_result appends/updates a row without duplicates", () => {
  const s = createStore();
  s.replace(snap());
  // The backend's status dict names the row list "results" (dashboard/comparison.py);
  // the store normalises it to "rows" to match the plan 11 store contract.
  s.applyComparison({ state: "round_running", comparison_id: "c-1", round: 1, results: [] });
  assert.equal(s.getState().comparison.state, "round_running");
  assert.deepEqual(s.getState().comparison.rows, []);
  s.applyComparisonResult({ rule: "fifo", run_id: "run-1", mean_cycle_time: 10 });
  assert.equal(s.getState().comparison.rows.length, 1);
  s.applyComparisonResult({ rule: "fifo", run_id: "run-1", mean_cycle_time: 11 });
  assert.equal(s.getState().comparison.rows.length, 1);
  assert.equal(s.getState().comparison.rows[0].mean_cycle_time, 11);
  s.applyComparisonResult({ rule: "edd", run_id: "run-2", mean_cycle_time: 9 });
  assert.equal(s.getState().comparison.rows.length, 2);
  s.applyComparison({ state: "finished", comparison_id: "c-1", round: 3, results: [{ rule: "fifo" }, { rule: "edd" }, { rule: "spt" }] });
  assert.equal(s.getState().comparison.state, "finished");
  assert.equal(s.getState().comparison.rows.length, 3);
  assert.equal("results" in s.getState().comparison, false);
});

test("merge done_recent: keeps done lots, overlays recent, dedupes by order_id", () => {
  const s = createStore();
  s.replace(snap({ history: [] }));
  s.merge({ seq: 11, lots: [{ order_id: "b", state: "running" }], done_recent: [{ order_id: "x", state: "done", eta_error_s: 3 }] });
  let ids = s.getState().line.lots.map((l) => l.order_id).sort();
  assert.deepEqual(ids, ["b", "d", "x"]);
  // b completes: appears in done_recent, also (stale) in lots -> present once as done
  s.merge({ seq: 12, lots: [{ order_id: "b", state: "running" }], done_recent: [{ order_id: "b", state: "done", eta_error_s: 72 }, { order_id: "x", state: "done", eta_error_s: 4 }] });
  const lots = s.getState().line.lots;
  assert.deepEqual(lots.map((l) => l.order_id).sort(), ["b", "d", "x"]);
  assert.equal(lots.find((l) => l.order_id === "b").state, "done");
  assert.equal(lots.find((l) => l.order_id === "b").eta_error_s, 72);
  assert.equal(lots.find((l) => l.order_id === "x").eta_error_s, 4);
  // repeated ticks do not grow the list
  s.merge({ seq: 13, lots: [], done_recent: [{ order_id: "b", state: "done", eta_error_s: 72 }] });
  assert.equal(s.getState().line.lots.length, 3);
});

test("merge ignores non-array / malformed done_recent and keeps selection", () => {
  const s = createStore();
  s.replace(snap({ history: [] }));
  s.select({ kind: "lot", id: "a" });
  s.merge({ seq: 11, lots: [{ order_id: "b", state: "running" }], done_recent: "nope" });
  assert.deepEqual(s.getState().line.lots.map((l) => l.order_id).sort(), ["b", "d"]);
  s.merge({ seq: 12, lots: [], done_recent: [null, 5, { order_id: 1, state: "done" }, { order_id: "q", state: "running" }, { order_id: "a", state: "done" }] });
  assert.deepEqual(s.getState().line.lots.map((l) => l.order_id).sort(), ["a", "d"]);
  assert.deepEqual(s.getState().selection, { kind: "lot", id: "a" });
});
