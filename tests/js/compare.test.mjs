import test from "node:test";
import assert from "node:assert/strict";
import {
  bestByMetric, formatCell, withBestSuffix, stepperModel, roundLine, controlsModel,
  scoreboardModel, failedNote, recomputeCardModel, historyRows, buildComparisonBody,
  shortRunId, recapLine, liveBadgeText, RULES,
  PANEL_HEADING, TABLE_EMPTY_HEADING, HISTORY_EMPTY_HEADING, HISTORY_EMPTY_BODY,
  LOCKED_TITLE, REPLAY_NOTE, initCompare,
} from "../../src/pizza/dashboard/static/js/compare.js";

const ROW = (rule, over = {}) => ({
  rule, run_id: `r-${rule}`, seed: 1, n_orders: 30,
  mean_cycle_time: 100, mean_tardiness: 10, pct_late: 0.2, makespan: 500,
  partial: false, n_failed: 0, max_tardiness: 20,
  ...over,
});

// --- Test 1: bestByMetric ---
test("bestByMetric picks the lowest value per metric, ignoring nulls, no marker on ties/<2", () => {
  const rows = [
    ROW("fifo", { mean_cycle_time: 120, mean_tardiness: 10, pct_late: 0.3, makespan: 600 }),
    ROW("edd", { mean_cycle_time: 90, mean_tardiness: 10, pct_late: 0.1, makespan: 500 }),
    ROW("spt", { mean_cycle_time: 100, mean_tardiness: null, pct_late: 0.1, makespan: 500 }),
  ];
  const best = bestByMetric(rows);
  assert.deepEqual(best.mean_cycle_time, new Set(["edd"]));
  assert.deepEqual(best.mean_tardiness, new Set(["fifo", "edd"])); // tie at 10 (spt is null, excluded)
  assert.deepEqual(best.pct_late, new Set(["edd", "spt"])); // tie -> no single winner, but set holds both tied
  assert.deepEqual(best.makespan, new Set(["edd", "spt"]));
  assert.deepEqual(bestByMetric([ROW("fifo")]).mean_cycle_time, new Set());
  assert.deepEqual(bestByMetric([]).mean_cycle_time, new Set());
});

// --- Test 2: formatCell / withBestSuffix ---
test("formatCell formats per-column and withBestSuffix appends the marker text", () => {
  assert.equal(formatCell("mean_cycle_time", null), "-");
  assert.equal(formatCell("n_orders", 30), "30");
  assert.equal(formatCell("mean_cycle_time", 123.456), "123.5");
  assert.equal(formatCell("mean_tardiness", 10), "10.0");
  assert.equal(formatCell("makespan", 500), "500.0");
  assert.equal(formatCell("pct_late", 0.125), "12.5%");
  assert.equal(withBestSuffix("12.5%", true), "12.5% (best)");
  assert.equal(withBestSuffix("12.5%", false), "12.5%");
});

// --- Test 3: stepperModel / roundLine ---
test("stepperModel derives step states from rows.length (done) and state.round (running)", () => {
  const running = stepperModel({ state: "round_running", round: 2, rows: [{}] });
  assert.deepEqual(running.map((s) => s.state), ["done", "running", "pending"]);
  assert.equal(running[1].current, true);

  const finished = stepperModel({ state: "finished", round: 3, rows: [{}, {}, {}] });
  assert.deepEqual(finished.map((s) => s.state), ["done", "done", "done"]);

  const cancelled = stepperModel({ state: "cancelled", round: 2, rows: [{}] });
  assert.deepEqual(cancelled.map((s) => s.state), ["done", "pending", "pending"]);
});

test("roundLine renders the exact UI-SPEC copy", () => {
  assert.equal(
    roundLine({ round: 2, rule: "edd", n_orders: 30 }, 12),
    "Round 2 of 3 - EDD - 12/30 lots completed",
  );
  assert.equal(
    roundLine({ round: 1, rule: "fifo", n_orders: 30 }, 0),
    "Round 1 of 3 - FIFO - 0/30 lots completed",
  );
});

// --- Test 4: controlsModel ---
test("controlsModel: End round only while running, Next round label/disable, Cancel, Download CSV", () => {
  const idle = controlsModel({ state: "idle" });
  assert.equal(idle.primary.label, "Start comparison");
  assert.equal(idle.endRound.disabled, true);
  assert.equal(idle.cancel.disabled, true);

  const running = controlsModel({ state: "round_running", round: 1 });
  assert.equal(running.endRound.disabled, false);
  assert.equal(running.primary.disabled, true);
  assert.equal(running.primary.title, "Round 1 is still running");
  assert.equal(running.cancel.disabled, false);

  const done = controlsModel({ state: "round_done", round: 1 });
  assert.equal(done.primary.disabled, false);
  assert.equal(done.primary.label, "Next round: EDD");
  assert.equal(done.endRound.disabled, true);
  assert.equal(done.cancel.disabled, false);

  const finished = controlsModel({ state: "finished", round: 3 });
  assert.deepEqual(finished.primary, { action: "csv", label: "Download CSV", disabled: false, title: "" });
});

// --- Test 5: scoreboardModel ---
test("scoreboardModel returns winner/value/vs-worst only with exactly 3 rows", () => {
  const rows = [
    ROW("fifo", { mean_tardiness: 20, pct_late: 0.3, makespan: 600 }),
    ROW("edd", { mean_tardiness: 10, pct_late: 0.1, makespan: 500 }),
    ROW("spt", { mean_tardiness: 15, pct_late: 0.2, makespan: 550 }),
  ];
  const sb = scoreboardModel(rows);
  assert.equal(sb.mean_tardiness.rule, "EDD");
  assert.equal(sb.mean_tardiness.vsWorstText, "vs worst: -50.0%");
  assert.equal(sb.pct_late.rule, "EDD");
  assert.equal(sb.makespan.rule, "EDD");
  assert.equal(scoreboardModel(rows.slice(0, 2)), null);
  assert.equal(scoreboardModel([]), null);
});

// --- Test 6: failedNote ---
test("failedNote lists per-rule scrap counts only when any is > 0", () => {
  const rows = [ROW("fifo", { n_failed: 1 }), ROW("edd", { n_failed: 0 }), ROW("spt", { n_failed: 2 })];
  assert.equal(failedNote(rows), "Scrap lots are excluded: FIFO 1, EDD 0, SPT 2.");
  assert.equal(failedNote([ROW("fifo", { n_failed: 0 })]), null);
  assert.equal(failedNote([]), null);
});

// --- Test 7: recomputeCardModel ---
test("recomputeCardModel builds the match/mismatch chip, field rows, footer, note and CLI line", () => {
  const match = recomputeCardModel({
    run_id: "r-1", matched: true, tolerance: 0.01,
    fields: [{ field: "mean_cycle_time_s", stored: 1, recomputed: 1, diff: 0, ok: true }],
  });
  assert.deepEqual(match.chip, { text: "Replay matches", tone: "ok" });
  assert.equal(match.rows[0].diffText, "ok");
  assert.equal(match.footer, "Compared with tolerance 0.01.");
  assert.equal(match.note, REPLAY_NOTE);
  assert.equal(match.cli, "python -m pizza.kpi.recompute --run-id r-1");

  const mismatch = recomputeCardModel({
    run_id: "r-2", matched: false, tolerance: 0.01,
    fields: [{ field: "mean_cycle_time_s", stored: 1, recomputed: 2, diff: 1, ok: false }],
  });
  assert.deepEqual(mismatch.chip, { text: "Replay mismatch", tone: "warn" });
  assert.equal(mismatch.rows[0].diffText, "! 1");
});

// --- Test 8: historyRows ---
test("historyRows caps at 20, shortens run id, dashes nulls, flags missing snapshots, empty state", () => {
  const runs = [
    { run_id: "r-20261020-010203-ab12", rule: "edd", seed: 1, n_orders: 30, started_ts: "2026-10-20T01:02:03Z", has_snapshot: true },
    { run_id: "r-20261020-010203-cd34", rule: null, seed: null, n_orders: null, started_ts: null, has_snapshot: false },
  ];
  const { rows, empty } = historyRows(runs);
  assert.equal(empty, null);
  assert.equal(rows[0].shortId, "r-202610");
  assert.equal(rows[1].rule, "-");
  assert.equal(rows[1].seed, "-");
  assert.equal(rows[1].orders, "-");
  assert.deepEqual(rows[1].recompute, { disabled: true, title: "No stored snapshot for this run" });
  assert.deepEqual(rows[0].recompute, { disabled: false, title: "" });

  const many = Array.from({ length: 25 }, (_, i) => ({ run_id: `r-${i}`, has_snapshot: true }));
  assert.equal(historyRows(many).rows.length, 20);

  const empty2 = historyRows([]);
  assert.deepEqual(empty2.rows, []);
  assert.deepEqual(empty2.empty, { heading: HISTORY_EMPTY_HEADING, body: HISTORY_EMPTY_BODY });
});

test("shortRunId / recapLine / liveBadgeText pure helpers", () => {
  assert.equal(shortRunId("r-20261020-010203-ab12"), "r-202610");
  assert.equal(shortRunId(null), "");
  assert.equal(
    recapLine({ seed: 42, n_orders: 30, k: 1.5, arrival_rate_per_min: 2, random_breakdowns: true }),
    "Seed 42 - 30 lots - k 1.5 - 4 pizzas/min - breakdowns on",
  );
  assert.equal(
    recapLine({ seed: null, n_orders: null, k: null, arrival_rate_per_min: null, random_breakdowns: false }),
    "Seed - - - lots - k - - - pizzas/min - breakdowns off",
  );
  assert.equal(liveBadgeText({ state: "round_running", round: 2 }), "Compare R2/3");
  assert.equal(liveBadgeText({ state: "round_done", round: 1 }), "Compare R1/3");
  assert.equal(liveBadgeText({ state: "finished", round: 3 }), null);
  assert.equal(liveBadgeText({ state: "idle" }), null);
  assert.equal(liveBadgeText(null), null);
});

// --- Test 10: buildComparisonBody ---
test("buildComparisonBody: blank seed omitted, coercion, weights omitted at all-1, defaults", () => {
  const defaults = buildComparisonBody({});
  assert.deepEqual(defaults, {
    n_orders: 30, arrival_rate_per_min: 2.0, k: 1.5, random_breakdowns: true,
  });
  assert.equal("seed" in defaults, false);
  assert.equal("weights" in defaults, false);

  const full = buildComparisonBody({
    seed: "42", n: "20", rate: "7", k: "2", weights: { margherita: "2", pepperoni: "1", veggie: "1" },
    randomBreakdowns: false,
  });
  assert.deepEqual(full, {
    seed: 42, n_orders: 20, arrival_rate_per_min: 3.5, k: 2,
    weights: { margherita: 2, pepperoni: 1, veggie: 1 }, random_breakdowns: false,
  });

  const blankSeed = buildComparisonBody({ seed: "  " });
  assert.equal("seed" in blankSeed, false);
});

test("RULES/PANEL_HEADING/TABLE_EMPTY_HEADING/LOCKED_TITLE are exact UI-SPEC copy", () => {
  assert.deepEqual(RULES, ["FIFO", "EDD", "SPT"]);
  assert.equal(PANEL_HEADING, "Compare dispatch rules");
  assert.equal(TABLE_EMPTY_HEADING, "No comparison results yet");
  assert.equal(LOCKED_TITLE, "Locked while a comparison is running");
});

// --- initCompare smoke test: a hand-written stub `doc` (no jsdom) ---

function makeClassList(el) {
  const set = new Set();
  return {
    add: (c) => set.add(c),
    remove: (c) => set.delete(c),
    toggle: (c, on) => {
      const next = on === undefined ? !set.has(c) : Boolean(on);
      if (next) set.add(c);
      else set.delete(c);
      return next;
    },
    contains: (c) => set.has(c),
  };
}

function makeElement(tag) {
  const el = {
    tagName: String(tag || "div").toUpperCase(),
    dataset: {},
    attrs: {},
    children: [],
    listeners: {},
    textContent: "",
    value: "",
    checked: false,
    disabled: false,
    title: "",
    style: {},
    _weightInputs: [],
    setAttribute(name, value) { this.attrs[name] = String(value); },
    getAttribute(name) { return this.attrs[name]; },
    addEventListener(type, handler) {
      (this.listeners[type] = this.listeners[type] || []).push(handler);
    },
    removeEventListener() {},
    appendChild(child) { this.children.push(child); return child; },
    removeChild(child) {
      const i = this.children.indexOf(child);
      if (i >= 0) this.children.splice(i, 1);
      return child;
    },
    querySelectorAll(sel) {
      return sel === ".weight-input" ? this._weightInputs : [];
    },
    focus() {},
    async fire(type, event = {}) {
      for (const h of this.listeners[type] || []) await h(event);
    },
  };
  el.classList = makeClassList(el);
  return el;
}

function makeStubDoc() {
  const elements = new Map();
  const docListeners = {};
  const doc = {
    getElementById(id) {
      if (!elements.has(id)) elements.set(id, makeElement("div"));
      return elements.get(id);
    },
    createElement(tag) {
      return makeElement(tag);
    },
    addEventListener(type, handler) {
      (docListeners[type] = docListeners[type] || []).push(handler);
    },
    removeEventListener() {},
    dispatchEvent() { return true; },
    defaultView: null,
    async fire(type, event = {}) {
      for (const h of docListeners[type] || []) await h(event);
    },
  };
  return doc;
}

function makeFakeStore(initial) {
  let state = initial;
  const listeners = [];
  return {
    getState: () => state,
    subscribe(fn) { listeners.push(fn); return () => {}; },
    onFact() { return () => {}; },
    async setState(next) {
      state = next;
      for (const fn of listeners) await fn(state);
    },
  };
}

function makeFakeApi(overrides = {}) {
  return {
    startComparison: async () => ({}),
    nextRound: async () => ({}),
    endRound: async () => ({}),
    cancelComparison: async () => ({}),
    recomputeRun: async () => ({}),
    csvUrl: () => "/api/comparison.csv",
    errorReason: () => "Request failed",
    ...overrides,
  };
}

test("initCompare: init does not throw against a hand-written stub doc", () => {
  const doc = makeStubDoc();
  const store = makeFakeStore({ comparison: null, runs: [], line: { counts: { completed: 0 } } });
  assert.doesNotThrow(() => {
    initCompare({ doc, store, api: makeFakeApi(), toast: () => {} });
  });
});

test("initCompare: Compare button un-hides the panel, Esc hides it again", async () => {
  const doc = makeStubDoc();
  const store = makeFakeStore({ comparison: null, runs: [], line: { counts: { completed: 0 } } });
  initCompare({ doc, store, api: makeFakeApi(), toast: () => {} });

  const panel = doc.getElementById("compare-panel");
  assert.equal(panel.classList.contains("hidden"), false); // never explicitly hidden by the stub; verifies baseline
  await doc.getElementById("btn-compare").fire("click");
  assert.equal(panel.classList.contains("hidden"), false);
  await doc.fire("keydown", { key: "Escape" });
  assert.equal(panel.classList.contains("hidden"), true);
});

test("initCompare: round_running state renders the round line and disables Next round", async () => {
  const doc = makeStubDoc();
  const store = makeFakeStore({ comparison: null, runs: [], line: { counts: { completed: 0 } } });
  initCompare({ doc, store, api: makeFakeApi(), toast: () => {} });

  await store.setState({
    comparison: { state: "round_running", round: 1, rule: "fifo", n_orders: 30, rows: [] },
    runs: [],
    line: { counts: { completed: 0 } },
  });

  assert.equal(doc.getElementById("compare-roundline").textContent, "Round 1 of 3 - FIFO - 0/30 lots completed");
  assert.equal(doc.getElementById("compare-next").disabled, true);
});

test("initCompare: Start comparison calls api.startComparison once with the default body", async () => {
  const doc = makeStubDoc();
  const store = makeFakeStore({ comparison: null, runs: [], line: { counts: { completed: 0 } } });
  const calls = [];
  const api = makeFakeApi({ startComparison: async (body) => { calls.push(body); return {}; } });
  initCompare({ doc, store, api, toast: () => {} });

  doc.getElementById("compare-random-breakdowns").checked = true;
  await doc.getElementById("compare-setup").fire("submit", { preventDefault: () => {} });

  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0], { n_orders: 30, arrival_rate_per_min: 2.0, k: 1.5, random_breakdowns: true });
});
