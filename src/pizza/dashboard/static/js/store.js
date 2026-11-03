// Browser store: plain objects, no DOM access at import time (node can import it).
export const HISTORY_CAP = 600;
const FEED_CAP = 5;
const SEEN_CAP = 2000;
const STALE_MS = 5000;

const DEFAULT_COUNTS = { wip: 0, completed: 0, bank: 0, failed: 0, stocker_overflow: 0, stocker_down: 0 };

// Simulation speed multiplier (quick-et6); anything unusable falls back to 1x.
function normalizeScale(value) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : 1;
}

function trim(arr, cap) {
  return arr.length > cap ? arr.slice(arr.length - cap) : arr;
}

function runIdOf(snapshot) {
  return (snapshot.run && snapshot.run.run_id) || (snapshot.line && snapshot.line.run_id) || null;
}

// A comparison is locked when a round is actively running or just finished
// and awaiting "Next round" (COMP-01, Open Question 3 amendment): the rule
// switch, Reset, Stop line, rate slider and + Order are all disabled then.
const LOCKED_STATES = new Set(["round_running", "round_done"]);

export function isComparisonLocked(state) {
  const comparison = state && state.comparison;
  return Boolean(comparison && LOCKED_STATES.has(comparison.state));
}

// The dashboard's comparison status dict (plan 08, dashboard/comparison.py
// ComparisonRunner.status()) names its row list "results"; normalise it to
// "rows" here so the store's own contract (consumed by plan 11's compare.js)
// stays stable even if the backend's internal field name differs.
function normalizeComparison(status) {
  if (!status) return null;
  const { results, ...rest } = status;
  return { ...rest, rows: results || status.rows || [] };
}

export function createStore() {
  const state = {
    seq: 0,
    connection: "connecting",
    lastDataAt: null,
    run: { state: "none", run_id: null },
    line: { stations: {}, lots: [], counts: { ...DEFAULT_COUNTS }, paused: false },
    failureConfig: null,
    recipes: null,
    kpi: null,
    history: [],
    panel: null,
    panelHistory: {},
    selection: null,
    feed: [],
    rule: null,
    comparison: null,
    runs: [],
    timeScale: 1,
    hostSleep: null,
    dirty: false,
  };
  let snapshotSeq = 0;
  let currentRunId = null;
  const seen = new Set();
  const listeners = new Set();
  const factListeners = new Set();

  function notify() {
    for (const fn of [...listeners]) fn(state);
  }

  function replace(snapshot) {
    const runId = runIdOf(snapshot);
    if (state.selection && runId !== currentRunId) state.selection = null;
    currentRunId = runId;
    snapshotSeq = snapshot.seq || 0;
    seen.clear();
    state.seq = snapshotSeq;
    state.run = snapshot.run || { state: "none", run_id: null };
    const line = snapshot.line || {};
    state.line = {
      stations: line.stations || {},
      lots: line.lots || [],
      counts: line.counts || { ...DEFAULT_COUNTS },
      paused: Boolean(line.paused),
    };
    state.kpi = snapshot.kpi || null;
    state.history = trim(snapshot.history || [], HISTORY_CAP);
    state.panel = snapshot.panel || null;
    state.panelHistory = snapshot.panel_history || {};
    state.rule = (line && line.rule) || null;
    state.comparison = normalizeComparison(snapshot.comparison);
    state.runs = snapshot.runs || [];
    state.timeScale = normalizeScale(snapshot.time_scale);
    state.hostSleep = snapshot.host_sleep || null;
    state.connection = "live";
    state.dirty = true;
    notify();
  }

  function merge(tick) {
    if (tick.run) state.run = tick.run;
    if (tick.stations) state.line.stations = tick.stations;
    if (tick.counts) state.line.counts = tick.counts;
    if (typeof tick.paused === "boolean") state.line.paused = tick.paused;
    if (Array.isArray(tick.lots)) {
      let done = state.line.lots.filter((l) => l.state === "done");
      if (Array.isArray(tick.done_recent)) {
        const recent = tick.done_recent.filter(
          (l) => l && typeof l === "object" && typeof l.order_id === "string" && l.state === "done",
        );
        const ids = new Set(recent.map((l) => l.order_id));
        done = done.filter((l) => !ids.has(l.order_id)).concat(recent);
      }
      const doneIds = new Set(done.map((l) => l.order_id));
      state.line.lots = done.concat(tick.lots.filter((l) => !(l && doneIds.has(l.order_id))));
    }
    if ("kpi" in tick) state.kpi = tick.kpi || null;
    if ("panel" in tick) state.panel = tick.panel || null;
    if (typeof tick.rule === "string" && tick.rule !== state.rule) state.rule = tick.rule;
    if ("time_scale" in tick) state.timeScale = normalizeScale(tick.time_scale);
    if ("host_sleep" in tick) state.hostSleep = tick.host_sleep || null;
    if (tick.kpi_point) {
      state.history.push(tick.kpi_point);
      if (state.history.length > HISTORY_CAP) state.history.shift();
    }
    if (typeof tick.seq === "number") state.seq = Math.max(state.seq, tick.seq);
    state.dirty = true;
    notify();
  }

  // The comparison status dict replaces wholesale (COMP-01): the backend always
  // sends the full, authoritative shape (state, comparison_id, round, rule,
  // n_orders, seed, k, arrival_rate_per_min, random_breakdowns, run_id,
  // rows[], best, interrupted) - see normalizeComparison for the "results" ->
  // "rows" rename.
  function applyComparison(status) {
    state.comparison = normalizeComparison(status);
    state.dirty = true;
    notify();
  }

  // A single result row for one (comparison_id, round); the row itself has no
  // comparison_id/round fields (dashboard contract, plan 08), but run_id is
  // minted fresh per round (D-06/D-08) so it is a safe de-dupe key.
  function applyComparisonResult(row) {
    if (!row) return;
    const base = state.comparison || {};
    const rows = (base.rows || []).slice();
    const idx = rows.findIndex((r) => r.run_id === row.run_id);
    if (idx >= 0) rows[idx] = row;
    else rows.push(row);
    state.comparison = { ...base, rows };
    state.dirty = true;
    notify();
  }

  function applyFact(fact) {
    const seq = fact && fact.seq;
    if (typeof seq !== "number" || seq <= snapshotSeq || seen.has(seq)) return false;
    seen.add(seq);
    if (seen.size > SEEN_CAP) seen.delete(seen.values().next().value);
    if (fact.kind === "rule_changed") state.rule = fact.rule;
    state.feed = state.feed.concat([{
      seq,
      kind: fact.kind,
      ts: fact.ts,
      station: fact.station,
      order_id: fact.order_id,
      product: fact.product,
      status: fact.status,
      reason: fact.reason,
      stock_kind: fact.stock_kind,
      source: fact.source,
      by: fact.by,
      cause: fact.cause,
      repair_in_s: fact.repair_in_s,
      step: fact.step,
      visit: fact.visit,
      redelivered: fact.redelivered,
      rule: fact.rule,
    }]).slice(-FEED_CAP);
    state.dirty = true;
    for (const fn of [...factListeners]) fn(fact);
    notify();
    return true;
  }

  return {
    HISTORY_CAP,
    getState: () => state,
    subscribe(fn) {
      listeners.add(fn);
      return () => listeners.delete(fn);
    },
    onFact(fn) {
      factListeners.add(fn);
      return () => factListeners.delete(fn);
    },
    replace,
    merge,
    applyComparison,
    applyComparisonResult,
    setTimeScale(value) {
      state.timeScale = normalizeScale(value);
      state.dirty = true;
      notify();
    },
    setFailureConfig(cfg) {
      state.failureConfig = cfg || null;
      state.dirty = true;
      notify();
    },
    setRecipes(map) {
      state.recipes = map && typeof map === "object" ? map : null;
      state.dirty = true;
      notify();
    },
    applyFact,
    select(sel) {
      state.selection = sel || null;
      state.dirty = true;
      notify();
    },
    setConnection(value) {
      state.connection = value;
      state.dirty = true;
      notify();
    },
    noteData() {
      state.lastDataAt = Date.now();
    },
    isStale(now) {
      if (state.lastDataAt == null) return false;
      return now - state.lastDataAt > STALE_MS;
    },
  };
}
