// Compare panel + History/Recompute view-models and DOM wiring (COMP-01..04).
// Pure helpers below have no DOM access at import time so Node can import this
// module directly in tests; DOM wiring (initCompare) is added at the bottom.
// api.js/toast.js/hud.js are only touched inside init()/initCompare's body
// (and only by functions those modules call, never at import time), so this
// module stays importable from plain Node for the pure-helper tests.
import * as apiModule from "./api.js";
import { showToast } from "./toast.js";
import * as hud from "./hud.js";
import { formatPizzaRate, toLotsPerMin } from "./rate.js";

export const RULES = ["FIFO", "EDD", "SPT"];
const ROUND_ORDER = ["fifo", "edd", "spt"];
const COMPARE_METRICS = ["mean_cycle_time", "mean_tardiness", "pct_late", "makespan"];
const SECONDS_METRICS = new Set(["mean_cycle_time", "mean_tardiness", "makespan"]);
const PRODUCTS = ["margherita", "pepperoni", "veggie"];

// --- Copywriting contract (07-UI-SPEC.md), shared by DOM wiring and tests ---
export const PANEL_HEADING = "Compare dispatch rules";
export const PANEL_HELPER =
  "Runs FIFO, EDD and SPT on the same seeded lots and breakdown sequence. Each round uses a fresh run.";
export const TIGHTNESS_HELPER = "Lower = tighter due dates, more late lots";
export const RULE_GROUP_TITLE = "Rule applies to the next release. Priority first (smaller number first), then the selected rule.";
export const LOCKED_TITLE = "Locked while a comparison is running";
export const END_ROUND_TITLE = "Ends this round early; results are marked partial.";
export const TABLE_EMPTY_HEADING = "No comparison results yet";
export const TABLE_EMPTY_BODY =
  "Start a comparison to run FIFO, EDD and SPT on the same lots. Results appear here after each round.";
export const HISTORY_EMPTY_HEADING = "No past runs";
export const HISTORY_EMPTY_BODY =
  "Runs appear here after you start one. You can replay any run to verify its KPIs.";
export const RECOMPUTE_LABEL = "Recompute";
export const RECOMPUTE_PENDING_LABEL = "Replaying...";
export const RECOMPUTE_DISABLED_TITLE = "No stored snapshot for this run";
export const REPLAY_MATCH_TEXT = "Replay matches";
export const REPLAY_MISMATCH_TEXT = "Replay mismatch";
export const REPLAY_NOTE = "Read-only replay. Nothing was published.";
export const INTERRUPTED_BANNER =
  "A comparison was interrupted. Finished rounds are kept. Start a new comparison to continue.";
export const ERROR_CSV = "Could not export CSV. Check that the dashboard backend is running and try again.";
export const CANCEL_CONFIRM_TEXT =
  "Cancel comparison: Stop the comparison and keep finished rounds? The running round is discarded from the table. Past events stay in Kafka.";
export const CANCEL_KEEP_LABEL = "Keep running";
export const CANCEL_CONFIRM_LABEL = "Cancel comparison";
export const START_PENDING_LABEL = "Starting...";
export const END_PENDING_LABEL = "Ending round...";
export const START_LABEL = "Start comparison";
export const DOWNLOAD_CSV_LABEL = "Download CSV";

export const DEFAULT_N = 30;
export const DEFAULT_RATE = 4; // pizzas/min (2.0 lots/min)
export const DEFAULT_K = 1.5;

export function errorStartNext(round) {
  return `Could not start round ${round}. Check that the dashboard backend and RabbitMQ are running, then press Next round again.`;
}

export function errorReplay(runId) {
  return `Replay failed for run ${runId}. Check that Kafka is running and try again.`;
}

export function toastRoundStarted(round, rule) {
  return `Round ${round}/3 started: ${String(rule).toUpperCase()}.`;
}

export function toastRoundFinished(round, rule) {
  return `Round ${round}/3 finished: ${String(rule).toUpperCase()}. Press Next round when ready.`;
}

// --- Pure view-model helpers (Task 1) ---

// Best (lowest) value per metric across the comparison rows (COMP-02). Ties or
// fewer than two numeric values for a metric yield no marker (empty set).
export function bestByMetric(rows) {
  const list = Array.isArray(rows) ? rows : [];
  const best = {};
  for (const metric of COMPARE_METRICS) {
    const numeric = list
      .map((r) => ({ rule: r.rule, value: r[metric] }))
      .filter((v) => typeof v.value === "number" && Number.isFinite(v.value));
    if (numeric.length < 2) {
      best[metric] = new Set();
      continue;
    }
    const min = Math.min(...numeric.map((v) => v.value));
    best[metric] = new Set(numeric.filter((v) => v.value === min).map((v) => v.rule));
  }
  return best;
}

export function formatCell(metric, value) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return "-";
  if (metric === "n_orders") return String(Math.round(Number(value)));
  if (metric === "pct_late") return `${(Number(value) * 100).toFixed(1)}%`;
  if (SECONDS_METRICS.has(metric)) return Number(value).toFixed(1);
  return String(value);
}

export function withBestSuffix(text, isBest) {
  return isBest ? `${text} (best)` : text;
}

// Round stepper model: "done" rounds are the ones that already produced a
// result row (rows.length, sequential by round), the current round is
// "running" only while status.state === "round_running"; everything else
// (including a cancelled/discarded in-flight round) is "pending" - this also
// matches the cancel-modal copy ("the running round is discarded").
export function stepperModel(status) {
  const s = status || {};
  const doneCount = Array.isArray(s.rows) ? s.rows.length : 0;
  const round = s.round || 1;
  return ROUND_ORDER.map((rule, i) => {
    const stepNum = i + 1;
    let state;
    if (stepNum <= doneCount) state = "done";
    else if (s.state === "round_running" && stepNum === round) state = "running";
    else state = "pending";
    return { step: stepNum, rule: rule.toUpperCase(), state, current: state === "running" };
  });
}

export function roundLine(status, completed) {
  const s = status || {};
  const n = s.n_orders == null ? "-" : s.n_orders;
  const rule = String(s.rule || "").toUpperCase();
  const done = Number.isFinite(completed) ? completed : 0;
  return `Round ${s.round || 1} of 3 - ${rule} - ${done}/${n} lots completed`;
}

// Controls row model: End round / Cancel / the primary CTA that walks
// "Start comparison" -> "Next round: {RULE}" -> "Download CSV" (D-06).
export function controlsModel(status) {
  const s = status || {};
  const state = s.state || "idle";
  const round = s.round || 1;
  const running = state === "round_running";
  const done = state === "round_done";
  const finished = state === "finished";
  let primary;
  if (state === "idle" || state === "error") {
    primary = { action: "start", label: START_LABEL, disabled: false, title: "" };
  } else if (finished) {
    primary = { action: "csv", label: DOWNLOAD_CSV_LABEL, disabled: false, title: "" };
  } else {
    const nextRule = done && round < 3 ? RULES[round] : null;
    primary = {
      action: "next",
      label: nextRule ? `Next round: ${nextRule}` : "Next round",
      disabled: !done,
      title: running ? `Round ${round} is still running` : "",
    };
  }
  return {
    primary,
    endRound: { disabled: !running, title: END_ROUND_TITLE },
    cancel: { disabled: !(running || done) },
  };
}

// Scoreboard strip (after round 3 only): winner/value/"-x%" vs worst, for
// Mean tardiness, Late % and Makespan. Returns null unless exactly 3 rows.
const SCORE_METRICS = [
  { key: "mean_tardiness", label: "Mean tardiness" },
  { key: "pct_late", label: "Late %" },
  { key: "makespan", label: "Makespan" },
];

export function scoreboardModel(rows) {
  if (!Array.isArray(rows) || rows.length !== 3) return null;
  const out = {};
  for (const { key, label } of SCORE_METRICS) {
    const vals = rows
      .map((r) => ({ rule: r.rule, value: r[key] }))
      .filter((v) => typeof v.value === "number" && Number.isFinite(v.value));
    if (vals.length < 2) {
      out[key] = null;
      continue;
    }
    const best = vals.reduce((a, b) => (b.value < a.value ? b : a));
    const worst = vals.reduce((a, b) => (b.value > a.value ? b : a));
    const pct = worst.value === 0 ? 0 : ((worst.value - best.value) / worst.value) * 100;
    out[key] = {
      label,
      rule: String(best.rule || "").toUpperCase(),
      value: best.value,
      valueText: formatCell(key, best.value),
      vsWorstText: `vs worst: -${pct.toFixed(1)}%`,
    };
  }
  return out;
}

// "Scrap orders are excluded: FIFO {a}, EDD {b}, SPT {c}." only when any
// round has a scrap order; null otherwise (nothing to show under the table).
export function failedNote(rows) {
  const list = Array.isArray(rows) ? rows : [];
  if (!list.length) return null;
  const byRule = {};
  for (const r of list) byRule[String(r.rule || "").toLowerCase()] = r.n_failed || 0;
  const any = ROUND_ORDER.some((r) => (byRule[r] || 0) > 0);
  if (!any) return null;
  const parts = ROUND_ORDER.map((r) => `${r.toUpperCase()} ${byRule[r] || 0}`);
  return `Scrap lots are excluded: ${parts.join(", ")}.`;
}

// Recompute verification card (COMP-03, D-13/D-14): the report shape comes
// from kpi/recompute.py::run_recompute (run_id, matched, tolerance, fields[]).
export function recomputeCardModel(report) {
  const r = report || {};
  const chip = r.matched
    ? { text: REPLAY_MATCH_TEXT, tone: "ok" }
    : { text: REPLAY_MISMATCH_TEXT, tone: "warn" };
  const rows = (r.fields || []).map((f) => ({
    field: f.field,
    stored: f.stored,
    recomputed: f.recomputed,
    diffText: f.ok ? "ok" : `! ${f.diff}`,
    ok: Boolean(f.ok),
  }));
  return {
    chip,
    rows,
    footer: `Compared with tolerance ${r.tolerance}.`,
    note: REPLAY_NOTE,
    cli: `python -m pizza.kpi.recompute --run-id ${r.run_id}`,
  };
}

// History list rows (COMP-03): run.started/comparison facts, newest first
// (server already sorts/caps; this is a defensive client-side cap/format).
export function historyRows(runs) {
  const list = Array.isArray(runs) ? runs : [];
  if (!list.length) {
    return { rows: [], empty: { heading: HISTORY_EMPTY_HEADING, body: HISTORY_EMPTY_BODY } };
  }
  const rows = list.slice(0, 20).map((r) => ({
    runId: r.run_id,
    shortId: shortRunId(r.run_id),
    rule: r.rule ? String(r.rule).toUpperCase() : "-",
    seed: r.seed == null ? "-" : r.seed,
    orders: r.n_orders == null ? "-" : r.n_orders,
    startedTs: r.started_ts || null,
    recompute: {
      disabled: r.has_snapshot === false,
      title: r.has_snapshot === false ? RECOMPUTE_DISABLED_TITLE : "",
    },
  }));
  return { rows, empty: null };
}

export function shortRunId(runId) {
  return String(runId || "").slice(0, 8);
}

// "HH:MM" from an ISO timestamp (UTC or offset-aware); "-" when absent/invalid.
export function shortTime(isoTs) {
  if (typeof isoTs !== "string") return "-";
  const m = /T(\d{2}):(\d{2})/.exec(isoTs);
  return m ? `${m[1]}:${m[2]}` : "-";
}

// Footer recap: "Seed {s} - {N} lots - k {k} - {rate} pizzas/min - breakdowns {on|off}".
export function recapLine(status) {
  const s = status || {};
  const seed = s.seed == null ? "-" : s.seed;
  const n = s.n_orders == null ? "-" : s.n_orders;
  const k = s.k == null ? "-" : s.k;
  const rate = typeof s.arrival_rate_per_min === "number" ? formatPizzaRate(s.arrival_rate_per_min) : "- pizzas/min";
  const breakdowns = s.random_breakdowns ? "on" : "off";
  return `Seed ${seed} - ${n} lots - k ${k} - ${rate} - breakdowns ${breakdowns}`;
}

// "Compare R{n}/3" live-badge override text while a round runs or awaits
// "Next round"; null otherwise (hud.js's own badgeView applies then).
export function liveBadgeText(status) {
  const s = status || {};
  if (s.state === "round_running" || s.state === "round_done") {
    return `Compare R${s.round || 1}/3`;
  }
  return null;
}

// Request body for POST /api/comparison/start (dashboard/comparison.py
// ComparisonStartRequest): blank seed omitted, numbers coerced, weights
// omitted when every product is weighted 1 (the request model's own
// default), random_breakdowns always sent (checked by default, D-07).
export function buildComparisonBody(form) {
  const v = form || {};
  const body = {};
  const rawSeed = v.seed === undefined || v.seed === null ? "" : String(v.seed).trim();
  if (rawSeed !== "") {
    const seed = Number(rawSeed);
    if (Number.isInteger(seed)) body.seed = seed;
  }
  const rawN = v.n === undefined || v.n === null || v.n === "" ? null : Number(v.n);
  body.n_orders = Number.isFinite(rawN) ? rawN : DEFAULT_N;
  const rawRate = v.rate === undefined || v.rate === null || v.rate === "" ? null : Number(v.rate);
  body.arrival_rate_per_min = toLotsPerMin(Number.isFinite(rawRate) ? rawRate : DEFAULT_RATE);
  const rawK = v.k === undefined || v.k === null || v.k === "" ? null : Number(v.k);
  body.k = Number.isFinite(rawK) ? rawK : DEFAULT_K;
  const weights = v.weights || {};
  const w = {};
  let allOne = true;
  for (const p of PRODUCTS) {
    const raw = weights[p];
    const val = raw === undefined || raw === null || raw === "" ? 1 : Number(raw);
    w[p] = Number.isFinite(val) ? val : 1;
    if (w[p] !== 1) allOne = false;
  }
  if (!allOne) body.weights = w;
  body.random_breakdowns = v.randomBreakdowns === undefined ? true : Boolean(v.randomBreakdowns);
  return body;
}

// --- DOM wiring (Task 2) ---
// initCompare takes its collaborators as plain arguments (no globals, no
// window/document access outside the passed `doc`) so it is testable with a
// hand-written stub document (see tests/js/compare.test.mjs). All dynamic
// text goes through textContent/createElement only (T-07-47; never via the
// HTML-parsing DOM sink).

function mk(doc, tag, cls, text) {
  const n = doc.createElement(tag);
  if (cls) n.className = cls;
  if (text != null) n.textContent = text;
  return n;
}

function clear(host) {
  if (!host || !host.children) return;
  while (host.children.length) host.removeChild(host.children[host.children.length - 1]);
}

function setHidden(el, hidden) {
  if (el && el.classList) el.classList.toggle("hidden", Boolean(hidden));
}

export function initCompare({ doc, store, api, toast }) {
  if (!doc || !store || !api || !toast) return;
  const panel = doc.getElementById("compare-panel");
  const openBtn = doc.getElementById("btn-compare");
  if (!panel || !openBtn) return;

  const closeBtn = doc.getElementById("compare-close");
  const heading = doc.getElementById("compare-heading");
  const banner = doc.getElementById("compare-banner");
  const form = doc.getElementById("compare-setup");
  const startBtn = doc.getElementById("compare-start");
  const seedInput = doc.getElementById("compare-seed");
  const nInput = doc.getElementById("compare-n");
  const rateInput = doc.getElementById("compare-rate");
  const kInput = doc.getElementById("compare-k");
  const breakdownsInput = doc.getElementById("compare-random-breakdowns");
  const recap = doc.getElementById("compare-recap");
  const stepperEl = doc.getElementById("compare-stepper");
  const roundLineEl = doc.getElementById("compare-roundline");
  const controlsEl = doc.getElementById("compare-controls");
  const endBtn = doc.getElementById("compare-end");
  const nextBtn = doc.getElementById("compare-next");
  const cancelBtn = doc.getElementById("compare-cancel");
  const emptyEl = doc.getElementById("compare-empty");
  const tableEl = doc.getElementById("compare-table");
  const tbody = doc.getElementById("compare-table-body");
  const exportLink = doc.getElementById("compare-export");
  const failedNoteEl = doc.getElementById("compare-failed-note");
  const scoreboardEl = doc.getElementById("compare-scoreboard");
  const footerEl = doc.getElementById("compare-footer");
  const historyList = doc.getElementById("compare-history-list");
  const historyEmpty = doc.getElementById("compare-history-empty");
  const historyToggle = doc.getElementById("compare-history-toggle");
  const historyBody = doc.getElementById("compare-history-body");
  const cancelModal = doc.getElementById("compare-cancel-modal");
  const cancelKeep = doc.getElementById("compare-cancel-keep");
  const cancelConfirm = doc.getElementById("compare-cancel-confirm");
  const backdrop = doc.getElementById("modal-backdrop");

  let open = false;
  let prevState = null;
  let prevRound = null;
  const recomputeState = new Map(); // run_id -> { pending, report }

  function currentStatus() {
    return store.getState().comparison || {};
  }

  function reason(err) {
    return (api.errorReason && api.errorReason(err)) || "Request failed";
  }

  // --- open/close (focus moves to the heading on open; Esc closes; closing
  // never stops a running comparison) ---
  function show() {
    open = true;
    setHidden(panel, false);
    if (panel.setAttribute) panel.setAttribute("aria-hidden", "false");
    if (openBtn.setAttribute) openBtn.setAttribute("aria-expanded", "true");
    if (heading && heading.focus) heading.focus();
  }

  function hide() {
    if (!open) return;
    open = false;
    setHidden(panel, true);
    if (panel.setAttribute) panel.setAttribute("aria-hidden", "true");
    if (openBtn.setAttribute) openBtn.setAttribute("aria-expanded", "false");
    if (openBtn.focus) openBtn.focus();
  }

  if (openBtn.setAttribute) {
    openBtn.setAttribute("aria-haspopup", "dialog");
    openBtn.setAttribute("aria-expanded", "false");
  }
  openBtn.addEventListener("click", () => (open ? hide() : show()));
  if (closeBtn) closeBtn.addEventListener("click", hide);
  doc.addEventListener("keydown", (event) => {
    if (open && event && event.key === "Escape") {
      if (event.preventDefault) event.preventDefault();
      hide();
    }
  });

  // --- cancel confirm modal ---
  function openCancelModal() {
    if (!cancelModal) return;
    if (doc.dispatchEvent) doc.dispatchEvent(new CustomEvent("modal-open"));
    setHidden(backdrop, false);
    setHidden(cancelModal, false);
    const focusTarget = cancelKeep || cancelConfirm;
    if (focusTarget && focusTarget.focus) focusTarget.focus();
  }
  function closeCancelModal() {
    if (!cancelModal) return;
    setHidden(cancelModal, true);
    setHidden(backdrop, true);
  }
  if (cancelBtn) cancelBtn.addEventListener("click", openCancelModal);
  if (cancelKeep) cancelKeep.addEventListener("click", closeCancelModal);
  if (cancelConfirm) {
    cancelConfirm.addEventListener("click", async () => {
      closeCancelModal();
      try {
        await api.cancelComparison();
      } catch (err) {
        toast({ kind: "error", text: reason(err) }, doc);
      }
    });
  }

  // --- setup form -> POST /api/comparison/start ---
  function readForm() {
    const weights = {};
    if (form && form.querySelectorAll) {
      for (const input of form.querySelectorAll(".weight-input")) {
        weights[input.dataset.product] = input.value;
      }
    }
    return {
      seed: seedInput ? seedInput.value : "",
      n: nInput ? nInput.value : "",
      rate: rateInput ? rateInput.value : "",
      k: kInput ? kInput.value : "",
      weights,
      randomBreakdowns: breakdownsInput ? Boolean(breakdownsInput.checked) : true,
    };
  }

  // Generic pending-button wrapper (Starting... / Ending round...): disables
  // the button, swaps its label, and shows the error toast if no matching
  // status arrives within 3 s (the server ack still re-enables it via render
  // when the real status transition lands; `getErrorText` is evaluated lazily
  // so the round number in the message is always current).
  function withPending(btn, pendingLabel, action, getErrorText) {
    return async () => {
      if (!btn || btn.disabled) return;
      const original = btn.textContent;
      btn.disabled = true;
      if (pendingLabel != null) btn.textContent = pendingLabel;
      const timer = setTimeout(() => {
        toast({ kind: "error", text: getErrorText() }, doc);
      }, 3000);
      try {
        await action();
      } catch (err) {
        clearTimeout(timer);
        btn.disabled = false;
        btn.textContent = original;
        toast({ kind: "error", text: reason(err) }, doc);
        return;
      }
      clearTimeout(timer);
    };
  }

  if (form) {
    form.addEventListener("submit", async (event) => {
      if (event && event.preventDefault) event.preventDefault();
      await withPending(
        startBtn,
        START_PENDING_LABEL,
        async () => {
          const body = buildComparisonBody(readForm());
          await api.startComparison(body);
        },
        () => errorStartNext(1),
      )();
    });
  }

  if (endBtn) {
    endBtn.addEventListener("click", () => {
      withPending(endBtn, END_PENDING_LABEL, async () => {
        await api.endRound();
      }, () => errorStartNext(currentStatus().round || 1))();
    });
  }

  function downloadCsv() {
    try {
      const href = api.csvUrl(currentStatus().comparison_id);
      if (exportLink && exportLink.setAttribute) exportLink.setAttribute("href", href);
      const win = doc.defaultView;
      if (win && win.location) win.location.href = href;
    } catch (err) {
      toast({ kind: "error", text: ERROR_CSV }, doc);
    }
  }

  if (nextBtn) {
    nextBtn.addEventListener("click", () => {
      const status = currentStatus();
      const model = controlsModel(status);
      if (model.primary.action === "csv") {
        downloadCsv();
        return;
      }
      if (model.primary.disabled) return;
      withPending(nextBtn, null, async () => {
        await api.nextRound();
      }, () => errorStartNext((status.round || 1) + 1))();
    });
  }

  // --- History / Recompute ---
  async function runRecompute(runId) {
    recomputeState.set(runId, { pending: true, report: null });
    render();
    try {
      const report = await api.recomputeRun(runId);
      recomputeState.set(runId, { pending: false, report });
    } catch (err) {
      recomputeState.delete(runId);
      toast({ kind: "error", text: errorReplay(runId) }, doc);
    }
    render();
  }

  function renderRecomputeCard(report) {
    const model = recomputeCardModel(report);
    const card = mk(doc, "div", "compare-recompute-card", null);
    card.appendChild(mk(doc, "span", `chip ${model.chip.tone}`, model.chip.text));
    const table = mk(doc, "table", null, null);
    const thead = mk(doc, "tr", null, null);
    for (const h of ["Field", "Stored", "Recomputed", "Diff"]) thead.appendChild(mk(doc, "th", null, h));
    table.appendChild(thead);
    for (const row of model.rows) {
      const tr = mk(doc, "tr", null, null);
      tr.appendChild(mk(doc, "td", null, row.field));
      tr.appendChild(mk(doc, "td", null, String(row.stored)));
      tr.appendChild(mk(doc, "td", null, String(row.recomputed)));
      tr.appendChild(mk(doc, "td", row.ok ? null : "warn-text", row.diffText));
      table.appendChild(tr);
    }
    card.appendChild(table);
    card.appendChild(mk(doc, "p", null, model.footer));
    card.appendChild(mk(doc, "p", null, model.note));
    card.appendChild(mk(doc, "p", null, `CLI: ${model.cli}`));
    return card;
  }

  function renderHistory(runs) {
    if (!historyList) return;
    clear(historyList);
    const { rows, empty } = historyRows(runs);
    setHidden(historyEmpty, !empty);
    if (empty && historyEmpty) {
      clear(historyEmpty);
      historyEmpty.appendChild(mk(doc, "p", "empty-copy", empty.heading));
      historyEmpty.appendChild(mk(doc, "p", "empty-copy", empty.body));
    }
    for (const r of rows) {
      const row = mk(doc, "div", "compare-history-row", null);
      row.appendChild(mk(doc, "span", "row-name", r.shortId));
      row.appendChild(mk(doc, "span", "row-sub", r.rule));
      row.appendChild(mk(doc, "span", "row-sub", String(r.seed)));
      row.appendChild(mk(doc, "span", "row-sub", String(r.orders)));
      row.appendChild(mk(doc, "span", "row-sub", shortTime(r.startedTs)));
      const btn = mk(doc, "button", "btn", RECOMPUTE_LABEL);
      btn.type = "button";
      const pending = recomputeState.get(r.runId);
      if (pending && pending.pending) {
        btn.disabled = true;
        btn.textContent = RECOMPUTE_PENDING_LABEL;
      } else {
        btn.disabled = r.recompute.disabled;
        btn.title = r.recompute.title;
      }
      btn.addEventListener("click", () => runRecompute(r.runId));
      row.appendChild(btn);
      historyList.appendChild(row);

      const result = recomputeState.get(r.runId);
      if (result && result.report) historyList.appendChild(renderRecomputeCard(result.report));
    }
  }

  if (historyToggle && historyBody) {
    historyToggle.addEventListener("click", () => {
      const expanded = historyToggle.getAttribute ? historyToggle.getAttribute("aria-expanded") === "true" : true;
      const next = !expanded;
      if (historyToggle.setAttribute) historyToggle.setAttribute("aria-expanded", String(next));
      setHidden(historyBody, !next);
    });
  }

  // --- render (called on every store notify; also drives the live-badge
  // override and the start/finish toasts on state transitions) ---
  function renderStepper(status) {
    if (!stepperEl) return;
    clear(stepperEl);
    for (const step of stepperModel(status)) {
      const li = mk(doc, "li", `step-pill ${step.state}`, `${step.step} ${step.rule}`);
      if (step.current && li.setAttribute) li.setAttribute("aria-current", "step");
      stepperEl.appendChild(li);
    }
  }

  function renderControls(status) {
    const model = controlsModel(status);
    if (endBtn) {
      endBtn.disabled = model.endRound.disabled;
      endBtn.title = model.endRound.title;
    }
    if (nextBtn) {
      nextBtn.disabled = model.primary.disabled;
      nextBtn.title = model.primary.title;
      nextBtn.textContent = model.primary.label;
    }
    if (cancelBtn) cancelBtn.disabled = model.cancel.disabled;
  }

  function renderTable(rows) {
    if (!tbody) return;
    clear(tbody);
    const best = bestByMetric(rows);
    for (const row of rows) {
      const tr = mk(doc, "tr", null, null);
      const ruleCell = mk(doc, "td", null, null);
      ruleCell.appendChild(mk(doc, "span", "chip", String(row.rule || "").toUpperCase()));
      if (row.partial) ruleCell.appendChild(mk(doc, "span", "chip idle", "partial"));
      tr.appendChild(ruleCell);
      tr.appendChild(mk(doc, "td", null, formatCell("n_orders", row.n_orders)));
      for (const metric of ["mean_cycle_time", "mean_tardiness", "pct_late", "makespan"]) {
        const isBest = Boolean(best[metric] && best[metric].has(row.rule));
        const text = withBestSuffix(formatCell(metric, row[metric]), isBest);
        tr.appendChild(mk(doc, "td", isBest ? "best" : null, text));
      }
      tbody.appendChild(tr);
    }
  }

  function renderScoreboard(sb) {
    if (!scoreboardEl) return;
    clear(scoreboardEl);
    for (const key of ["mean_tardiness", "pct_late", "makespan"]) {
      const m = sb[key];
      if (!m) continue;
      const card = mk(doc, "div", "score-card", null);
      card.appendChild(mk(doc, "div", "score-label", m.label));
      card.appendChild(mk(doc, "div", "score-rule", m.rule));
      card.appendChild(mk(doc, "div", "score-value", m.valueText));
      card.appendChild(mk(doc, "div", "score-sub", m.vsWorstText));
      scoreboardEl.appendChild(card);
    }
  }

  function maybeToastTransition(status) {
    const state = status.state || null;
    const round = status.round || null;
    if (state === "round_running" && (prevState !== "round_running" || prevRound !== round)) {
      toast({ text: toastRoundStarted(round || 1, status.rule || "") }, doc);
    }
    if (state === "round_done" && prevState !== "round_done") {
      toast({ text: toastRoundFinished(round || 1, status.rule || "") }, doc);
    }
  }

  function render() {
    const state = store.getState();
    const status = state.comparison || {};
    const rows = status.rows || [];
    const started = Boolean(status.state) && status.state !== "idle";

    if (banner) {
      const interrupted = Boolean(status.interrupted);
      banner.textContent = interrupted ? INTERRUPTED_BANNER : "";
      setHidden(banner, !interrupted);
    }

    setHidden(form, started);
    setHidden(recap, !started);
    if (recap && started) recap.textContent = recapLine(status);

    setHidden(stepperEl, !started);
    setHidden(roundLineEl, !started);
    setHidden(controlsEl, !started);
    if (started) {
      renderStepper(status);
      const completed = (state.line && state.line.counts && state.line.counts.completed) || 0;
      if (roundLineEl) roundLineEl.textContent = roundLine(status, completed);
      renderControls(status);
    }

    setHidden(emptyEl, rows.length > 0);
    setHidden(tableEl, rows.length === 0);
    if (rows.length > 0) {
      renderTable(rows);
      const note = failedNote(rows);
      if (failedNoteEl) failedNoteEl.textContent = note || "";
      setHidden(failedNoteEl, !note);
      if (exportLink && exportLink.setAttribute) exportLink.setAttribute("href", api.csvUrl(status.comparison_id));
    }

    const sb = scoreboardModel(rows);
    setHidden(scoreboardEl, !sb);
    if (sb) renderScoreboard(sb);

    setHidden(footerEl, rows.length === 0);
    if (footerEl && rows.length > 0) footerEl.textContent = recapLine(status);

    renderHistory(state.runs || []);

    if (hud.setBadgeOverride) hud.setBadgeOverride(liveBadgeText(status));

    maybeToastTransition(status);
    prevState = status.state || null;
    prevRound = status.round || null;
  }

  store.subscribe(render);
  render();
}

// Production entry point, wired from main.js's OPTIONAL module loader
// (ctx = { store, el, document }); uses the real api.js/toast.js/hud.js so
// initCompare itself stays collaborator-generic and unit-testable.
export function init(ctx) {
  const { store, document: doc } = ctx;
  initCompare({ doc, store, api: apiModule, toast: showToast });
}
