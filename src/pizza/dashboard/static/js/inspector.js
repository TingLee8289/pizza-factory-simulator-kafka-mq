// Inspector card (D-11). Values come from authoritative store state only;
// utilization is read from the relayed kpi payload and never derived in the browser.
import {
  breakStation, repairStation, bankLot, scrapLot, releaseLot, cancelLotHold, setLotPriority, getFailureConfig, getLotEta, getLotDetail, getRecipes, setRecipe, errorReason,
} from "./api.js";
import { showToast } from "./toast.js";
import { parsePriority, PRIORITY_DEFAULT, PRIORITY_ERROR, PRIORITY_MIN, PRIORITY_MAX } from "./priority.js";

export const ROUTE =["spin", "sauce", "cheese", "toppings", "bake", "slice", "package"];
export const DISPLAY = {
  spin: "Spin", sauce: "Sauce", cheese: "Cheese", toppings: "Toppings",
  bake: "Bake", slice: "Slice", package: "Package",
};
const RENDER_MS = 200;
const QUEUE_AMBER_AT = 4;
const DASH = "—";

// Local-time HH:mm:ss (same as clockLabel), or "" when unavailable so callers can use `||` fallback.
export function clockOf(iso) {
  const label = clockLabel(iso);
  return label === DASH ? "" : label;
}

const REASONS = { capacity: "Capacity", customer: "Customer", schedule: "Schedule" };
export const SCRAP_REASONS = {
  operator: "Operator decision", quality: "Quality", material: "Material", other: "Other",
};
const REPAIR_OPTIONS = [["", "Auto (MTTR)"], ["10", "10 s"], ["30", "30 s"], ["60", "60 s"]];

function stationName(id) {
  return DISPLAY[id] || id || DASH;
}

function mmss(seconds) {
  if (typeof seconds !== "number" || !Number.isFinite(seconds) || seconds < 0) return DASH;
  const total = Math.floor(seconds);
  const m = Math.floor(total / 60);
  const s = total % 60;
  return `${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
}

export const RECIPE_RETRY_MIN_MS = 2000;
export const RECIPE_RETRY_MAX_MS = 30000;

// A station's recipe table counts as loaded only when it is a non-empty object.
// Null/undefined/{} all mean "not loaded yet", so the lazy fetch and the model
// agree on what missing data looks like.
export function recipeTableFor(state, stationId) {
  const recipes = state && state.recipes;
  const table = recipes && recipes[stationId];
  if (!table || typeof table !== "object" || Array.isArray(table)) return null;
  return Object.keys(table).length > 0 ? table : null;
}

// Exponential backoff: 0 or 1 failure -> 2s, then 4s, 8s, ... capped at 30s.
export function recipeRetryDelay(failures) {
  const n = Number.isFinite(failures) && failures > 0 ? failures : 0;
  return Math.min(RECIPE_RETRY_MAX_MS, RECIPE_RETRY_MIN_MS * 2 ** Math.max(0, n - 1));
}

// One −/+ step on the recipe table: 1 s, kept within [1, 3600] s; null when the step is a no-op.
export const RECIPE_MIN_S = 1;
export const RECIPE_MAX_S = 3600;
export function nextRecipeSeconds(value, delta) {
  if (typeof value !== "number" || !Number.isFinite(value)) return null;
  const next = Math.round(Math.min(RECIPE_MAX_S, Math.max(RECIPE_MIN_S, value + delta)) * 10) / 10;
  return next === value ? null : next;
}

// Recipe editor draft (sketch 006 A2): edits stay local until Apply. `draft` maps product to a
// typed string or a number; products missing from it show the live value.
export function recipeDraftView(live, draft) {
  const rows = Object.keys(live || {}).map((product) => {
    const liveValue = live[product];
    const has = Boolean(draft) && Object.prototype.hasOwnProperty.call(draft, product);
    const raw = has ? draft[product] : liveValue;
    const n = typeof raw === "number" ? raw : String(raw).trim() === "" ? NaN : Number(raw);
    const valid = Number.isFinite(n) && n >= RECIPE_MIN_S && n <= RECIPE_MAX_S;
    const value = valid ? Math.round(n * 10) / 10 : n;
    return { product, live: liveValue, raw, value, valid, dirty: !valid || value !== liveValue };
  });
  const changed = rows.filter((r) => r.valid && r.dirty);
  const invalid = rows.some((r) => !r.valid);
  return { rows, changed, invalid, canApply: changed.length > 0 && !invalid, canReset: rows.some((r) => r.dirty) };
}

// Orange dot on the station's Recipe tab (sketch 007): shown while another tab is open and the
// recipe draft still holds unapplied (or invalid) edits.
export function recipeTabDot(live, draft, tab) {
  if (tab === "recipe" || !live || !draft) return false;
  return recipeDraftView(live, draft).canReset;
}

export function shouldFetchRecipes(state, gate, nowMs, selectionChanged = false) {
  if (!state) return false;
  const sel = state.selection;
  if (!sel || sel.kind !== "station") return false;
  if (recipeTableFor(state, sel.id)) return false;
  const g = gate || {};
  if (g.inflight) return false;
  if (!selectionChanged && typeof g.lastAttemptAt === "number"
      && nowMs - g.lastAttemptAt < recipeRetryDelay(g.failures)) return false;
  return true;
}

export function inspectorModel(state, detail = null) {
  const sel = state && state.selection;
  if (!sel) return null;
  const line = (state && state.line) || {};
  if (sel.kind === "station") {
    const st = line.stations && line.stations[sel.id];
    if (!st) return { kind: "gone" };
    const k = state.kpi && state.kpi.stations && state.kpi.stations[sel.id];
    const util = k && typeof k.utilization === "number" ? k.utilization : null;
    const avail = k && typeof k.availability === "number" ? k.availability : null;
    const downS = k && typeof k.down_s === "number" ? k.down_s : null;
    const cfg = state.failureConfig || null;
    const recipes = recipeTableFor(state, sel.id);
    const run = state.run || {};
    let randomBreakdowns = "Random breakdowns: Off";
    if (run.random_breakdowns) {
      randomBreakdowns = cfg
        ? `Random breakdowns: MTBF ${cfg.mtbf_s} s / MTTR ${cfg.mttr_s} s`
        : "Random breakdowns: On";
    }
    return {
      kind: "station",
      name: sel.id,
      status: st.status || "offline",
      queue: typeof st.queue === "number" ? st.queue : 0,
      utilization: util,
      currentLot: st.current_lot || null,
      cause: st.cause || null,
      repairEtaTs: st.repair_eta_ts || null,
      blocked: typeof st.blocked === "number" ? st.blocked : 0,
      availability: avail,
      downS,
      randomBreakdowns,
      recipes,
    };
  }
  if (sel.kind === "lot") {
    const lot = (line.lots || []).find((l) => l.order_id === sel.id);
    if (!lot) return { kind: "gone" };
    // Heavy history is not streamed; it comes from GET /api/lot/{id} for the selected lot only.
    const heavy = detail && detail.order_id === lot.order_id ? detail : lot;
    return {
      kind: "lot",
      id: lot.order_id,
      product: lot.product,
      qty: lot.qty,
      priority: Number.isFinite(Number(lot.priority)) ? Number(lot.priority) : PRIORITY_DEFAULT,
      // Effective Due: planned due_ts pushed later by pause seconds (derived server-side).
      due: lot.due_eff_ts || lot.due_ts || null,
      step: lot.state === "done" ? "done" : lot.station || null,
      state: lot.state,
      station: lot.station || null,
      visit: typeof lot.visit === "number" ? lot.visit : 1,
      dispatched: Boolean(lot.dispatched),
      cooling: Boolean(lot.cooling),
      stocker: lot.stocker || null,
      bank: lot.bank || null,
      hold: lot.hold || null,
      failed: lot.failed || null,
      retrySince: lot.retry_since_ts || null,
      reworkCount: typeof lot.rework_count === "number" ? lot.rework_count : 0,
      failures: Array.isArray(heavy.failures) ? heavy.failures : [],
      visits: Array.isArray(heavy.visits) ? heavy.visits : [],
      priorityChanges: Array.isArray(heavy.priority_changes) ? heavy.priority_changes : [],
      eta: lot.eta_ts || null,
      stationEta: heavy.station_eta && typeof heavy.station_eta === "object" ? heavy.station_eta : null,
      etaNow: lot.eta_now_ts || null,
      etaError: typeof lot.eta_error_s === "number" && Number.isFinite(lot.eta_error_s) ? lot.eta_error_s : null,
      completedTs: lot.completed_ts || null,
    };
  }
  return { kind: "gone" };
}

export function breakButtons(m) {
  const pending = Boolean(m && m.pending);
  const status = m && m.status;
  const running = status === "idle" || status === "busy";
  let breakTip = "";
  if (status === "down") breakTip = "Station is already down";
  else if (status === "offline") breakTip = "Station is offline";
  return {
    break: { enabled: running && !pending, tooltip: breakTip },
    repair: { enabled: status === "down" && !pending, tooltip: status === "down" ? "" : status === "offline" ? "Station is offline" : "Station is running" },
  };
}

export function downLabel(m, nowMs) {
  if (!m || m.status !== "down") return "Down";
  const eta = m.repairEtaTs ? Date.parse(m.repairEtaTs) : NaN;
  if (!Number.isFinite(eta)) return "Down";
  const secs = Math.max(0, Math.ceil((eta - nowMs) / 1000));
  return `Down - auto-repair in ${secs} s`;
}

const HOLD_PENDING_TIP = "Hold already requested - cancel it first";

// Chip text for a pending deferred bank / scrap request, e.g. "Bank after Bake".
export function holdChip(m) {
  const h = m && m.hold;
  if (!h) return null;
  return `${h.kind === "scrap" ? "Scrap" : "Bank"} after ${stationName(h.station)}`;
}

export function bankControls(m, reason, cfg) {
  const st = stationName(m && m.station);
  if (m && m.state === "banked") {
    const bank = m.bank || {};
    const where = stationName(bank.station || m.station);
    if (bank.source === "auto_down") {
      return { mode: "release", enabled: false, tooltip: `Released automatically when ${where} recovers` };
    }
    if (bank.source === "auto_wait") {
      const n = cfg && cfg.bank_wait_threshold != null ? cfg.bank_wait_threshold : DASH;
      return { mode: "release", enabled: false, tooltip: `Released automatically when ${where} WAIT drops below ${n}` };
    }
    return { mode: "release", enabled: true, tooltip: "" };
  }
  const off = (tooltip) => ({ mode: "bank", enabled: false, tooltip });
  if (!m) return off("");
  if (m.state === "failed") return off("Scrap lots cannot be banked");
  if (m.state === "done") return off("Lot is finished");
  if (m.state === "pre_release") return off("Release the lot first");
  if (m.hold) return off(HOLD_PENDING_TIP);
  if (!reason) return off("Choose a reason first");
  return { mode: "bank", enabled: true, tooltip: "" };
}

// Manual scrap is legal for any released, unfinished lot. Dispatched / running lots get a
// deferred scrap (applied when the current op completes); cooling lots scrap immediately.
// Mirrors the scheduler: pre_release / finished / already-scrapped lots cannot be scrapped.
export function scrapControls(m) {
  const off = (tooltip) => ({ enabled: false, tooltip });
  if (!m) return off("");
  if (m.state === "failed" || m.failed) return off("Lot is already scrapped");
  if (m.state === "done") return off("Lot is finished");
  if (m.state === "pre_release") return off("Release the lot first");
  if (m.hold) return off(HOLD_PENDING_TIP);
  return { enabled: true, tooltip: "" };
}

// Items of the Actions tab: Bank (or Release for a banked lot), Scrap and, with a hold, Cancel hold.
// A bank reason is chosen after the item is picked, so eligibility is checked with a
// placeholder reason here.
export function actionMenuItems(m, cfg) {
  const bank = bankControls(m, "pending-choice", cfg);
  const first = bank.mode === "release"
    ? { id: "release", label: "Release", enabled: bank.enabled, tooltip: bank.tooltip }
    : { id: "bank", label: "Bank", enabled: bank.enabled, tooltip: bank.tooltip };
  const scrap = scrapControls(m);
  const items = [first, { id: "scrap", label: "Scrap", enabled: scrap.enabled, tooltip: scrap.tooltip }];
  if (m && m.hold) items.push({ id: "cancel_hold", label: "Cancel hold", enabled: true, tooltip: "" });
  return items;
}

// Identity of the dock's visible state. The dock DOM is rebuilt only when this changes,
// so unrelated lot fields (countdowns, eta text) never touch it.
export function dockStateKey(lotId, items, ui) {
  const u = ui || {};
  return JSON.stringify([
    lotId,
    (items || []).map((it) => [it.id, it.label, Boolean(it.enabled), it.tooltip || ""]),
    [u.view || "closed", u.reasonChoice || "", u.scrapReason || "", Boolean(u.scrapArmed), Boolean(u.pending)],
  ]);
}

// A lot is scrap when the order lifecycle failed. The data layer keeps
// state === "failed"; the dashboard only changes how it is presented.
export function isScrapLot(m) {
  return Boolean(m) && (m.state === "failed" || Boolean(m.failed));
}

// Numbered machine / stocker names. Machine ids use the lowercase station slug ("bake-1",
// "toppings-1") and the stocker uses the same format ("stocker-1"). The index defaults to 1
// because each station and the stocker are single units today; pass the unit's
// number once the line has more than one.
export function machineLabel(station, no = 1) {
  return `${station}-${no}`;
}

export function stockerLabel(no = 1) {
  return `stocker-${no}`;
}

export function locationLabel(m) {
  if (isScrapLot(m)) return "Scrap bin";
  if (m && m.state === "done") return DASH;
  if (m && (m.state === "banked" || m.stocker)) return stockerLabel();
  if (m && m.cooling) return "Waiting for rework (cooling)";
  if (!m || !m.station) return DASH;
  return machineLabel(m.station);
}

export function reworkLabel(m, cfg) {
  if (!m || !(m.visit > 1)) return null;
  const limits = (cfg && cfg.rework_limits) || {};
  const max = limits[m.station];
  return `Rework ${m.visit - 1}/${max != null ? max : DASH}`;
}

const CAUSE_LABEL = { breakdown: "machine breakdown", quality: "quality" };

function failureNote(f, cfg) {
  const cause = f && Object.prototype.hasOwnProperty.call(CAUSE_LABEL, f.cause) ? f.cause : "quality";
  const visit = f && typeof f.visit === "number" ? f.visit : 1;
  const max = ((cfg && cfg.rework_limits) || {})[f && f.station];
  // Same rule as scheduler/rework.py decide(): rework while visit <= limit, else scrap (DLQ).
  const outcome = max == null ? "" : visit <= max ? ` - rework ${visit}/${max}` : ` - scrapped (rework limit ${max})`;
  return `${stationName(f && f.station)} failed (${CAUSE_LABEL[cause]})${outcome}`;
}

function bankNote(b, m, cfg) {
  const where = stationName(b.station || m.station);
  const reason = REASONS[b.reason] || b.reason || DASH;
  if (b.source === "auto_down") {
    const n = cfg && cfg.bank_after_s != null ? cfg.bank_after_s : DASH;
    return `Auto Bank due to ${reason} - ${where} down over ${n} s`;
  }
  if (b.source === "auto_wait") {
    const n = cfg && cfg.bank_wait_threshold != null ? cfg.bank_wait_threshold : DASH;
    return `Auto Bank due to ${reason} - ${where} WAIT over ${n}`;
  }
  return `Manual Bank due to ${reason}`;
}

function holdNote(h) {
  const scrap = h.kind === "scrap";
  const reason = (scrap ? SCRAP_REASONS : REASONS)[h.reason];
  return `${scrap ? "Scrap" : "Bank"} requested after ${stationName(h.station)}${reason ? ` due to ${reason}` : ""}`;
}

// Note lines for the bottom of the lot inspector, oldest first: "HH:mm:ss <what happened>"
// for failures/rework, the current bank and a pending deferred bank/scrap request.
export function lotNotes(m, cfg) {
  if (!m) return [];
  const notes = [];
  for (const f of Array.isArray(m.failures) ? m.failures : []) notes.push([f && f.ts, failureNote(f, cfg)]);
  if (m.state === "banked" && m.bank) notes.push([m.bank.since_ts, bankNote(m.bank, m, cfg)]);
  if (m.hold) notes.push([m.hold.since_ts, holdNote(m.hold)]);
  const at = (ts) => {
    const ms = typeof ts === "string" ? Date.parse(ts) : NaN;
    return Number.isFinite(ms) ? ms : Infinity;
  };
  return notes
    .map(([ts, text], i) => ({ ts, text, i }))
    .sort((a, b) => at(a.ts) - at(b.ts) || a.i - b.i)
    .map(({ ts, text }) => `${clockLabel(ts)} ${text}`);
}

// Pure Due/Priority rows for the lot inspector (SCHED-03, UI-SPEC section 1).
// `lot` here only needs a `due_ts` field (ISO string); callers pass either the
// raw line lot or `{ due_ts: m.due }` from the inspector model.
export function dueRow(lot, nowMs) {
  if (!lot || !lot.due_ts) return null;
  const dueMs = Date.parse(lot.due_ts);
  if (!Number.isFinite(dueMs)) return null;
  const now = typeof nowMs === "number" ? nowMs : Date.now();
  const diffS = (dueMs - now) / 1000;
  if (diffS >= 0) return { label: "Due", text: mmss(diffS) };
  return { label: "Due", text: `Late by ${mmss(-diffS)}`, chip: "warn" };
}

// Whether the priority editor may Apply, plus the tooltip explaining why not (D-16).
export function priorityEdit(m) {
  if (!m) return { enabled: false, tooltip: "" };
  if (m.state === "pre_release") return { enabled: false, tooltip: "Release the lot first" };
  if (m.state === "done" || m.state === "failed" || m.failed) {
    return { enabled: false, tooltip: "Lot is finished" };
  }
  return { enabled: true, tooltip: "" };
}

function pad2(n) {
  return String(n).padStart(2, "0");
}

// Local-time HH:mm:ss for an ISO timestamp; DASH for anything unparseable.
export function clockLabel(iso) {
  if (typeof iso !== "string") return DASH;
  const ms = Date.parse(iso);
  if (!Number.isFinite(ms)) return DASH;
  const d = new Date(ms);
  return `${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}`;
}

function isoMs(iso) {
  if (typeof iso !== "string") return NaN;
  return Date.parse(iso);
}

// Pure ETA rows for the lot inspector (D-10/D-11, UI-SPEC section B).
// Entries: { label, text, chip?, badge?: {text, tone}, suffix? }.
export function etaRows(model, nowMs) {
  if (!model || model.kind !== "lot") return [];
  const now = typeof nowMs === "number" ? nowMs : Date.now();
  const rows = [];
  const planned = { label: "Initial ETA", text: clockLabel(model.eta) };
  const failed = model.state === "failed" || Boolean(model.failed);
  const done = model.state === "done" || Boolean(model.completedTs);
  if (failed && !done) return [planned];
  if (done) {
    rows.push(planned);
    const e = model.etaError;
    if (typeof e !== "number" || !Number.isFinite(e) || !model.eta) {
      rows.push({ label: "ETA error", text: DASH });
    } else if (Math.abs(e) < 0.5) {
      rows.push({ label: "ETA error", text: "On estimate", chip: "busy" });
    } else if (e > 0) {
      rows.push({ label: "ETA error", text: `+${mmss(e)} later`, chip: "warn" });
    } else {
      rows.push({ label: "ETA error", text: `-${mmss(-e)} earlier`, chip: "busy" });
    }
    return rows;
  }
  if (model.bank) {
    rows.push({ label: "Current ETA", text: DASH }, { label: "Finishes in", text: DASH });
    return rows;
  }
  const etaMs = isoMs(model.eta);
  const nowEtaMs = isoMs(model.etaNow);
  const cur = { label: "Current ETA", text: clockLabel(model.etaNow) };
  if (Number.isFinite(nowEtaMs) && Number.isFinite(etaMs)) {
    const drift = (nowEtaMs - etaMs) / 1000;
    if (Math.abs(drift) >= 1) cur.suffix = `(${drift > 0 ? "+" : "-"}${mmss(Math.abs(drift))} vs initial)`;
  }
  rows.push(cur);
  const targetMs = Number.isFinite(nowEtaMs) ? nowEtaMs : etaMs;
  if (!Number.isFinite(targetMs)) {
    rows.push({ label: "Finishes in", text: DASH });
  } else {
    const remS = (targetMs - now) / 1000;
    if (remS >= 0) rows.push({ label: "Finishes in", text: mmss(remS) });
    else {
      rows.push({
        label: "Finishes in", text: `Late by ${mmss(-remS)}`, chip: "warn", badge: { text: "Behind ETA", tone: "warn" },
      });
    }
  }
  return rows;
}

// On/after completion: did the lot meet its Due timestamp?
export function dueOutcome(model) {
  if (!model) return null;
  const c = isoMs(model.completedTs);
  const d = isoMs(model.due);
  if (!Number.isFinite(c) || !Number.isFinite(d)) return null;
  return c <= d ? { text: "On time", chip: "busy" } : { text: "Late", chip: "warn" };
}

// Due row for the inspector: scrap lots are finished, so their release-time
// due_ts must not turn into a countdown, "Late by ..." or an outcome chip.
export function lotDue(model, nowMs) {
  if (isScrapLot(model)) return { text: DASH, chip: null, outcome: null };
  const dr = dueRow({ due_ts: model && model.due }, nowMs);
  return {
    text: dr ? dr.text : DASH,
    chip: dr && dr.chip ? dr.chip : null,
    outcome: dueOutcome(model),
  };
}

function capitalize(text) {
  const t = typeof text === "string" ? text : "";
  return t ? t.charAt(0).toUpperCase() + t.slice(1) : DASH;
}

// Fixed 9-row Lot Info panel (sketch 005, rounds 4-7). Entries: { label, text, chip?: {text, tone} }.
// `displayProduct` may be a display-name string or a function(productId) -> string.
export function lotInfoRows(model, cfg, nowMs, displayProduct) {
  if (!model || model.kind !== "lot") return [];
  const now = typeof nowMs === "number" ? nowMs : Date.now();
  const scrap = isScrapLot(model);
  const done = model.state === "done";
  let product;
  if (typeof displayProduct === "function") product = displayProduct(model.product);
  else if (typeof displayProduct === "string" && displayProduct) product = displayProduct;
  else product = capitalize(model.product);

  let step;
  let stepChip = null;
  if (done) step = "SHIPPED";
  else if (scrap) step = model.failed && model.failed.station ? stationName(model.failed.station) : stationName(model.station);
  else {
    step = model.station ? stationName(model.station) : DASH;
    const rework = reworkLabel(model, cfg);
    if (rework) stepChip = { text: rework, tone: "amber" };
  }

  const stateLabel = {
    queued: "WAIT", pre_release: "WAIT", running: "RUN", done: "DONE", failed: "SCRAP", banked: "BANK",
  }[model.state] || DASH;
  const holdText = holdChip(model);

  let currentEta;
  if (done) currentEta = clockLabel(model.completedTs);
  else if (scrap || model.bank || model.state === "banked") currentEta = DASH;
  else currentEta = clockLabel(model.etaNow);

  const due = lotDue(model, now);
  let dueChip = null;
  if (due.outcome) dueChip = { text: due.outcome.text, tone: due.outcome.chip };
  else if (due.text !== DASH) dueChip = { text: due.chip ? due.text : `in ${due.text}`, tone: due.chip || "busy" };

  return [
    { label: "Product ID", text: product },
    { label: "Qty", text: model.qty == null ? DASH : String(model.qty) },
    { label: "Priority", text: String(model.priority) },
    { label: "Step", text: step, chip: stepChip },
    { label: "Status", text: stateLabel, chip: holdText ? { text: holdText, tone: "amber" } : null },
    { label: "Location", text: locationLabel(model) },
    { label: "Initial ETA", text: clockLabel(model.eta) },
    { label: "Current ETA", text: currentEta },
    { label: "Due", text: scrap ? DASH : clockLabel(model.due), chip: dueChip },
  ];
}

function isoOrNull(v) {
  return typeof v === "string" && Number.isFinite(Date.parse(v)) ? v : null;
}

function recipeSeconds(recipesByStation, station, product) {
  const table = recipesByStation && recipesByStation[station];
  const v = table && typeof table === "object" ? table[product] : undefined;
  return typeof v === "number" && Number.isFinite(v) ? `${v}s` : DASH;
}

// Process Detail table model. One row per station visit (rework = one row per visit), plus
// the queued rework visit as the current row, future rows (~ estimates) and skipped rows after
// a scrap. Rows: { kind, mark, station, visit, step, machine, recipe, inn, out, notes }.
// `live` (optional) is { station: { in, out } } from GET /api/lot/{id}/eta; it overrides the
// frozen stationEta on future/running rows and the frozen value is kept as `row.plan`.
export function processDetailRows(model, recipesByStation, cfg, live) {
  if (!model || model.kind !== "lot") return { rows: [] };
  const scrap = isScrapLot(model);
  const done = model.state === "done";
  const product = model.product;
  const stationEta = model.stationEta && typeof model.stationEta === "object" ? model.stationEta : {};
  const liveFor = (station) => {
    if (done || !live || typeof live !== "object") return null;
    const e = live[station];
    const out = e && isoOrNull(e.out);
    return out ? { inn: isoOrNull(e.in), out } : null;
  };
  const setPlan = (row, frozen, liveOut) => {
    if (frozen && clockLabel(frozen) !== clockLabel(liveOut)) row.plan = `Plan ${clockLabel(frozen)}`;
  };
  const raw = [];
  for (const v of Array.isArray(model.visits) ? model.visits : []) {
    if (!v || !ROUTE.includes(v.station)) continue;
    const visit = typeof v.visit === "number" ? v.visit : 1;
    let kind = "current";
    if (v.completed_ts) kind = "done";
    else if (v.failed_ts) kind = "failed";
    raw.push({ station: v.station, visit, kind, started: v.started_ts, completed: v.completed_ts, failed: v.failed_ts });
  }
  const hasRow = (station, visit) => raw.some((r) => r.station === station && r.visit === visit);
  if (!done && !scrap && model.station && ROUTE.includes(model.station) && !hasRow(model.station, model.visit)) {
    // A station already completed (banked after Package) has no pending visit to append.
    if (!raw.some((r) => r.station === model.station && r.kind === "done")) {
      raw.push({ station: model.station, visit: model.visit, kind: "current", started: null, completed: null, failed: null });
    }
  }
  // A scrapped lot has no current row; a stale in-flight record is shown as failed.
  if (scrap) for (const r of raw) if (r.kind === "current") r.kind = "failed";
  const have = new Set(raw.map((r) => r.station));
  for (const station of ROUTE) {
    if (have.has(station) || done) continue;
    raw.push({ station, visit: 1, kind: scrap ? "skipped" : "future", started: null, completed: null, failed: null });
  }
  raw.sort((a, b) => ROUTE.indexOf(a.station) - ROUTE.indexOf(b.station) || a.visit - b.visit);
  const visitCount = {};
  for (const r of raw) visitCount[r.station] = (visitCount[r.station] || 0) + 1;

  const rows = raw.map((r, i) => {
    const row = {
      kind: r.kind,
      mark: { done: "✓", failed: "✗", current: "▶", future: "○", skipped: "–" }[r.kind],
      station: r.station,
      visit: r.visit,
      step: visitCount[r.station] > 1 ? `${stationName(r.station)} #${r.visit}` : stationName(r.station),
      machine: DASH,
      recipe: DASH,
      inn: DASH,
      out: DASH,
      plan: null,
      notes: [],
    };
    if (r.kind === "skipped") return row;
    row.machine = machineLabel(r.station);
    row.recipe = recipeSeconds(recipesByStation, r.station, product);
    if (r.kind === "future") {
      const prev = i > 0 ? raw[i - 1] : null;
      const prevEta = prev ? isoOrNull(stationEta[prev.station]) : null;
      const eta = isoOrNull(stationEta[r.station]);
      row.inn = prevEta ? `~${clockLabel(prevEta)}` : DASH;
      row.out = eta ? `~${clockLabel(eta)}` : DASH;
      const lv = liveFor(r.station);
      if (lv) {
        if (lv.inn) row.inn = `~${clockLabel(lv.inn)}`;
        row.out = `~${clockLabel(lv.out)}`;
        setPlan(row, eta, lv.out);
      }
      return row;
    }
    row.inn = r.started ? clockLabel(r.started) : DASH;
    if (r.kind === "done") row.out = clockLabel(r.completed);
    else if (r.kind === "failed") row.out = r.failed ? clockLabel(r.failed) : DASH;
    else if (r.kind === "current" && r.started) {
      const lv = liveFor(r.station);
      if (lv) {
        row.out = `~${clockLabel(lv.out)}`;
        setPlan(row, isoOrNull(stationEta[r.station]), lv.out);
      }
    }
    return row;
  });

  const real = (r) => r.kind !== "future" && r.kind !== "skipped";
  const latestRowOf = (station) => {
    let found = null;
    for (const row of rows) if (row.station === station && real(row)) found = row;
    return found;
  };
  const fallback = () => rows.find((r) => r.kind === "current") || rows.find(real) || null;
  const addNote = (row, ts, text, tone) => {
    const target = row || fallback();
    if (target) target.notes.push({ ts: ts || null, text, tone });
  };
  for (const f of Array.isArray(model.failures) ? model.failures : []) {
    if (!f) continue;
    const visit = typeof f.visit === "number" ? f.visit : 1;
    const row = rows.find((r) => r.station === f.station && r.visit === visit && real(r));
    addNote(row || latestRowOf(f.station), f.ts, failureNote(f, cfg), "danger");
  }
  if (model.hold) addNote(latestRowOf(model.hold.station), model.hold.since_ts, holdNote(model.hold), "amber");
  if (model.state === "banked" && model.bank) {
    addNote(latestRowOf(model.bank.station || model.station), model.bank.since_ts, bankNote(model.bank, model, cfg), "amber");
  }
  // Priority changes nest under the visit the lot was at (last visit started before the change).
  const visitsIn = Array.isArray(model.visits) ? model.visits : [];
  for (const c of Array.isArray(model.priorityChanges) ? model.priorityChanges : []) {
    if (!c) continue;
    const at = Date.parse(c.ts);
    let row = null;
    if (Number.isFinite(at)) {
      for (const v of visitsIn) {
        const sMs = Date.parse(v && v.started_ts);
        if (Number.isFinite(sMs) && sMs <= at) {
          row = rows.find((r) => r.station === v.station && r.visit === v.visit && real(r)) || row;
        }
      }
    }
    addNote(row, c.ts, `Priority ${c.from} → ${c.to}`, "amber");
  }
  const tms = (ts) => {
    const ms = typeof ts === "string" ? Date.parse(ts) : NaN;
    return Number.isFinite(ms) ? ms : Infinity;
  };
  for (const row of rows) row.notes.sort((a, b) => tms(a.ts) - tms(b.ts));
  return { rows };
}

// Total nested notes; the tab bar compares it to the last seen count for the orange dot.
export function noteCount(model, cfg) {
  return processDetailRows(model, null, cfg).rows.reduce((n, r) => n + r.notes.length, 0);
}

function mk(doc, tag, cls, text) {
  const n = doc.createElement(tag);
  if (cls) n.className = cls;
  if (text != null) n.textContent = text;
  return n;
}

function clear(host) {
  while (host.firstChild) host.removeChild(host.firstChild);
}

function row(doc, label, valueNode) {
  const r = mk(doc, "div", "insp-row");
  r.appendChild(mk(doc, "span", "insp-label", label));
  r.appendChild(valueNode);
  return r;
}

function chip(doc, text, tone) {
  return mk(doc, "span", `chip ${tone}`, text);
}

export function init(ctx) {
  const { store, el, document: doc } = ctx;
  const card = el("inspector");
  const body = el("inspector-body");
  const closeBtn = el("inspector-close");
  if (!card || !body) return;
  let closed = false;
  let wasSelected = false;

  // --- control plumbing: UI state changes only from SSE facts, never from the click ---
  const ACK_MS = 3000;
  let pending = null; // { key, kind, action, label, timer, reason }
  let reasonChoice = "";
  let actionView = "closed"; // closed | bank | scrap
  let actionLotId = null;
  let scrapReason = "";
  let scrapArmed = false; // second (confirm) step of the irreversible scrap
  let priorityDraft = null; // { id, value } local slider value until Apply
  let prioritySending = false;
  let repairChoice = "";
  let failureConfigRequested = false;
  const recipeGate = { inflight: false, failures: 0, lastAttemptAt: null };
  let recipeDraft = null; // { station, values: { product: typed string | number } } until Apply
  let recipeSending = false;
  let lastStationKey = null;

  function clearPending() {
    if (pending && pending.timer) clearTimeout(pending.timer);
    pending = null;
  }

  function startPending(key, kind, action, label, extra) {
    clearPending();
    const p = { key, kind, action, label, timer: null, ...extra };
    p.timer = setTimeout(() => {
      if (pending !== p) return;
      pending = null;
      const text = kind === "station"
        ? `No response from ${label}. It may be offline - check the Stations tab.`
        : `No response from the scheduler for lot ${label}. It may already be at the station - check its state.`;
      showToast({ kind: "error", text }, doc);
      render();
    }, ACK_MS);
    pending = p;
    render();
  }

  async function sendControl(key, kind, action, label, call, extra) {
    startPending(key, kind, action, label, extra);
    const mine = pending;
    try {
      await call();
    } catch (err) {
      if (pending === mine) clearPending();
      const text = kind === "station"
        ? `Could not send ${action} to ${label}. Check that RabbitMQ is running and try again.`
        : `Could not send ${action} for lot ${label}. Check that RabbitMQ is running and try again.`;
      showToast({ kind: "error", text }, doc);
      render();
    }
  }

  store.onFact((fact) => {
    if (!pending || !fact) return;
    if (pending.kind === "station" && fact.kind === "station_status" && fact.station === pending.key) {
      const label = pending.label;
      clearPending();
      showToast({ text: fact.status === "down" ? `${label} is down.` : `${label} is back up.` }, doc);
      render();
    } else if (pending.kind === "lot" && fact.order_id === pending.key) {
      if (fact.kind === "lot_banked" && pending.action === "Bank") {
        const reason = fact.reason || pending.reason;
        clearPending();
        showToast({ text: `Lot ${fact.order_id} banked (${reason}).` }, doc);
        render();
      } else if (fact.kind === "order_lifecycle" && fact.state === "failed" && pending.action === "Scrap") {
        const reason = pending.reason;
        clearPending();
        showToast({ text: `Lot ${fact.order_id} scrapped (${reason}).` }, doc);
        render();
      } else if (fact.kind === "lot_hold" && fact.state === "requested" && (pending.action === "Bank" || pending.action === "Scrap")) {
        const verb = pending.action === "Scrap" ? "scrap" : "bank";
        clearPending();
        showToast({ text: `Lot ${fact.order_id} will ${verb} after ${stationName(fact.station)}.` }, doc);
        render();
      } else if (fact.kind === "lot_hold" && fact.state === "cancelled" && pending.action === "Cancel hold") {
        clearPending();
        showToast({ text: `Hold on lot ${fact.order_id} cancelled.` }, doc);
        render();
      } else if (fact.kind === "lot_released" && pending.action === "Release") {
        clearPending();
        showToast({ text: `Lot ${fact.order_id} released to the ${stationName(fact.station)} queue.` }, doc);
        render();
      }
    }
  });

  function applyButton(btn, ctl, pendingText) {
    btn.disabled = !ctl.enabled;
    btn.setAttribute("aria-disabled", String(!ctl.enabled));
    // Disabled controls show no tooltip or reason text (sketch 005 round 3).
    if (pendingText) btn.textContent = pendingText;
  }

  function renderStation(m) {
    const state = store.getState();
    const now = Date.now();
    const cfg = state.failureConfig;
    // Tab memory is per selection: a new station always opens on Station Info.
    if (m.name !== tabStationId) {
      stationTab = "info";
      tabStationId = m.name;
    }
    const head = mk(doc, "div", "insp-head");
    head.appendChild(mk(doc, "h3", "insp-title", stationName(m.name)));
    const focus = mk(doc, "button", "btn primary", "Focus");
    focus.type = "button";
    focus.addEventListener("click", () => {
      if (ctx.scene3d) ctx.scene3d.focusStation(m.name);
    });
    head.appendChild(focus);
    body.appendChild(head);
    const draft = recipeDraft && recipeDraft.station === m.name ? recipeDraft.values : null;
    renderTabs(STATION_TABS, stationTab, setStationTab, (id) => id === "recipe" && recipeTabDot(m.recipes, draft, stationTab), "Unapplied recipe changes");
    if (stationTab === "recipe") {
      if (m.recipes) renderRecipes(m);
      else if (recipeGate.failures > 0) {
        body.appendChild(mk(doc, "p", "insp-note", "Recipes unavailable - retrying in the background."));
      }
    } else if (stationTab === "actions") renderStationActions(m);
    else renderStationInfo(m, now, cfg);
  }

  function renderStationInfo(m, now, cfg) {
    let label = m.status;
    let tone = "idle";
    if (m.status === "busy") tone = "ok";
    else if (m.status === "down") {
      tone = "danger";
      label = downLabel(m, now);
    } else if (m.status === "offline") {
      tone = "amber";
      label = "Offline";
    }
    body.appendChild(row(doc, "Status", chip(doc, label, tone)));
    body.appendChild(row(doc, "Queue", chip(doc, String(m.queue), m.queue >= QUEUE_AMBER_AT ? "amber" : "idle")));
    const util = m.utilization == null ? DASH : `${Math.round(m.utilization * 100)}%`;
    body.appendChild(row(doc, "Utilization", mk(doc, "span", "insp-value", util)));
    const avail = m.availability == null ? DASH : `${Math.round(m.availability * 100)}%`;
    body.appendChild(row(doc, "Availability", mk(doc, "span", "insp-value", avail)));
    body.appendChild(row(doc, "Down time", mk(doc, "span", "insp-value", m.downS == null ? DASH : mmss(m.downS))));
    if (m.status === "down") {
      const cause = m.cause === "random" ? "Random (MTBF)" : m.cause === "manual" ? "Manual" : DASH;
      body.appendChild(row(doc, "Cause", mk(doc, "span", "insp-value", cause)));
    }
    body.appendChild(row(doc, "Random breakdowns", mk(doc, "span", "insp-value", m.randomBreakdowns.replace("Random breakdowns: ", ""))));
    if (m.status === "offline") {
      const hb = cfg && typeof cfg.heartbeat_interval_s === "number" ? cfg.heartbeat_interval_s * 3 : DASH;
      body.appendChild(mk(doc, "p", "insp-note", `No heartbeat for ${hb} s. Waiting for the station to return.`));
    }
  }

  // Actions tab: repair-time select + one Break/Repair toggle.
  function renderStationActions(m) {
    const isPending = Boolean(pending && pending.kind === "station" && pending.key === m.name);
    body.appendChild(mk(doc, "div", "insp-section-title", "Breakdown"));
    const actions = mk(doc, "div", "insp-actions");
    const select = mk(doc, "select", "repair-select");
    select.setAttribute("aria-label", "Repair time");
    for (const [value, text] of REPAIR_OPTIONS) {
      const opt = mk(doc, "option", null, text);
      opt.value = value;
      select.appendChild(opt);
    }
    select.value = repairChoice;
    select.disabled = isPending;
    select.addEventListener("change", () => { repairChoice = select.value; });
    actions.appendChild(select);
    // One toggle: Repair while the station is down, Break while it runs.
    const ctl = breakButtons({ status: m.status, pending: isPending });
    const down = m.status === "down";
    const toggle = mk(doc, "button", down ? "btn" : "btn destructive", down ? "Repair Station" : "Break Station");
    toggle.type = "button";
    applyButton(toggle, down ? ctl.repair : ctl.break);
    toggle.addEventListener("click", () => {
      if (toggle.disabled) return;
      if (down) {
        sendControl(m.name, "station", "Repair", stationName(m.name), () => repairStation(m.name));
        return;
      }
      const secs = repairChoice === "" ? undefined : Number(repairChoice);
      sendControl(m.name, "station", "Break", stationName(m.name), () => breakStation(m.name, secs));
    });
    actions.appendChild(toggle);
    body.appendChild(actions);
  }

  // Recipe table (sketch 006 A2): product | − | [input] s | +. Edits stay in recipeDraft until
  // Apply (or Enter); Reset drops them. −/+ step exactly 1 s, no repeat or modifier keys.
  function renderRecipes(m) {
    if (!recipeDraft || recipeDraft.station !== m.name) recipeDraft = { station: m.name, values: {} };
    const draft = recipeDraft.values;
    body.appendChild(mk(doc, "div", "insp-section-title", "Recipes"));
    const grid = mk(doc, "div", "recipe-grid");
    const lines = {};
    const foot = mk(doc, "div", "recipe-foot");
    const count = mk(doc, "span", "recipe-count");
    const reset = mk(doc, "button", "btn", "Reset");
    const apply = mk(doc, "button", "btn primary", recipeSending ? "Applying..." : "Apply");
    reset.type = "button";
    apply.type = "button";
    const refresh = () => {
      const view = recipeDraftView(m.recipes, draft);
      for (const r of view.rows) {
        const line = lines[r.product];
        line.classList.toggle("dirty", r.dirty);
        line.querySelector(".recipe-input").classList.toggle("bad", !r.valid);
        line.querySelector(".recipe-was").textContent = r.dirty ? `was ${r.live}s` : "";
      }
      count.className = view.invalid ? "recipe-count bad" : "recipe-count";
      const n = view.changed.length;
      count.textContent = view.invalid
        ? `Use ${RECIPE_MIN_S}-${RECIPE_MAX_S} s`
        : n ? `${n} unapplied change${n > 1 ? "s" : ""}` : "No changes";
      apply.disabled = recipeSending || !view.canApply;
      reset.disabled = recipeSending || !view.canReset;
    };
    for (const product of Object.keys(m.recipes)) {
      const line = mk(doc, "div", "rtab");
      lines[product] = line;
      line.title = "Seconds per pizza for new jobs at this station";
      const label = mk(doc, "span", "recipe-label");
      label.appendChild(mk(doc, "span", "recipe-name", product));
      label.appendChild(mk(doc, "span", "recipe-was"));
      line.appendChild(label);
      const input = mk(doc, "input", "recipe-input");
      input.type = "number";
      input.min = String(RECIPE_MIN_S);
      input.max = String(RECIPE_MAX_S);
      input.step = "0.5";
      input.value = String(product in draft ? draft[product] : m.recipes[product]);
      input.disabled = recipeSending;
      input.setAttribute("aria-label", `${product} seconds at ${stationName(m.name)}`);
      input.addEventListener("input", () => {
        draft[product] = input.value;
        refresh();
      });
      input.addEventListener("keydown", (e) => {
        if (e.key === "Enter") {
          e.preventDefault();
          applyRecipes(m);
        } else if (e.key === "Escape") {
          // Revert this row only; keep the inspector open.
          e.stopPropagation();
          delete draft[product];
          input.value = String(m.recipes[product]);
          refresh();
        }
      });
      const down = mk(doc, "button", "btn", "−");
      const up = mk(doc, "button", "btn", "+");
      for (const [b, delta, verb] of [[down, -1, "Decrease"], [up, 1, "Increase"]]) {
        b.type = "button";
        b.disabled = recipeSending;
        b.setAttribute("aria-label", `${verb} ${product} by 1 second`);
        b.addEventListener("click", () => {
          const row = recipeDraftView(m.recipes, draft).rows.find((r) => r.product === product);
          const next = nextRecipeSeconds(row && row.valid ? row.value : m.recipes[product], delta);
          if (next === null) return;
          draft[product] = next;
          input.value = String(next);
          refresh();
        });
      }
      const cell = mk(doc, "span", "recipe-cell");
      cell.appendChild(input);
      cell.appendChild(mk(doc, "span", "recipe-unit", "s"));
      line.appendChild(down);
      line.appendChild(cell);
      line.appendChild(up);
      grid.appendChild(line);
    }
    reset.addEventListener("click", () => {
      recipeDraft = null;
      render(true);
    });
    apply.addEventListener("click", () => applyRecipes(m));
    foot.appendChild(count);
    foot.appendChild(reset);
    foot.appendChild(apply);
    body.appendChild(grid);
    body.appendChild(foot);
    body.appendChild(mk(doc, "p", "insp-note", "Applies to new jobs at this station; running jobs keep their time."));
    refresh();
  }

  // One setRecipe per changed product, in order; a failure keeps it and the rest as unapplied.
  async function applyRecipes(m) {
    if (recipeSending || !recipeDraft || recipeDraft.station !== m.name) return;
    const view = recipeDraftView(m.recipes, recipeDraft.values);
    if (!view.canApply) return;
    recipeSending = true;
    render(true);
    let applied = 0;
    try {
      for (const r of view.changed) {
        try {
          const out = await setRecipe(m.name, r.product, r.value);
          const current = store.getState();
          const recipes = Object.assign({}, current.recipes || {});
          recipes[m.name] = Object.assign({}, recipes[m.name] || {}, { [r.product]: out.unit_time_s });
          store.setRecipes(recipes);
          if (recipeDraft && recipeDraft.station === m.name) delete recipeDraft.values[r.product];
          applied += 1;
        } catch (err) {
          showToast({ kind: "error", text: `Could not update ${r.product}: ${errorReason(err)}` }, doc);
          break;
        }
      }
    } finally {
      recipeSending = false;
      if (applied > 0) {
        const text = applied === 1
          ? `${stationName(m.name)} - ${view.changed[0].product}: ${view.changed[0].value} s`
          : `${stationName(m.name)}: ${applied} recipes applied`;
        showToast({ text }, doc);
      }
      render(true);
    }
  }

  function loadRecipes() {
    if (recipeGate.inflight) return;
    recipeGate.inflight = true;
    recipeGate.lastAttemptAt = Date.now();
    getRecipes()
      .then((data) => {
        store.setRecipes((data && data.recipes) || {});
        recipeGate.failures = 0;
      })
      .catch(() => {
        recipeGate.failures += 1;
      })
      .finally(() => {
        recipeGate.inflight = false;
        render();
      });
  }

  function ensureRecipes(selectionChanged) {
    if (shouldFetchRecipes(store.getState(), recipeGate, Date.now(), selectionChanged)) loadRecipes();
  }

  async function applyPriority(m, value) {
    const pr = parsePriority(value);
    if (pr === null) {
      showToast({ kind: "error", text: PRIORITY_ERROR }, doc);
      return;
    }
    prioritySending = true;
    render(true);
    try {
      await setLotPriority(m.id, pr);
      priorityDraft = null;
      showToast({ text: `Lot ${m.id} priority set to P${pr}.` }, doc);
    } catch (err) {
      showToast({ kind: "error", text: `Could not change priority: ${errorReason(err)}` }, doc);
    } finally {
      prioritySending = false;
      render(true);
    }
  }

  function renderPriorityEditor(m) {
    const edit = priorityEdit(m);
    const draft = priorityDraft && priorityDraft.id === m.id ? priorityDraft.value : String(m.priority);
    const wrap = mk(doc, "span", "insp-value priority-edit", "");
    const sliderBox = mk(doc, "span", "priority-slider-box", "");
    const slider = mk(doc, "input", "priority-slider");
    slider.type = "range";
    slider.min = String(PRIORITY_MIN);
    slider.max = String(PRIORITY_MAX);
    slider.step = "1";
    slider.value = draft;
    slider.disabled = !edit.enabled || prioritySending;
    slider.setAttribute("aria-label", "New priority (1 most urgent, 99 least urgent)");
    const readout = mk(doc, "output", "priority-readout", String(Number(draft)));
    slider.addEventListener("input", () => {
      priorityDraft = { id: m.id, value: slider.value };
      readout.textContent = String(Number(slider.value));
    });
    const apply = mk(doc, "button", "btn", prioritySending ? "Applying..." : "Apply");
    apply.type = "button";
    apply.disabled = !edit.enabled || prioritySending;
    apply.setAttribute("aria-disabled", String(apply.disabled));
    apply.addEventListener("click", () => {
      if (apply.disabled) return;
      applyPriority(m, slider.value);
    });
    sliderBox.appendChild(readout);
    sliderBox.appendChild(slider);
    wrap.appendChild(sliderBox);
    wrap.appendChild(apply);
    body.appendChild(row(doc, "Priority", wrap));
  }

  // --- Lot tabs (sketch 005): Lot Info | Process Detail | Actions. The tab is UI state only:
  // selecting or switching lots always reopens Lot Info. ---
  const LOT_TABS = [["info", "Lot Info"], ["detail", "Process Detail"], ["actions", "Actions"]];
  let lotTab = "info";
  let tabLotId = null;
  let seenNoteCount = 0;
  let noteDot = false;

  // --- Station tabs (sketch 007): Station Info | Recipe | Actions; same per-selection rule. ---
  const STATION_TABS = [["info", "Station Info"], ["recipe", "Recipe"], ["actions", "Actions"]];
  let stationTab = "info";
  let tabStationId = null;

  // --- Live per-station ETA for the open lot's Process Detail tab (polled ~1 s, volatile). ---
  const LIVE_ETA_MS = 1000;
  let liveEta = null;
  let liveEtaLotId = null;
  let liveEtaKey = "";
  let liveEtaTimer = null;
  let liveEtaBusy = false;
  let liveEtaAbort = null;

  function stopLiveEta() {
    if (liveEtaTimer) clearInterval(liveEtaTimer);
    liveEtaTimer = null;
    if (liveEtaAbort) liveEtaAbort.abort();
    liveEtaAbort = null;
    liveEtaBusy = false;
    liveEta = null;
    liveEtaLotId = null;
    liveEtaKey = "";
  }

  async function pollLiveEta(id) {
    if (liveEtaBusy) return;
    liveEtaBusy = true;
    const ac = typeof AbortController === "function" ? new AbortController() : null;
    liveEtaAbort = ac;
    let next = null;
    try {
      const res = await getLotEta(id, ac ? { signal: ac.signal } : {});
      if (res && res.available && res.stations && typeof res.stations === "object") next = res.stations;
    } catch (_) {
      next = null;
    }
    if (liveEtaAbort === ac) liveEtaAbort = null;
    liveEtaBusy = false;
    if (liveEtaLotId !== id) return; // stopped or switched while in flight
    const key = next ? JSON.stringify(next) : "";
    if (key === liveEtaKey) return;
    liveEtaKey = key;
    liveEta = next;
    render(true);
  }

  function syncLiveEta(m) {
    if (lotTab !== "detail" || m.state === "done") {
      if (liveEtaLotId !== null) stopLiveEta();
      return;
    }
    if (liveEtaLotId === m.id && liveEtaTimer) return;
    stopLiveEta();
    liveEtaLotId = m.id;
    const id = m.id;
    liveEtaTimer = setInterval(() => pollLiveEta(id), LIVE_ETA_MS);
    pollLiveEta(id);
  }

  // --- On-demand heavy history (visits/failures/priority_changes/station_eta) for the selected lot. ---
  let lotDetail = null;
  let lotDetailId = null;
  let lotDetailKey = "";
  let lotDetailTimer = null;
  let lotDetailBusy = false;
  let lotDetailAbort = null;

  function stopLotDetail() {
    if (lotDetailTimer) clearInterval(lotDetailTimer);
    lotDetailTimer = null;
    if (lotDetailAbort) lotDetailAbort.abort();
    lotDetailAbort = null;
    lotDetailBusy = false;
    lotDetail = null;
    lotDetailId = null;
    lotDetailKey = "";
  }

  async function pollLotDetail(id) {
    if (lotDetailBusy) return;
    lotDetailBusy = true;
    const ac = typeof AbortController === "function" ? new AbortController() : null;
    lotDetailAbort = ac;
    let next = null;
    try {
      const res = await getLotDetail(id, ac ? { signal: ac.signal } : {});
      if (res && typeof res === "object" && res.order_id === id) next = res;
    } catch (_) {
      next = null; // keep the previous detail
    }
    if (lotDetailAbort === ac) lotDetailAbort = null;
    lotDetailBusy = false;
    if (lotDetailId !== id || !next) return; // switched/stopped in flight, or fetch failed
    const key = JSON.stringify(next);
    if (key === lotDetailKey) return;
    lotDetailKey = key;
    lotDetail = next;
    render(true);
  }

  function syncLotDetail(m) {
    if (!m || m.kind !== "lot") {
      if (lotDetailId !== null) stopLotDetail();
      return;
    }
    if (lotDetailId === m.id) return; // already polling (or done lot already fetched)
    stopLotDetail();
    lotDetailId = m.id;
    const id = m.id;
    if (m.state !== "done") lotDetailTimer = setInterval(() => pollLotDetail(id), LIVE_ETA_MS);
    pollLotDetail(id);
  }

  function setLotTab(tab) {
    if (tab === lotTab) return;
    lotTab = tab;
    resetAction();
    render(true);
  }

  function setStationTab(tab) {
    if (tab === stationTab) return;
    stationTab = tab;
    render(true);
  }

  // Shared tab bar for the lot and station inspectors; `hasDot(id)` adds the orange dot.
  function renderTabs(tabs, active, onPick, hasDot, dotLabel) {
    const bar = mk(doc, "div", "insp-tabs");
    bar.setAttribute("role", "tablist");
    for (const [id, text] of tabs) {
      const btn = mk(doc, "button", id === active ? "insp-tab active" : "insp-tab", text);
      btn.type = "button";
      btn.setAttribute("role", "tab");
      btn.setAttribute("aria-selected", String(id === active));
      if (hasDot(id)) {
        const dot = mk(doc, "span", "tab-dot");
        dot.setAttribute("aria-label", dotLabel);
        btn.appendChild(dot);
      }
      btn.addEventListener("click", () => onPick(id));
      bar.appendChild(btn);
    }
    body.appendChild(bar);
  }

  function renderTabBar() {
    renderTabs(LOT_TABS, lotTab, setLotTab, (id) => id === "detail" && noteDot, "New events");
  }

  function renderLotInfo(m, cfg) {
    for (const r of lotInfoRows(m, cfg, Date.now())) {
      const value = mk(doc, "span", "insp-value", r.text);
      if (r.chip) value.appendChild(chip(doc, r.chip.text, r.chip.tone));
      body.appendChild(row(doc, r.label, value));
    }
  }

  function renderProcessDetail(m, cfg) {
    const state = store.getState();
    const { rows } = processDetailRows(m, state.recipes || {}, cfg, liveEtaLotId === m.id ? liveEta : null);
    const table = mk(doc, "table", "insp-detail");
    const thead = mk(doc, "thead");
    const hr = mk(doc, "tr");
    for (const h of ["Step", "Machine", "Recipe", "In", "Out"]) hr.appendChild(mk(doc, "th", null, h));
    thead.appendChild(hr);
    table.appendChild(thead);
    const tbody = mk(doc, "tbody");
    for (const r of rows) {
      const tr = mk(doc, "tr", `d-${r.kind}`);
      tr.appendChild(mk(doc, "td", "d-step", `${r.mark} ${r.step}`));
      tr.appendChild(mk(doc, "td", null, r.machine));
      tr.appendChild(mk(doc, "td", null, r.recipe));
      tr.appendChild(mk(doc, "td", null, r.inn));
      const outCell = mk(doc, "td", null, r.out);
      if (r.plan) {
        outCell.title = r.plan;
        outCell.appendChild(mk(doc, "span", "d-plan", ` ${r.plan}`));
      }
      tr.appendChild(outCell);
      tbody.appendChild(tr);
      if (r.notes.length) {
        const nr = mk(doc, "tr", "d-notes");
        const cell = mk(doc, "td");
        cell.colSpan = 5;
        for (const n of r.notes) {
          cell.appendChild(mk(doc, "div", `dn-${n.tone}`, `↳ ${clockLabel(n.ts)} ${n.text}`));
        }
        nr.appendChild(cell);
        tbody.appendChild(nr);
      }
    }
    table.appendChild(tbody);
    body.appendChild(table);
  }

  function renderLot(m) {
    const state = store.getState();
    const cfg = state.failureConfig;
    // Tab memory is per selection: a new lot always opens on Lot Info with a clean dot.
    if (m.id !== tabLotId) {
      lotTab = "info";
      tabLotId = m.id;
      seenNoteCount = noteCount(m, cfg);
      noteDot = false;
    }
    const notesNow = noteCount(m, cfg);
    if (lotTab === "detail") {
      seenNoteCount = notesNow;
      noteDot = false;
    } else if (notesNow > seenNoteCount) {
      noteDot = true;
    }
    const head = mk(doc, "div", "insp-head");
    head.appendChild(mk(doc, "h3", "insp-title", m.id));
    const follow = mk(doc, "button", "btn primary", "Follow");
    follow.type = "button";
    follow.addEventListener("click", () => {
      if (ctx.scene3d) ctx.scene3d.followLot(m.id);
    });
    head.appendChild(follow);
    body.appendChild(head);
    renderTabBar();
    if (lotTab === "detail") renderProcessDetail(m, cfg);
    else if (lotTab === "actions") {
      renderPriorityEditor(m);
      body.appendChild(mk(doc, "div", "insp-section-title", "Hold / Bank / Scrap"));
      renderActionDock(m, cfg);
    } else renderLotInfo(m, cfg);
    if (lotTab !== "actions") dropDock();
    syncLiveEta(m);
  }

  // --- Action dock: [Bank/Release] [Scrap] [Cancel hold] under the Actions tab's heading.
  // The dock lives in its own persistent host (outside `body`, which is cleared on every
  // render) and is rebuilt only when dockStateKey changes, so ticks never swallow clicks
  // or flicker hover. Reason lists open in flow above the button row. ---
  const dockHost = mk(doc, "div", "insp-dock-host");
  dockHost.classList.add("hidden");
  card.appendChild(dockHost);
  let dockNode = null;
  let dockKey = null;

  function dropDock() {
    if (dockNode && dockNode.parentNode) dockNode.parentNode.removeChild(dockNode);
    dockNode = null;
    dockKey = null;
    dockHost.classList.add("hidden");
  }

  function resetAction() {
    actionView = "closed";
    scrapReason = "";
    scrapArmed = false;
  }

  function reasonList(label, reasons, onPick) {
    const menu = mk(doc, "div", "insp-menu");
    menu.setAttribute("role", "menu");
    menu.setAttribute("aria-label", label);
    for (const [v, text] of Object.entries(reasons)) {
      const btn = mk(doc, "button", "btn insp-menu-item", text);
      btn.type = "button";
      btn.setAttribute("role", "menuitem");
      btn.addEventListener("click", () => onPick(v));
      menu.appendChild(btn);
    }
    return menu;
  }

  function renderActionDock(m, cfg) {
    if (actionLotId !== m.id) {
      actionLotId = m.id;
      resetAction();
    }
    const items = actionMenuItems(m, cfg);
    const byId = Object.fromEntries(items.map((it) => [it.id, it]));
    const isPending = Boolean(pending && pending.kind === "lot" && pending.key === m.id);
    // A sub-panel whose operation became illegal (lot started, got scrapped, ...) closes itself.
    if ((actionView === "bank" || actionView === "scrap") && (isPending || !(byId[actionView] && byId[actionView].enabled))) {
      resetAction();
    }
    const key = dockStateKey(m.id, items, { view: actionView, reasonChoice, scrapReason, scrapArmed, pending: isPending });
    dockHost.classList.remove("hidden");
    if (dockNode && key === dockKey) {
      if (dockNode.parentNode !== dockHost) dockHost.appendChild(dockNode);
      return;
    }

    const dock = mk(doc, "div", "insp-dock");
    const [first, scrap] = items;
    // The reason list / scrap confirm opens below the button row so the buttons never shift.
    const sub = mk(doc, "div", "insp-dock-sub");

    if (!isPending && actionView === "bank") {
      sub.appendChild(reasonList("Bank reason", REASONS, (reason) => {
        resetAction();
        sendControl(m.id, "lot", "Bank", m.id, () => bankLot(m.id, reason), { reason: REASONS[reason] || reason });
      }));
    } else if (!isPending && actionView === "scrap") {
      if (!scrapArmed) {
        sub.appendChild(reasonList("Scrap reason", SCRAP_REASONS, (reason) => {
          scrapReason = reason;
          scrapArmed = true;
          render(true);
        }));
      } else {
        const panel = mk(doc, "div", "insp-menu");
        panel.appendChild(mk(doc, "p", "insp-note insp-warn", `Scrap lot ${m.id} (${SCRAP_REASONS[scrapReason]})? This cannot be undone.`));
        const row2 = mk(doc, "div", "insp-actions");
        const cancel = mk(doc, "button", "btn", "Cancel");
        cancel.type = "button";
        cancel.addEventListener("click", () => { resetAction(); render(true); });
        const confirm = mk(doc, "button", "btn destructive", "Confirm Scrap");
        confirm.type = "button";
        confirm.addEventListener("click", () => {
          const reason = scrapReason;
          if (!reason) return;
          resetAction();
          sendControl(m.id, "lot", "Scrap", m.id, () => scrapLot(m.id, reason), { reason: SCRAP_REASONS[reason] || reason });
        });
        row2.appendChild(cancel);
        row2.appendChild(confirm);
        panel.appendChild(row2);
        sub.appendChild(panel);
      }
    }

    const rowEl = mk(doc, "div", "insp-dock-row");
    if (isPending) {
      const working = mk(doc, "button", "btn primary", "Working...");
      working.type = "button";
      working.disabled = true;
      working.setAttribute("aria-disabled", "true");
      rowEl.appendChild(working);
      dock.appendChild(rowEl);
    } else {
      const firstBtn = mk(doc, "button", "btn", first.label);
      firstBtn.type = "button";
      applyButton(firstBtn, first);
      if (first.id === "bank") {
        firstBtn.setAttribute("aria-haspopup", "menu");
        firstBtn.setAttribute("aria-expanded", String(actionView === "bank"));
      }
      firstBtn.addEventListener("click", () => {
        if (firstBtn.disabled) return;
        if (first.id === "release") {
          resetAction();
          sendControl(m.id, "lot", "Release", m.id, () => releaseLot(m.id));
          return;
        }
        const open = actionView === "bank";
        resetAction();
        if (!open) actionView = "bank";
        render(true);
      });
      const scrapBtn = mk(doc, "button", "btn", "Scrap");
      scrapBtn.type = "button";
      applyButton(scrapBtn, scrap);
      scrapBtn.setAttribute("aria-haspopup", "menu");
      scrapBtn.setAttribute("aria-expanded", String(actionView === "scrap"));
      scrapBtn.addEventListener("click", () => {
        if (scrapBtn.disabled) return;
        const open = actionView === "scrap";
        resetAction();
        if (!open) actionView = "scrap";
        render(true);
      });
      rowEl.appendChild(firstBtn);
      rowEl.appendChild(scrapBtn);
      if (byId.cancel_hold) {
        const cancelBtn = mk(doc, "button", "btn", "Cancel hold");
        cancelBtn.type = "button";
        cancelBtn.addEventListener("click", () => {
          resetAction();
          sendControl(m.id, "lot", "Cancel hold", m.id, () => cancelLotHold(m.id));
        });
        rowEl.appendChild(cancelBtn);
      }
      dock.appendChild(rowEl);
    }
    if (sub.firstChild) dock.appendChild(sub);

    if (dockNode && dockNode.parentNode === dockHost) dockHost.replaceChild(dock, dockNode);
    else dockHost.appendChild(dock);
    dockNode = dock;
    dockKey = key;
  }

  // Esc / outside click close an open reason list. Registered once (not per render).
  // Capture phase: runs before scene.js's bubble-phase Esc (deselect), which is
  // registered earlier, so stopping here keeps the lot selected.
  doc.addEventListener("keydown", (e) => {
    if (e.key !== "Escape" || actionView === "closed") return;
    e.stopImmediatePropagation();
    resetAction();
    render(true);
  }, true);
  doc.addEventListener("mousedown", (e) => {
    if (actionView === "closed") return;
    if (dockNode && dockNode.contains(e.target)) return;
    resetAction();
    render(true);
  });

  // A click only counts when press and release land on the same node, so a rebuild between
  // them swallows it. Hold background renders while a button is down inside the card and
  // catch up after the click has been dispatched.
  let pointerHeld = false;
  let renderDeferred = false;
  card.addEventListener("pointerdown", (e) => {
    if (e.button === 0) pointerHeld = true;
  });
  const releasePointer = () => {
    if (!pointerHeld) return;
    pointerHeld = false;
    if (!renderDeferred) return;
    renderDeferred = false;
    setTimeout(() => render(), 0);
  };
  doc.addEventListener("pointerup", releasePointer, true);
  doc.addEventListener("pointercancel", releasePointer, true);

  function render(force) {
    if (!force && pointerHeld) {
      renderDeferred = true;
      return;
    }
    const state = store.getState();
    const selection = state && state.selection;
    const stationKey = selection && selection.kind === "station" ? selection.id : null;
    const selectionChanged = stationKey !== lastStationKey;
    lastStationKey = stationKey;
    if (priorityDraft && !(selection && selection.kind === "lot" && selection.id === priorityDraft.id)) priorityDraft = null;
    ensureRecipes(selectionChanged);
    const active = doc.activeElement;
    if (!force && active && (active.tagName === "SELECT" || active.tagName === "INPUT") && card.contains(active)) return;
    const selected = Boolean(state.selection);
    if (selected && !wasSelected) closed = false;
    wasSelected = selected;
    let m = inspectorModel(state);
    syncLotDetail(m);
    if (m && m.kind === "lot" && lotDetailId === m.id && lotDetail) m = inspectorModel(state, lotDetail);
    card.classList.toggle("hidden", closed || !m);
    clear(body);
    if (!m || m.kind !== "lot") {
      dropDock();
      if (liveEtaLotId !== null) stopLiveEta();
    }
    if (m && m.kind === "station") renderStation(m);
    else if (m && m.kind === "lot") renderLot(m);
    else if (m && m.kind === "gone") body.appendChild(mk(doc, "p", "insp-note", "This item is no longer on the line"));
  }

  if (closeBtn) {
    closeBtn.addEventListener("click", () => {
      closed = true;
      if (ctx.scene3d) ctx.scene3d.select(null);
      store.select(null);
    });
  }
  doc.addEventListener("keydown", (e) => {
    if (e.key !== "Escape") return;
    const pop = el("order-popover");
    if (pop && !pop.classList.contains("hidden")) return;
    if (!store.getState().selection) return;
    if (ctx.scene3d) ctx.scene3d.select(null);
    store.select(null);
  });

  let last = 0;
  let timer = null;
  if (!failureConfigRequested) {
    failureConfigRequested = true;
    getFailureConfig().then((cfg) => store.setFailureConfig(cfg)).catch(() => {});
  }
  loadRecipes();
  store.subscribe(() => {
    const wait = RENDER_MS - (Date.now() - last);
    if (wait <= 0) {
      last = Date.now();
      render();
    } else if (!timer) {
      timer = setTimeout(() => {
        timer = null;
        last = Date.now();
        render();
      }, wait);
    }
  });
  render();
  // Finishes-in countdown ticks every second even when no SSE tick arrives.
  setInterval(() => {
    const st = store.getState();
    if (st && st.selection && st.selection.kind === "lot") render();
  }, 1000);
}
