import test from "node:test";
import assert from "node:assert/strict";
import {
  inspectorModel, ROUTE, breakButtons, downLabel, bankControls, locationLabel, machineLabel, stockerLabel, reworkLabel, lotNotes,
  dueRow, priorityEdit, etaRows, lotInfoRows, processDetailRows, noteCount, dueOutcome, clockLabel, clockOf, isScrapLot, lotDue,
  scrapControls, holdChip, actionMenuItems, dockStateKey, SCRAP_REASONS,
  recipeTableFor, recipeRetryDelay, nextRecipeSeconds, recipeDraftView, recipeTabDot, shouldFetchRecipes, RECIPE_RETRY_MIN_MS, RECIPE_RETRY_MAX_MS,
} from "../../src/pizza/dashboard/static/js/inspector.js";

const CFG = { mtbf_s: 120, mttr_s: 20, rework_limits: { bake: 2 }, bank_after_s: 30, bank_wait_threshold: 5, heartbeat_interval_s: 2 };

const lot = {
  order_id: "o-1", product: "margherita", qty: 2, priority: 5, due_ts: "2026-01-01T10:05:00Z",
  state: "running", station: "sauce", steps_done: 1,
  steps: {
    spin: { started_ts: "2026-01-01T10:00:00Z", completed_ts: "2026-01-01T10:00:12Z" },
    sauce: { started_ts: "2026-01-01T10:00:13Z", completed_ts: null },
  },
};
const base = (selection) => ({
  selection,
  line: { stations: { bake: { status: "busy", queue: 4, current_lot: "o-1" } }, lots: [lot] },
  kpi: { stations: { bake: { utilization: 0.5 } } },
});

test("null selection gives null", () => assert.equal(inspectorModel(base(null)), null));

test("station model reads kpi utilization", () => {
  const m = inspectorModel(base({ kind: "station", id: "bake" }));
  assert.equal(m.kind, "station");
  assert.equal(m.utilization, 0.5);
  assert.equal(m.queue, 4);
  assert.equal(m.currentLot, "o-1");
});

test("utilization is null when kpi absent (never derived)", () => {
  const s = base({ kind: "station", id: "bake" });
  s.kpi = null;
  assert.equal(inspectorModel(s).utilization, null);
});

test("station model carries its recipe table", () => {
  const s = base({ kind: "station", id: "bake" });
  s.recipes = { bake: { margherita: 12, pepperoni: 13, veggie: 14 } };
  assert.deepEqual(inspectorModel(s).recipes, s.recipes.bake);
  assert.equal(inspectorModel(base({ kind: "station", id: "bake" })).recipes, null);
  s.recipes = { bake: {} };
  assert.equal(inspectorModel(s).recipes, null);
});

test("lot model and gone", () => {
  const m = inspectorModel(base({ kind: "lot", id: "o-1" }));
  assert.equal(m.step, "sauce");
  assert.equal(m.priority, 5);
  assert.equal(inspectorModel(base({ kind: "lot", id: "zzz" })).kind, "gone");
  assert.equal(inspectorModel(base({ kind: "station", id: "nope" })).kind, "gone");
});

test("breakButtons enable rules and tooltips", () => {
  assert.deepEqual(breakButtons({ status: "idle" }), { break: { enabled: true, tooltip: "" }, repair: { enabled: false, tooltip: "Station is running" } });
  assert.equal(breakButtons({ status: "busy" }).break.enabled, true);
  const down = breakButtons({ status: "down" });
  assert.equal(down.break.enabled, false);
  assert.equal(down.break.tooltip, "Station is already down");
  assert.equal(down.repair.enabled, true);
  const off = breakButtons({ status: "offline" });
  assert.equal(off.break.tooltip, "Station is offline");
  assert.equal(off.repair.enabled, false);
  assert.equal(breakButtons({ status: "down", pending: true }).repair.enabled, false);
  assert.equal(breakButtons({ status: "idle", pending: true }).break.enabled, false);
});

test("downLabel countdown clamps at zero", () => {
  const now = Date.parse("2026-01-01T10:00:00Z");
  assert.equal(downLabel({ status: "down", repairEtaTs: "2026-01-01T10:00:12Z" }, now), "Down - auto-repair in 12 s");
  assert.equal(downLabel({ status: "down", repairEtaTs: "2026-01-01T09:59:00Z" }, now), "Down - auto-repair in 0 s");
  assert.equal(downLabel({ status: "down", repairEtaTs: null }, now), "Down");
});

test("station model adds failure fields", () => {
  const s = base({ kind: "station", id: "bake" });
  s.line.stations.bake = { status: "down", queue: 1, cause: "manual", repair_eta_ts: "2026-01-01T10:00:12Z", blocked: 2 };
  s.kpi = { stations: { bake: { utilization: 0.5, availability: 0.92, down_s: 75 } } };
  s.run = { random_breakdowns: false };
  let m = inspectorModel(s);
  assert.equal(m.cause, "manual");
  assert.equal(m.blocked, 2);
  assert.equal(m.availability, 0.92);
  assert.equal(m.downS, 75);
  assert.equal(m.randomBreakdowns, "Random breakdowns: Off");
  s.run = { random_breakdowns: true };
  s.failureConfig = CFG;
  m = inspectorModel(s);
  assert.equal(m.randomBreakdowns, "Random breakdowns: MTBF 120 s / MTTR 20 s");
  s.kpi = null;
  assert.equal(inspectorModel(s).availability, null);
});

test("bankControls cases", () => {
  const wait = { state: "queued", station: "bake", dispatched: false };
  assert.deepEqual(bankControls(wait, "", CFG), { mode: "bank", enabled: false, tooltip: "Choose a reason first" });
  assert.equal(bankControls(wait, "capacity", CFG).enabled, true);
  // dispatched / cooling / running lots can be banked (deferred or immediate)
  assert.equal(bankControls({ ...wait, dispatched: true }, "capacity", CFG).enabled, true);
  assert.equal(bankControls({ ...wait, cooling: true }, "capacity", CFG).enabled, true);
  assert.equal(bankControls({ ...wait, state: "pre_release" }, "capacity", CFG).tooltip, "Release the lot first");
  const held = { ...wait, hold: { kind: "bank", station: "bake" } };
  assert.deepEqual(bankControls(held, "capacity", CFG), { mode: "bank", enabled: false, tooltip: "Hold already requested - cancel it first" });
  // WR-02: a rework lot back in the queue after the hold TTL can be banked
  const back = { ...wait, visit: 2, retrySince: "t", cooling: false };
  assert.equal(bankControls(back, "capacity", CFG).enabled, true);
  assert.equal(locationLabel(back), "bake-1");
  assert.equal(bankControls({ ...wait, state: "running" }, "capacity", CFG).enabled, true);
  assert.equal(bankControls({ ...wait, state: "done" }, "capacity", CFG).tooltip, "Lot is finished");
  assert.equal(bankControls({ ...wait, state: "failed" }, "capacity", CFG).tooltip, "Scrap lots cannot be banked");
  const banked = { state: "banked", station: "bake", bank: { source: "manual", station: "bake" } };
  assert.deepEqual(bankControls(banked, "", CFG), { mode: "release", enabled: true, tooltip: "" });
  const down = bankControls({ ...banked, bank: { source: "auto_down", station: "bake" } }, "", CFG);
  assert.equal(down.enabled, false);
  assert.equal(down.tooltip, "Released automatically when Bake recovers");
  assert.equal(bankControls({ ...banked, bank: { source: "auto_wait", station: "bake" } }, "", CFG).tooltip,
    "Released automatically when Bake WAIT drops below 5");
});

test("locationLabel and reworkLabel", () => {
  assert.equal(locationLabel({ state: "queued", station: "bake" }), "bake-1");
  assert.equal(locationLabel({ state: "running", station: "bake" }), "bake-1");
  assert.equal(locationLabel({ state: "queued", station: "bake", stocker: { kind: "overflow", station: "bake" } }), "stocker-1");
  assert.equal(locationLabel({ state: "queued", station: "bake", stocker: { kind: "station_down", station: "bake" } }), "stocker-1");
  assert.equal(locationLabel({ state: "queued", station: "bake", cooling: true }), "Waiting for rework (cooling)");
  assert.equal(locationLabel({ state: "banked", station: "bake" }), "stocker-1");
  assert.equal(machineLabel("toppings", 2), "toppings-2");
  assert.equal(stockerLabel(3), "stocker-3");
  assert.equal(reworkLabel({ visit: 2, station: "bake" }, CFG), "Rework 1/2");
  assert.equal(reworkLabel({ visit: 1, station: "bake" }, CFG), null);
});

test("lotNotes: bank note merges source and reason, auto banks name the trigger", () => {
  const since = "2026-01-01T10:00:00Z";
  const hhmmss = clockLabel(since);
  const man = lotNotes({ state: "banked", station: "bake", bank: { reason: "capacity", source: "manual", station: "bake", since_ts: since } }, CFG);
  assert.deepEqual(man, [`${hhmmss} Manual Bank due to Capacity`]);
  const auto = lotNotes({ state: "banked", station: "bake", bank: { reason: "customer", source: "auto_down", station: "bake", since_ts: since } }, CFG);
  assert.deepEqual(auto, [`${hhmmss} Auto Bank due to Customer - Bake down over 30 s`]);
  const w = lotNotes({ state: "banked", station: "bake", bank: { reason: "schedule", source: "auto_wait", station: "bake", since_ts: since } }, CFG);
  assert.deepEqual(w, [`${hhmmss} Auto Bank due to Schedule - Bake WAIT over 5`]);
  assert.deepEqual(lotNotes({ state: "queued" }, CFG), []);
  assert.deepEqual(lotNotes(null, CFG), []);
});

test("lotNotes: failures carry rework outcome, hold is noted, all sorted by time", () => {
  const t = (s) => `2026-01-01T10:00:${s}Z`;
  const m = {
    state: "queued", station: "bake",
    failures: [
      { station: "bake", visit: 2, cause: "quality", ts: t("30") },
      { station: "bake", visit: 1, cause: "breakdown", ts: t("10") },
      { station: "bake", visit: 3, cause: "breakdown", ts: t("50") },
      { station: "sauce", visit: 1, cause: "<img onerror=x>", ts: t("20") },
    ],
    hold: { kind: "scrap", reason: "material", station: "bake", since_ts: t("40") },
  };
  assert.deepEqual(lotNotes(m, CFG), [
    `${clockLabel(t("10"))} Bake failed (machine breakdown) - rework 1/2`,
    `${clockLabel(t("20"))} Sauce failed (quality)`,
    `${clockLabel(t("30"))} Bake failed (quality) - rework 2/2`,
    `${clockLabel(t("40"))} Scrap requested after Bake due to Material`,
    `${clockLabel(t("50"))} Bake failed (machine breakdown) - scrapped (rework limit 2)`,
  ]);
  assert.deepEqual(lotNotes({ state: "queued", failures: "nope" }, CFG), []);
});

test("dueRow: future due is a plain mm:ss row, past due is a warn chip, null when no due_ts", () => {
  const now = Date.parse("2026-01-01T10:00:00Z");
  assert.deepEqual(dueRow({ due_ts: "2026-01-01T10:05:00Z" }, now), { label: "Due", text: "05:00" });
  assert.deepEqual(dueRow({ due_ts: "2026-01-01T09:58:30Z" }, now), { label: "Due", text: "Late by 01:30", chip: "warn" });
  assert.equal(dueRow({ due_ts: null }, now), null);
  assert.equal(dueRow(null, now), null);
});

test("priorityEdit: Apply disabled for terminal and pre_release lots", () => {
  for (const state of ["done", "failed", "pre_release"]) {
    assert.equal(priorityEdit({ state }).enabled, false, state);
  }
  assert.equal(priorityEdit({ state: "queued", failed: { station: "bake" } }).enabled, false);
  assert.equal(priorityEdit({ state: "banked" }).enabled, true);
  assert.equal(priorityEdit(null).enabled, false);
});

test("priorityEdit: tooltip explains why Apply is disabled", () => {
  assert.equal(priorityEdit({ state: "pre_release" }).tooltip, "Release the lot first");
  assert.equal(priorityEdit({ state: "done" }).tooltip, "Lot is finished");
  assert.equal(priorityEdit({ state: "banked" }).tooltip, "");
});

const NOW = 1700000000000;

test("recipeTableFor returns only a non-empty table for the station", () => {
  const table = { margherita: 12, pepperoni: 13 };
  assert.equal(recipeTableFor({ recipes: { bake: table } }, "bake"), table);
  assert.equal(recipeTableFor({ recipes: null }, "bake"), null);
  assert.equal(recipeTableFor({ recipes: {} }, "bake"), null);
  assert.equal(recipeTableFor({ recipes: { bake: {} } }, "bake"), null);
  assert.equal(recipeTableFor({ recipes: { bake: table } }, "spin"), null);
  assert.equal(recipeTableFor(null, "bake"), null);
});

test("recipeRetryDelay doubles from the minimum and caps at the maximum", () => {
  assert.equal(recipeRetryDelay(0), 2000);
  assert.equal(recipeRetryDelay(0), RECIPE_RETRY_MIN_MS);
  assert.equal(recipeRetryDelay(1), RECIPE_RETRY_MIN_MS);
  assert.equal(recipeRetryDelay(2), 4000);
  assert.equal(recipeRetryDelay(3), 8000);
  assert.equal(recipeRetryDelay(10), RECIPE_RETRY_MAX_MS);
  assert.equal(recipeRetryDelay(-1), RECIPE_RETRY_MIN_MS);
  assert.equal(recipeRetryDelay(NaN), RECIPE_RETRY_MIN_MS);
});

test("shouldFetchRecipes gates on selection, cached data, inflight and backoff", () => {
  const sel = { selection: { kind: "station", id: "bake" } };
  const gate = (extra = {}) => ({ inflight: false, failures: 0, lastAttemptAt: null, ...extra });
  assert.equal(shouldFetchRecipes(null, gate(), NOW), false);
  assert.equal(shouldFetchRecipes({ selection: null }, gate(), NOW), false);
  assert.equal(shouldFetchRecipes({ selection: { kind: "lot", id: "o-1" } }, gate(), NOW), false);
  assert.equal(shouldFetchRecipes({ ...sel, recipes: { bake: { margherita: 12 } } }, gate(), NOW), false);
  assert.equal(shouldFetchRecipes(sel, gate(), NOW), true);
  assert.equal(shouldFetchRecipes(sel, gate({ inflight: true }), NOW), false);
  assert.equal(shouldFetchRecipes(sel, gate({ failures: 1, lastAttemptAt: NOW - 1000 }), NOW), false);
  assert.equal(shouldFetchRecipes(sel, gate({ failures: 1, lastAttemptAt: NOW - 2500 }), NOW), true);
  assert.equal(shouldFetchRecipes(sel, gate({ failures: 3, lastAttemptAt: NOW - 5000 }), NOW), false);
  assert.equal(shouldFetchRecipes(sel, gate({ failures: 3, lastAttemptAt: NOW - 8500 }), NOW), true);
});

test("shouldFetchRecipes bypasses backoff only when the station selection changed", () => {
  const sel = { selection: { kind: "station", id: "bake" } };
  const recent = { inflight: false, failures: 2, lastAttemptAt: NOW - 100 };
  assert.equal(shouldFetchRecipes(sel, recent, NOW, false), false);
  assert.equal(shouldFetchRecipes(sel, recent, NOW, true), true);
  assert.equal(shouldFetchRecipes(sel, { ...recent, inflight: true }, NOW, true), false);
  assert.equal(shouldFetchRecipes({ ...sel, recipes: { bake: { margherita: 12 } } }, recent, NOW, true), false);
});

test("inspectorModel passes reworkCount and failures with defaults", () => {
  const withData = { ...lot, rework_count: 2, failures: [{ visit: 1, cause: "breakdown" }] };
  const st = base({ kind: "lot", id: "o-1" });
  const m = inspectorModel({ ...st, line: { ...st.line, lots: [withData] } });
  assert.equal(m.reworkCount, 2);
  assert.equal(m.failures.length, 1);
  const d = inspectorModel(base({ kind: "lot", id: "o-1" }));
  assert.equal(d.reworkCount, 0);
  assert.deepEqual(d.failures, []);
});

const T0 = Date.parse("2026-01-01T10:00:00Z");
const iso = (sec) => new Date(T0 + sec * 1000).toISOString();
const lm = (over = {}) => ({ kind: "lot", state: "running", bank: null, failed: null, due: iso(300), eta: iso(90), etaNow: iso(98), etaError: null, completedTs: null, ...over });

test("inspectorModel passes ETA fields, nulls for legacy lots", () => {
  const st = base({ kind: "lot", id: "o-1" });
  st.line.lots = [{ ...lot, eta_ts: "E", station_eta: { bake: "S" }, eta_now_ts: "N", eta_error_s: 5, completed_ts: "C" }, { order_id: "legacy", state: "running" }];
  const m = inspectorModel(st);
  assert.deepEqual([m.eta, m.stationEta, m.etaNow, m.etaError, m.completedTs], ["E", { bake: "S" }, "N", 5, "C"]);
  st.selection = { kind: "lot", id: "legacy" };
  const l = inspectorModel(st);
  assert.deepEqual([l.eta, l.stationEta, l.etaNow, l.etaError, l.completedTs], [null, null, null, null, null]);
});

test("etaRows in progress: ETA with drift vs initial, time left", () => {
  const r = etaRows(lm(), T0);
  assert.deepEqual(r.map((x) => x.label), ["Current ETA", "Finishes in"]);
  assert.equal(r[0].text, clockLabel(iso(98)));
  assert.equal(r[0].suffix, "(+00:08 vs initial)");
  assert.equal(r[1].text, "01:38");
  assert.equal(r[1].chip, undefined);
});

test("etaRows missing data and drift variants", () => {
  assert.equal(etaRows(lm({ etaNow: null }), T0)[1].text, "01:30");
  assert.equal(etaRows(lm({ etaNow: null }), T0)[0].text, "—");
  const none = etaRows(lm({ eta: null, etaNow: null }), T0);
  assert.equal(none.length, 2);
  assert.ok(none.every((x) => x.text === "—" && !x.chip));
  assert.equal(etaRows(lm({ etaNow: iso(90.5) }), T0)[0].suffix, undefined);
  assert.equal(etaRows(lm({ etaNow: iso(87) }), T0)[0].suffix, "(-00:03 vs initial)");
});

test("etaRows overdue", () => {
  const r = etaRows(lm(), T0 + 110 * 1000)[1];
  assert.equal(r.text, "Late by 00:12");
  assert.equal(r.chip, "warn");
  assert.deepEqual(r.badge, { text: "Behind ETA", tone: "warn" });
});

test("etaRows completed lots", () => {
  const d = (e) => etaRows(lm({ state: "done", completedTs: iso(100), etaError: e }), T0);
  assert.deepEqual(d(72).map((x) => x.label), ["Initial ETA", "ETA error"]);
  assert.deepEqual([d(72)[1].text, d(72)[1].chip], ["+01:12 later", "warn"]);
  assert.deepEqual([d(-20)[1].text, d(-20)[1].chip], ["-00:20 earlier", "busy"]);
  assert.deepEqual([d(0.2)[1].text, d(0.2)[1].chip], ["On estimate", "busy"]);
  const miss = etaRows(lm({ state: "done", completedTs: iso(100), eta: null, etaError: null }), T0)[1];
  assert.deepEqual([miss.text, miss.chip], ["—", undefined]);
});

test("etaRows bank and failed lots", () => {
  const b = etaRows(lm({ state: "banked", bank: { reason: "capacity" } }), T0);
  assert.deepEqual(b.map((x) => x.text), ["—", "—"]);
  assert.deepEqual(b.map((x) => x.label), ["Current ETA", "Finishes in"]);
  assert.equal(etaRows(lm({ state: "failed", failed: { x: 1 } }), T0).length, 1);
  assert.equal(etaRows(lm({ state: "failed", failed: { x: 1 } }), T0)[0].label, "Initial ETA");
  assert.deepEqual(etaRows(null, T0), []);
  assert.equal(etaRows(lm({ eta: "garbage", etaNow: "garbage" }), T0)[0].text, "—");
});

test("dueOutcome", () => {
  assert.deepEqual(dueOutcome(lm({ completedTs: iso(200) })), { text: "On time", chip: "busy" });
  assert.deepEqual(dueOutcome(lm({ completedTs: iso(400) })), { text: "Late", chip: "warn" });
  assert.equal(dueOutcome(lm()), null);
  assert.equal(dueOutcome(lm({ completedTs: iso(1), due: null })), null);
  assert.equal(dueOutcome(null), null);
});

test("isScrapLot covers failed state and failed marker", () => {
  assert.equal(isScrapLot({ state: "failed" }), true);
  assert.equal(isScrapLot({ state: "queued", failed: { station: "bake", visit: 3 } }), true);
  assert.equal(isScrapLot({ state: "running" }), false);
  assert.equal(isScrapLot(null), false);
});

test("lotDue suppresses Due text and outcome for scrap lots", () => {
  // Past due and already completed: still no countdown, no Late by, no outcome chip.
  const scrap = lm({ state: "failed", failed: { station: "bake", visit: 3 }, due: iso(-60), completedTs: iso(200) });
  assert.deepEqual(lotDue(scrap, T0), { text: "—", chip: null, outcome: null });
  assert.equal(lotDue({ ...scrap, failed: null }, T0).text, "—");
  assert.deepEqual(lotDue({ ...scrap, state: "queued" }, T0), { text: "—", chip: null, outcome: null });
});

test("lotDue preserves live countdown and completed non-scrap outcome", () => {
  assert.deepEqual(lotDue(lm(), T0), { text: "05:00", chip: null, outcome: null });
  assert.deepEqual(lotDue(lm({ due: iso(-90) }), T0), { text: "Late by 01:30", chip: "warn", outcome: null });
  assert.deepEqual(lotDue(lm({ state: "done", completedTs: iso(200) }), T0), {
    text: "05:00", chip: null, outcome: { text: "On time", chip: "busy" },
  });
  assert.deepEqual(lotDue(lm({ state: "done", completedTs: iso(400) }), T0), {
    text: "05:00", chip: null, outcome: { text: "Late", chip: "warn" },
  });
  assert.deepEqual(lotDue(lm({ due: null }), T0), { text: "—", chip: null, outcome: null });
});

test("locationLabel sends scrap lots to the scrap bin", () => {
  assert.equal(locationLabel({ state: "failed", station: "bake" }), "Scrap bin");
  assert.equal(locationLabel({ state: "queued", station: "bake", failed: { station: "bake" }, stocker: { kind: "overflow", station: "bake" } }), "Scrap bin");
});

test("clockOf renders local HH:mm:ss, matching clockLabel", () => {
  const iso = "2026-10-21T09:05:07Z";
  const d = new Date(iso);
  const p = (n) => String(n).padStart(2, "0");
  const expected = `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
  assert.equal(clockOf(iso), expected);
  assert.equal(clockOf(iso), clockLabel(iso));
});

test("clockOf returns empty string for invalid or missing input, clockLabel keeps its dash", () => {
  for (const bad of [undefined, null, 123, "", "garbage"]) assert.equal(clockOf(bad), "");
  assert.equal(clockLabel("garbage"), "—");
  const iso = "2026-10-21T09:05:07Z";
  assert.equal(clockOf(null) || clockOf(iso), clockLabel(iso));
});

test("scrapControls: WAIT and BANK lots are scrappable", () => {
  const wait = { state: "queued", station: "bake", dispatched: false };
  assert.deepEqual(scrapControls(wait), { enabled: true, tooltip: "" });
  assert.equal(scrapControls({ ...wait, visit: 2, cooling: false }).enabled, true);
  assert.equal(scrapControls({ state: "banked", station: "bake", bank: { source: "auto_down" } }).enabled, true);
});

test("scrapControls: RUN / dispatched / cooling lots are scrappable (deferred or immediate)", () => {
  const wait = { state: "queued", station: "bake", dispatched: false };
  assert.deepEqual(scrapControls({ ...wait, state: "running" }), { enabled: true, tooltip: "" });
  assert.equal(scrapControls({ ...wait, dispatched: true }).enabled, true);
  assert.equal(scrapControls({ ...wait, cooling: true }).enabled, true);
});

test("scrapControls: DONE / SCRAP / pre_release / pending hold are disabled with a reason", () => {
  const wait = { state: "queued", station: "bake", dispatched: false };
  assert.equal(scrapControls({ ...wait, state: "done" }).tooltip, "Lot is finished");
  assert.equal(scrapControls({ ...wait, state: "failed" }).tooltip, "Lot is already scrapped");
  assert.equal(scrapControls({ ...wait, failed: { station: "bake" } }).enabled, false);
  assert.equal(scrapControls({ ...wait, state: "pre_release" }).tooltip, "Release the lot first");
  assert.deepEqual(scrapControls({ ...wait, hold: { kind: "scrap", station: "bake" } }),
    { enabled: false, tooltip: "Hold already requested - cancel it first" });
  assert.equal(scrapControls(null).enabled, false);
});

test("holdChip and the cancel-hold action item", () => {
  const wait = { state: "running", station: "bake" };
  assert.equal(holdChip(wait), null);
  assert.equal(holdChip(null), null);
  const bankHold = { ...wait, hold: { kind: "bank", station: "bake", reason: "capacity" } };
  assert.equal(holdChip(bankHold), "Bank after Bake");
  assert.equal(holdChip({ ...wait, hold: { kind: "scrap", station: "bake" } }), "Scrap after Bake");
  assert.deepEqual(actionMenuItems(wait, CFG).map((i) => i.id), ["bank", "scrap"]);
  const items = actionMenuItems(bankHold, CFG);
  assert.deepEqual(items.map((i) => i.id), ["bank", "scrap", "cancel_hold"]);
  assert.deepEqual([items[0].enabled, items[1].enabled, items[2].enabled], [false, false, true]);
  assert.equal(items[2].label, "Cancel hold");
  // the dock rebuilds when a hold appears
  assert.notEqual(dockStateKey("o-1", actionMenuItems(wait, CFG), {}), dockStateKey("o-1", items, {}));
});

test("inspectorModel passes the hold through", () => {
  const st = base({ kind: "lot", id: "o-1" });
  const hold = { kind: "bank", station: "bake", reason: "capacity", since_ts: "t" };
  const withHold = { ...st.line.lots[0], hold };
  assert.deepEqual(inspectorModel({ ...st, line: { ...st.line, lots: [withHold] } }).hold, hold);
  assert.equal(inspectorModel(st).hold, null);
});

test("actionMenuItems: Bank + Scrap for WAIT, Release + Scrap for BANK", () => {
  const wait = { state: "queued", station: "bake", dispatched: false };
  const items = actionMenuItems(wait, CFG);
  assert.deepEqual(items.map((i) => [i.id, i.label, i.enabled]), [["bank", "Bank", true], ["scrap", "Scrap", true]]);
  const banked = { state: "banked", station: "bake", bank: { source: "manual", station: "bake" } };
  assert.deepEqual(actionMenuItems(banked, CFG).map((i) => [i.id, i.label, i.enabled]), [["release", "Release", true], ["scrap", "Scrap", true]]);
  // auto-banked lots cannot be released but can still be scrapped
  const auto = actionMenuItems({ ...banked, bank: { source: "auto_down", station: "bake" } }, CFG);
  assert.equal(auto[0].enabled, false);
  assert.equal(auto[1].enabled, true);
});

test("actionMenuItems keeps the existing Bank enable rules and disables both items for finished lots", () => {
  const wait = { state: "queued", station: "bake", dispatched: true };
  const [bank, scrap] = actionMenuItems(wait, CFG);
  assert.deepEqual([bank.enabled, scrap.enabled], [true, true]);
  const done = actionMenuItems({ state: "failed", station: "bake" }, CFG);
  assert.deepEqual(done.map((i) => i.enabled), [false, false]);
  assert.equal(done[0].tooltip, "Scrap lots cannot be banked");
});

test("SCRAP_REASONS is a small fixed list", () => {
  assert.deepEqual(Object.keys(SCRAP_REASONS), ["operator", "quality", "material", "other"]);
});

test("lot model uses effective due (pause-extended) with due_ts fallback", () => {
  const sel = { kind: "lot", id: "o-1" };
  const withEff = { ...lot, due_eff_ts: "2026-01-01T10:08:00Z" };
  const s1 = { ...base(sel), line: { stations: {}, lots: [withEff] } };
  assert.equal(inspectorModel(s1).due, "2026-01-01T10:08:00Z");
  assert.equal(inspectorModel(base(sel)).due, "2026-01-01T10:05:00Z");
});

test("dueOutcome: completed after planned due but before effective due is On time", () => {
  const planned = Date.parse("2026-01-01T10:05:00Z");
  const completed = new Date(planned + 60 * 1000).toISOString();
  const eff = new Date(planned + 300 * 1000).toISOString();
  assert.deepEqual(dueOutcome({ due: eff, completedTs: completed }), { text: "On time", chip: "busy" });
  assert.deepEqual(dueOutcome({ due: new Date(planned).toISOString(), completedTs: completed }), { text: "Late", chip: "warn" });
});

test("dockStateKey: stable for identical input, changes on each dock-relevant change", () => {
  const items = [{ id: "bank", label: "Bank", enabled: true, tooltip: "" }, { id: "scrap", label: "Scrap", enabled: true, tooltip: "" }];
  const ui = { view: "closed", reasonChoice: "", scrapReason: "", scrapArmed: false, pending: false };
  const base = dockStateKey("o-1", items, ui);
  assert.equal(dockStateKey("o-1", items.map((i) => ({ ...i })), { ...ui }), base);
  assert.notEqual(dockStateKey("o-2", items, ui), base);
  assert.notEqual(dockStateKey("o-1", [{ ...items[0], enabled: false }, items[1]], ui), base);
  assert.notEqual(dockStateKey("o-1", [{ ...items[0], tooltip: "Lot is already sent to Bake" }, items[1]], ui), base);
  assert.notEqual(dockStateKey("o-1", items, { ...ui, view: "bank" }), base);
  assert.notEqual(dockStateKey("o-1", items, { ...ui, reasonChoice: "x" }), base);
  assert.notEqual(dockStateKey("o-1", items, { ...ui, scrapReason: "quality" }), base);
  assert.notEqual(dockStateKey("o-1", items, { ...ui, scrapArmed: true }), base);
  assert.notEqual(dockStateKey("o-1", items, { ...ui, pending: true }), base);
});

// -- Lot inspector tabs (quick 261024-0d7): Lot Info rows + Process Detail rows -------------

const LABELS = ["Product ID", "Qty", "Priority", "Step", "Status", "Location", "Initial ETA", "Current ETA", "Due"];
const pm = (over = {}) => lm({
  id: "o-1", product: "margherita", qty: 3, priority: 5, station: "cheese", visit: 1, hold: null, stocker: null, cooling: false,
  failures: [], visits: [], priorityChanges: [], stationEta: null, ...over,
});
const VISIT = (station, visit, s, c, f) => ({
  station, visit, started_ts: s == null ? null : iso(s), completed_ts: c == null ? null : iso(c), failed_ts: f == null ? null : iso(f),
});

test("lotInfoRows: exactly 9 rows in order for every status", () => {
  const cases = [
    pm({ state: "queued" }),
    pm({ state: "running" }),
    pm({ state: "banked", bank: { reason: "capacity", source: "manual", station: "cheese", since_ts: iso(5) } }),
    pm({ state: "failed", failed: { station: "cheese", visit: 1, ts: iso(5) }, station: null }),
    pm({ state: "done", station: null, completedTs: iso(100) }),
    pm({ state: "pre_release", station: null }),
  ];
  for (const m of cases) {
    const rows = lotInfoRows(m, CFG, T0);
    assert.deepEqual(rows.map((r) => r.label), LABELS, m.state);
  }
  assert.deepEqual(lotInfoRows(null, CFG, T0), []);
});

test("lotInfoRows: product, qty, priority are plain; WAIT lot location is the machine id", () => {
  const rows = Object.fromEntries(lotInfoRows(pm({ state: "queued", station: "toppings" }), CFG, T0).map((r) => [r.label, r]));
  assert.equal(rows["Product ID"].text, "Margherita");
  assert.equal(rows.Qty.text, "3");
  assert.equal(rows.Priority.text, "5");
  assert.equal(rows.Status.text, "WAIT");
  assert.equal(rows.Location.text, "toppings-1");
  assert.equal(rows.Step.text, "Toppings");
  assert.equal(rows.Step.chip, null);
  assert.equal(rows["Current ETA"].text, clockLabel(iso(98)));
  assert.equal(rows["Initial ETA"].text, clockLabel(iso(90)));
});

test("lotInfoRows: rework chip only on a rework visit, hold chip on Status", () => {
  const r = Object.fromEntries(lotInfoRows(pm({ state: "queued", station: "bake", visit: 2, hold: { kind: "bank", station: "bake" } }), CFG, T0).map((x) => [x.label, x]));
  assert.deepEqual(r.Step.chip, { text: "Rework 1/2", tone: "amber" });
  assert.deepEqual(r.Status.chip, { text: "Bank after Bake", tone: "amber" });
});

test("lotInfoRows: DONE, SCRAP and BANK lots", () => {
  const d = Object.fromEntries(lotInfoRows(pm({ state: "done", station: null, completedTs: iso(100), due: iso(50) }), CFG, T0).map((x) => [x.label, x]));
  assert.equal(d.Step.text, "SHIPPED");
  assert.equal(d.Status.text, "DONE");
  assert.equal(d.Location.text, "—");
  assert.equal(d["Current ETA"].text, clockLabel(iso(100)));
  assert.equal(d.Due.chip.text, "Late");
  const s = Object.fromEntries(lotInfoRows(pm({ state: "failed", station: null, failed: { station: "bake", visit: 1 }, visit: 3 }), CFG, T0).map((x) => [x.label, x]));
  assert.equal(s.Step.text, "Bake");
  assert.equal(s.Step.chip, null);
  assert.equal(s.Status.text, "SCRAP");
  assert.equal(s.Location.text, "Scrap bin");
  assert.equal(s["Current ETA"].text, "—");
  assert.equal(s.Due.text, "—");
  assert.equal(s.Due.chip, null);
  const b = Object.fromEntries(lotInfoRows(pm({ state: "banked", bank: { reason: "capacity", source: "manual", station: "cheese" } }), CFG, T0).map((x) => [x.label, x]));
  assert.equal(b.Status.text, "BANK");
  assert.equal(b.Location.text, "stocker-1");
  assert.equal(b["Current ETA"].text, "—");
});

test("lotInfoRows: Due chip countdown and late", () => {
  const busy = lotInfoRows(pm({ state: "running" }), CFG, T0).find((x) => x.label === "Due");
  assert.deepEqual(busy.chip, { text: "in 05:00", tone: "busy" });
  const late = lotInfoRows(pm({ state: "running", due: iso(10) }), CFG, T0 + 80000).find((x) => x.label === "Due");
  assert.deepEqual(late.chip, { text: "Late by 01:10", tone: "warn" });
});

test("locationLabel: done lot and missing station show a dash", () => {
  assert.equal(locationLabel({ state: "done", station: null }), "—");
  assert.equal(locationLabel({ state: "pre_release", station: null }), "—");
});

test("inspectorModel passes visits and priority changes", () => {
  const st = base({ kind: "lot", id: "o-1" });
  st.line.lots = [{ ...lot, visits: [{ station: "spin", visit: 1 }], priority_changes: [{ ts: "t", from: 5, to: 1 }] }, { order_id: "legacy", state: "running" }];
  const m = inspectorModel(st);
  assert.equal(m.visits.length, 1);
  assert.deepEqual(m.priorityChanges, [{ ts: "t", from: 5, to: 1 }]);
  st.selection = { kind: "lot", id: "legacy" };
  assert.deepEqual(inspectorModel(st).visits, []);
  assert.deepEqual(inspectorModel(st).priorityChanges, []);
});

const RECIPES = { spin: { margherita: 5 }, sauce: { margherita: 6 }, cheese: { margherita: 12 }, bake: { margherita: 20.5 } };

test("processDetailRows: one row per station with done/current/future kinds", () => {
  const m = pm({
    state: "running", station: "sauce",
    visits: [VISIT("spin", 1, 1, 6), VISIT("sauce", 1, 7, null)],
    stationEta: { spin: iso(6), sauce: iso(13), cheese: iso(25), toppings: iso(40) },
  });
  const { rows } = processDetailRows(m, RECIPES, CFG);
  assert.equal(rows.length, 7);
  assert.deepEqual(rows.map((r) => r.kind), ["done", "current", "future", "future", "future", "future", "future"]);
  assert.deepEqual(rows.map((r) => r.mark).slice(0, 3), ["✓", "▶", "○"]);
  assert.equal(rows[0].step, "Spin");
  assert.equal(rows[0].machine, "spin-1");
  assert.equal(rows[0].recipe, "5s");
  assert.equal(rows[0].inn, clockLabel(iso(1)));
  assert.equal(rows[0].out, clockLabel(iso(6)));
  assert.equal(rows[1].inn, clockLabel(iso(7)));
  assert.equal(rows[1].out, "—");
  assert.equal(rows[2].inn, `~${clockLabel(iso(13))}`);
  assert.equal(rows[2].out, `~${clockLabel(iso(25))}`);
  assert.equal(rows[2].recipe, "12s");
  assert.equal(rows[3].recipe, "—");
  assert.equal(rows[3].out, `~${clockLabel(iso(40))}`);
  assert.equal(rows[4].out, "—");
  assert.equal(rows[4].recipe, "20.5s");
});

test("processDetailRows: rework visits are separate rows (#1 failed, #2 current)", () => {
  const m = pm({
    state: "running", station: "cheese", visit: 2,
    visits: [VISIT("spin", 1, 1, 6), VISIT("sauce", 1, 7, 12), VISIT("cheese", 1, 13, null, 20), VISIT("cheese", 2, 25, null)],
    failures: [{ station: "cheese", visit: 1, cause: "quality", ts: iso(20) }],
  });
  const { rows } = processDetailRows(m, RECIPES, CFG);
  const cheese = rows.filter((r) => r.station === "cheese");
  assert.deepEqual(cheese.map((r) => [r.step, r.mark, r.kind]), [["Cheese #1", "✗", "failed"], ["Cheese #2", "▶", "current"]]);
  assert.equal(cheese[0].out, clockLabel(iso(20)));
  assert.equal(cheese[0].notes.length, 1);
  assert.equal(cheese[0].notes[0].tone, "danger");
  assert.match(cheese[0].notes[0].text, /Cheese failed \(quality\)/);
  assert.equal(cheese[1].notes.length, 0);
  assert.equal(rows.find((r) => r.station === "spin").step, "Spin");
});

test("processDetailRows: queued rework visit is appended as the current row", () => {
  const m = pm({
    state: "queued", station: "cheese", visit: 2,
    visits: [VISIT("cheese", 1, 13, null, 20)],
  });
  const { rows } = processDetailRows(m, RECIPES, CFG);
  const cheese = rows.filter((r) => r.station === "cheese");
  assert.deepEqual(cheese.map((r) => [r.step, r.kind]), [["Cheese #1", "failed"], ["Cheese #2", "current"]]);
  assert.equal(cheese[1].inn, "—");
  assert.equal(cheese[1].out, "—");
  assert.equal(cheese[1].recipe, "12s");
});

test("processDetailRows: scrap lot skips the remaining stations", () => {
  const m = pm({
    state: "failed", station: null, failed: { station: "cheese", visit: 3, ts: iso(30) },
    visits: [VISIT("spin", 1, 1, 6), VISIT("sauce", 1, 7, 12), VISIT("cheese", 1, 13, null, 20)],
    failures: [{ station: "cheese", visit: 1, cause: "quality", ts: iso(20) }],
  });
  const { rows } = processDetailRows(m, RECIPES, CFG);
  assert.deepEqual(rows.map((r) => r.kind), ["done", "done", "failed", "skipped", "skipped", "skipped", "skipped"]);
  const sk = rows[3];
  assert.deepEqual([sk.mark, sk.machine, sk.recipe, sk.inn, sk.out], ["–", "—", "—", "—", "—"]);
  assert.equal(rows.some((r) => r.kind === "current" || r.kind === "future"), false);
});

test("processDetailRows: done lot is all done rows", () => {
  const stations = ["spin", "sauce", "cheese", "toppings", "bake", "slice", "package"];
  const m = pm({ state: "done", station: null, completedTs: iso(80), visits: stations.map((s, i) => VISIT(s, 1, i * 10, i * 10 + 8)) });
  const { rows } = processDetailRows(m, RECIPES, CFG);
  assert.equal(rows.length, 7);
  assert.equal(rows.every((r) => r.kind === "done"), true);
});

test("processDetailRows: hold, bank and priority notes nest under their visit, sorted by time", () => {
  const m = pm({
    state: "banked", station: "bake",
    visits: [VISIT("spin", 1, 1, 6), VISIT("bake", 1, 10, null)],
    hold: { kind: "scrap", station: "spin", reason: "quality", since_ts: iso(4) },
    bank: { reason: "capacity", source: "manual", station: "bake", since_ts: iso(15) },
    priorityChanges: [{ ts: iso(12), from: 5, to: 1 }, { ts: iso(3), from: 9, to: 5 }],
    failures: [],
  });
  const { rows } = processDetailRows(m, RECIPES, CFG);
  const spin = rows.find((r) => r.station === "spin");
  const bake = rows.find((r) => r.station === "bake");
  assert.deepEqual(spin.notes.map((n) => [n.tone, n.text]), [
    ["amber", "Priority 9 → 5"],
    ["amber", "Scrap requested after Spin due to Quality"],
  ]);
  assert.deepEqual(bake.notes.map((n) => n.text), ["Priority 5 → 1", "Manual Bank due to Capacity"]);
  assert.equal(noteCount(m, CFG), 4);
});

test("noteCount: zero without events, non-lot models are empty", () => {
  assert.equal(noteCount(pm({ state: "running" }), CFG), 0);
  assert.equal(noteCount(null, CFG), 0);
  assert.deepEqual(processDetailRows(null, RECIPES, CFG), { rows: [] });
});

test("processDetailRows live: future rows use live ~IN/~OUT, running row gets ~OUT, frozen kept as plan", () => {
  const m = pm({
    state: "running", station: "sauce",
    visits: [VISIT("spin", 1, 1, 6), VISIT("sauce", 1, 7, null)],
    stationEta: { sauce: iso(13), cheese: iso(25), toppings: iso(40) },
  });
  const live = {
    sauce: { in: null, out: iso(16) },
    cheese: { in: iso(30), out: iso(42) },
    toppings: { in: iso(42), out: iso(70) },
  };
  const { rows } = processDetailRows(m, RECIPES, CFG, live);
  assert.equal(rows[1].kind, "current");
  assert.equal(rows[1].out, `~${clockLabel(iso(16))}`);
  assert.equal(rows[1].plan, `Plan ${clockLabel(iso(13))}`);
  assert.equal(rows[2].inn, `~${clockLabel(iso(30))}`);
  assert.equal(rows[2].out, `~${clockLabel(iso(42))}`);
  assert.equal(rows[2].plan, `Plan ${clockLabel(iso(25))}`);
  assert.equal(rows[3].inn, `~${clockLabel(iso(42))}`);
  assert.equal(rows[0].plan, null);
});

test("processDetailRows live: same-as-frozen or no frozen gives no plan", () => {
  const m = pm({
    state: "running", station: "cheese",
    visits: [VISIT("spin", 1, 1, 6), VISIT("sauce", 1, 7, 12), VISIT("cheese", 1, 13, null)],
    stationEta: { toppings: iso(40) },
  });
  const live = { toppings: { in: iso(30), out: iso(40) }, bake: { in: iso(40), out: iso(60) } };
  const { rows } = processDetailRows(m, RECIPES, CFG, live);
  const top = rows.find((r) => r.station === "toppings");
  const bake = rows.find((r) => r.station === "bake");
  assert.equal(top.plan, null);
  assert.equal(bake.plan, null);
  assert.equal(bake.out, `~${clockLabel(iso(60))}`);
});

test("processDetailRows live: null/empty live is identical to no live; done lot ignores live", () => {
  const m = pm({
    state: "running", station: "sauce",
    visits: [VISIT("spin", 1, 1, 6), VISIT("sauce", 1, 7, null)],
    stationEta: { spin: iso(6), sauce: iso(13), cheese: iso(25) },
  });
  const plain = processDetailRows(m, RECIPES, CFG);
  assert.deepEqual(processDetailRows(m, RECIPES, CFG, null), plain);
  assert.deepEqual(processDetailRows(m, RECIPES, CFG, {}), plain);
  const done = pm({
    state: "done", station: null, completedTs: iso(100),
    visits: ROUTE.map((s, i) => VISIT(s, 1, i * 10, i * 10 + 5)),
  });
  const live = Object.fromEntries(ROUTE.map((s) => [s, { in: iso(1), out: iso(2) }]));
  assert.deepEqual(processDetailRows(done, RECIPES, CFG, live), processDetailRows(done, RECIPES, CFG));
});

test("nextRecipeSeconds steps 1 s within [1, 3600] and returns null on a no-op", () => {
  assert.equal(nextRecipeSeconds(12, 1), 13);
  assert.equal(nextRecipeSeconds(12, -1), 11);
  assert.equal(nextRecipeSeconds(20.5, -1), 19.5);
  assert.equal(nextRecipeSeconds(1.5, -1), 1);
  assert.equal(nextRecipeSeconds(1, -1), null);
  assert.equal(nextRecipeSeconds(3600, 1), null);
  assert.equal(nextRecipeSeconds(NaN, 1), null);
  assert.equal(nextRecipeSeconds(undefined, 1), null);
});

test("recipeDraftView counts only valid changed rows and blocks Apply on an invalid value", () => {
  const live = { margherita: 12, pepperoni: 13, veggie: 14 };
  const clean = recipeDraftView(live, {});
  assert.equal(clean.changed.length, 0);
  assert.equal(clean.canApply, false);
  assert.equal(clean.canReset, false);

  const edited = recipeDraftView(live, { margherita: "45", pepperoni: 13 });
  assert.deepEqual(edited.changed.map((r) => [r.product, r.value]), [["margherita", 45]]);
  assert.equal(edited.rows.find((r) => r.product === "pepperoni").dirty, false);
  assert.equal(edited.canApply, true);

  const bad = recipeDraftView(live, { margherita: "45", veggie: "" });
  assert.equal(bad.invalid, true);
  assert.equal(bad.canApply, false);
  assert.equal(bad.canReset, true);
  assert.equal(recipeDraftView(live, { veggie: "0.5" }).invalid, true);
  assert.equal(recipeDraftView(live, { veggie: "3601" }).invalid, true);
  assert.equal(recipeDraftView(live, { veggie: "20.55" }).changed[0].value, 20.6);
  assert.deepEqual(recipeDraftView(null, null).rows, []);
});

test("lot model merges on-demand detail for the matching lot only", () => {
  const slim = { order_id: "o-1", product: "margherita", qty: 1, state: "queued", station: "spin" };
  const s = { selection: { kind: "lot", id: "o-1" }, line: { stations: {}, lots: [slim] }, kpi: null };
  const detail = {
    order_id: "o-1", visits: [{ station: "spin" }], failures: [{ ts: "t" }],
    priority_changes: [{ ts: "t", from: 1, to: 2 }], station_eta: { spin: "2026-01-01T10:00:00Z" },
  };
  const m = inspectorModel(s, detail);
  assert.deepEqual(m.visits, detail.visits);
  assert.deepEqual(m.failures, detail.failures);
  assert.deepEqual(m.priorityChanges, detail.priority_changes);
  assert.deepEqual(m.stationEta, detail.station_eta);
  const other = inspectorModel(s, { ...detail, order_id: "o-2" });
  assert.deepEqual(other.visits, []);
  assert.equal(other.stationEta, null);
  const none = inspectorModel(s, null);
  assert.deepEqual([none.visits, none.failures, none.priorityChanges, none.stationEta], [[], [], [], null]);
});

test("lot model still reads heavy fields from a legacy full-shape lot", () => {
  const m = inspectorModel(base({ kind: "lot", id: "o-1" }), null);
  assert.ok(Array.isArray(m.visits));
});

test("recipeTabDot flags unapplied recipe edits only while another station tab is open", () => {
  const live = { margherita: 14, pepperoni: 16 };
  assert.equal(recipeTabDot(live, null, "info"), false);
  assert.equal(recipeTabDot(live, {}, "info"), false);
  assert.equal(recipeTabDot(live, { margherita: 14 }, "actions"), false);
  assert.equal(recipeTabDot(live, { margherita: 20 }, "info"), true);
  assert.equal(recipeTabDot(live, { margherita: 20 }, "recipe"), false);
  assert.equal(recipeTabDot(live, { pepperoni: "" }, "actions"), true);
  assert.equal(recipeTabDot(null, { margherita: 20 }, "info"), false);
});
