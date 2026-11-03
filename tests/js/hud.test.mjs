import test from "node:test";
import assert from "node:assert/strict";
import { formatKpi, littleView, dueView } from "../../src/pizza/dashboard/static/js/hud.js";
import { seriesFrom } from "../../src/pizza/dashboard/static/js/charts.js";

const kpi = (over = {}) => ({
  warming_up: false,
  throughput_per_s: 0.05,
  pizzas_per_s: 0.1,
  wip_now: 7,
  wip_pizzas: 14,
  wip_avg: 6.5,
  wip_avg_pizzas: 13,
  cycle_time: { n: 10, mean_s: 42.04, p50_s: 40, p95_s: 60.5, per_pizza_mean_s: 21.02, per_pizza_p50_s: 20, per_pizza_p95_s: 30.25 },
  stations: { bake: { utilization: 0.934 } },
  bottleneck: "bake",
  theory: { capacity_per_s: 0.05, capacity_pizzas_per_s: 0.1, raw_process_time_s: 30, critical_wip: 3.5, bottleneck_station: "bake" },
  little: { status: "pass", l: 6.5, lambda_w: 6.3, rel_error: 0.05, tolerance: 0.15 },
  ...over,
});

test("formatKpi with null kpi shows dashes and warming", () => {
  const v = formatKpi(null);
  assert.equal(v.throughput.value, "—");
  assert.equal(v.wip.sub, "warming up…");
  assert.equal(v.little.tone, "warming");
});

test("formatKpi warming kpi keeps values with warming label", () => {
  const v = formatKpi(kpi({ warming_up: true }));
  assert.equal(v.wip.value, "14");
  assert.equal(v.wip.sub, "warming up…");
  assert.equal(v.wip.warming, true);
});

test("formatKpi normal kpi", () => {
  const v = formatKpi(kpi());
  assert.equal(v.throughput.value, "6.0");
  assert.equal(v.throughput.sub, "3.0 lots/min");
  assert.equal(v.throughput.title, "Throughput (pizzas/min; lots/min shown in sub-label)");
  assert.equal(v.wip.value, "14");
  assert.equal(v.wip.sub, "7 lots");
  assert.equal(v.cycle.value, "0.35");
  assert.equal(v.cycle.sub, "p50 0.33 / p95 0.50");
  assert.equal(v.bottleneck, undefined);
});

test("formatKpi ignores everything except kpi (no browser-side KPI)", () => {
  const a = formatKpi(kpi());
  const b = formatKpi({ ...kpi(), lots: [1, 2, 3], history: [{ x: 1 }] });
  assert.deepEqual(a, b);
});

test("littleView tones", () => {
  const mk = (rel) => ({ status: "pass", l: 1, lambda_w: 1, rel_error: rel, tolerance: 0.15 });
  const th = kpi().theory;
  assert.equal(littleView(mk(0.05), th).tone, "ok");
  assert.equal(littleView(mk(0.15), th).tone, "warn");
  assert.equal(littleView(mk(0.3), th).tone, "warn");
  assert.match(littleView(mk(0.05), th).text, /^L 1\.00  vs  λ·W 1\.00 pizzas   Δ 5\.0%$/);
  assert.match(littleView(mk(0.05), th).theory, /critical WIP 3\.5/);
});

test("littleView insufficient data", () => {
  const v = littleView({ status: "insufficient_data", rel_error: null }, null);
  assert.equal(v.tone, "warming");
  assert.equal(v.text, "warming up…");
});

test("seriesFrom caps and nulls", () => {
  const h = Array.from({ length: 700 }, (_, i) => ({ a: i % 2 ? null : i }));
  const s = seriesFrom(h, "a", 600);
  assert.equal(s.length, 600);
  assert.equal(s[1], null);
});

test("wip sub-line and title", () => {
  const v = formatKpi(kpi({ wip_bank: 2, wip_failed: 1 }));
  assert.equal(v.wip.sub, "7 lots");
  assert.equal(v.wip.title, "WIP (pizzas; lots shown in sub-label). Includes WAIT in the stocker. Excludes BANK and scrap lots. Excl. BANK 2, scrap 1.");
  assert.equal(formatKpi(kpi({ warming_up: true, wip_bank: 2 })).wip.sub, "warming up…");
});

test("little basis text and title", () => {
  const v = formatKpi(kpi({ wip_pizzas: 10, wip_bank_capacity: 3 }));
  assert.match(v.little.theory, / · L = WIP 10 pizzas \+ BANK cap 3 lots$/);
  assert.equal(formatKpi(kpi()).little.theory.includes("BANK cap"), false);
  assert.equal(
    v.little.title,
    "Little's Law (pizzas): L = λ·W. L is time-averaged pizza WIP, λ is pizzas/min, W is the pizza-weighted mean cycle time. L counts WIP plus capacity-BANK lots, because their bank time counts in cycle time. Customer/schedule BANK and scrap lots are excluded.",
  );
  assert.equal(v.little.tone, "ok");
});

test("throughput sub appends theory x availability", () => {
  const v = formatKpi(kpi({ stations: { bake: { utilization: 0.9, availability: 0.8 } } }));
  assert.equal(v.throughput.sub, "3.0 lots/min");
  assert.equal(v.throughput.title, "Throughput (pizzas/min; lots/min shown in sub-label). theory×A 4.8 pizzas/min");
  assert.match(formatKpi(kpi()).little.theory, /capacity 6\.0 pizzas\/min/);
});

const DUE_TITLE = "Due-date KPIs. Late = completed after due time. Scrap lots are excluded.";

test("dueView: zero late percent is tone ok", () => {
  const v = dueView({ late_count: 0, n_completed: 10, n_failed: 0, late_pct: 0, tardiness_mean_s: 0, tardiness_max_s: 0 });
  assert.equal(v.text, "Late 0%  -  0/10 lots");
  assert.equal(v.tone, "ok");
  assert.equal(v.theory, "Tardiness mean 0 s / max 0 s");
  assert.equal(v.title, DUE_TITLE);
});

test("dueView: nonzero late percent is tone warn, rounds seconds", () => {
  const v = dueView({ late_count: 3, n_completed: 12, n_failed: 0, late_pct: 0.25, tardiness_mean_s: 41.2, tardiness_max_s: 130 });
  assert.equal(v.text, "Late 25%  -  3/12 lots");
  assert.equal(v.tone, "warn");
  assert.equal(v.theory, "Tardiness mean 41 s / max 130 s");
  assert.equal(v.title, DUE_TITLE);
});

test("dueView: null or no completed orders is warming", () => {
  assert.deepEqual(dueView(null), { text: "Late - warming up...", tone: "warming", theory: "", title: DUE_TITLE });
  assert.equal(dueView({ n_completed: 0, late_pct: 0 }).tone, "warming");
});

test("dueView: scrap orders append suffix only when > 0", () => {
  const base = { late_count: 3, n_completed: 12, late_pct: 0.25, tardiness_mean_s: 41.2, tardiness_max_s: 130 };
  assert.equal(dueView({ ...base, n_failed: 0 }).text, "Late 25%  -  3/12 lots");
  assert.equal(dueView({ ...base, n_failed: 2 }).text, "Late 25%  -  3/12 lots, scrap 2");
});
