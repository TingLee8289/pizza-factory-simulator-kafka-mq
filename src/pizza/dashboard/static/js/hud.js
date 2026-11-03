// HUD renderer. All numbers come from state.kpi (the relayed KpiSnapshot); nothing is computed here
// beyond unit conversion and formatting (D-01).
import { isComparisonLocked } from "./store.js";
import { formatPizzaRate } from "./rate.js";

const DASH = "—";
const WARMING = "warming up…";
const LITTLE_WARN_AT = 0.15;
const FLUSH_MS = 200;
const BANNER_TEXT =
  "Can't reach the dashboard service. Check that the dashboard container is running " +
  "(docker compose --profile line up), then it will reconnect automatically.";
const COMPARISON_LOCKED_TITLE = "Locked while a comparison is running";

function num(v, digits) {
  return typeof v === "number" && Number.isFinite(v) ? v.toFixed(digits) : DASH;
}

// Cycle time is stored in seconds; the HUD shows minutes.
const toMin = (v) => (typeof v === "number" ? v / 60 : null);

const WIP_TITLE = "WIP (pizzas; lots shown in sub-label). Includes WAIT in the stocker. Excludes BANK and scrap lots.";
const THROUGHPUT_TITLE = "Throughput (pizzas/min; lots/min shown in sub-label)";
const LITTLE_TITLE =
  "Little's Law (pizzas): L = λ·W. L is time-averaged pizza WIP, λ is pizzas/min, W is the pizza-weighted mean cycle time. L counts WIP plus capacity-BANK lots, because their bank time counts in cycle time. " +
  "Customer/schedule BANK and scrap lots are excluded.";
const DUE_TITLE = "Due-date KPIs. Late = completed after due time. Scrap lots are excluded.";
const DUE_WARMING = "Late - warming up...";

// Pure due-date chip view model (KPI-07, D-09, D-11). All numbers come from
// kpi.due (the relayed KpiSnapshot.due); nothing is computed here beyond
// unit conversion and formatting, matching the rest of this module (D-01).
export function dueView(due) {
  if (!due || !due.n_completed) {
    return { text: DUE_WARMING, tone: "warming", theory: "", title: DUE_TITLE };
  }
  const p = Math.round((due.late_pct || 0) * 100);
  const late = due.late_count || 0;
  const done = due.n_completed;
  let text = `Late ${p}%  -  ${late}/${done} lots`;
  if (due.n_failed > 0) text += `, scrap ${due.n_failed}`;
  const tone = p === 0 ? "ok" : "warn";
  const mean = Math.round(due.tardiness_mean_s || 0);
  const max = Math.round(due.tardiness_max_s || 0);
  const theory = `Tardiness mean ${mean} s / max ${max} s`;
  return { text, tone, theory, title: DUE_TITLE };
}

export function littleView(little, theory, kpi) {
  const th = theory || {};
  const parts = [];
  if (typeof th.capacity_pizzas_per_s === "number") parts.push(`capacity ${(th.capacity_pizzas_per_s * 60).toFixed(1)} pizzas/min`);
  if (typeof th.raw_process_time_s === "number") parts.push(`raw process time ${th.raw_process_time_s.toFixed(1)}s`);
  if (typeof th.critical_wip === "number") parts.push(`critical WIP ${th.critical_wip.toFixed(1)}`);
  let theoryText = parts.length ? `Theory: ${parts.join(" · ")}` : "";
  const cap = kpi && typeof kpi.wip_bank_capacity === "number" ? kpi.wip_bank_capacity : 0;
  if (cap > 0) {
    const basis = `L = WIP ${num(kpi.wip_pizzas, 0)} pizzas + BANK cap ${cap} lots`;
    theoryText = theoryText ? `${theoryText} · ${basis}` : basis;
  }
  const title = LITTLE_TITLE;
  if (!little || little.status === "insufficient_data" || typeof little.rel_error !== "number") {
    return { text: WARMING, tone: "warming", theory: theoryText, title };
  }
  const tone = little.rel_error < LITTLE_WARN_AT ? "ok" : "warn";
  const text = `L ${num(little.l, 2)}  vs  λ·W ${num(little.lambda_w, 2)} pizzas   Δ ${(little.rel_error * 100).toFixed(1)}%`;
  return { text, tone, theory: theoryText, title };
}

function theoryAvail(kpi) {
  const th = kpi.theory || {};
  const st = th.bottleneck_station;
  const av = st && kpi.stations && kpi.stations[st] ? kpi.stations[st].availability : null;
  if (typeof th.capacity_pizzas_per_s !== "number" || typeof av !== "number") return "";
  return `theory×A ${(th.capacity_pizzas_per_s * av * 60).toFixed(1)} pizzas/min`;
}

export function formatKpi(kpi) {
  const empty = { value: DASH, sub: WARMING, warming: true };
  if (!kpi) {
    return {
      throughput: { ...empty }, wip: { ...empty }, cycle: { ...empty },
      little: littleView(null, null),
    };
  }
  const warming = Boolean(kpi.warming_up);
  const ct = kpi.cycle_time || {};
  const sub = (s) => (warming ? WARMING : s);
  return {
    throughput: {
      value: num(typeof kpi.pizzas_per_s === "number" ? kpi.pizzas_per_s * 60 : null, 1),
      sub: sub(`${num(typeof kpi.throughput_per_s === "number" ? kpi.throughput_per_s * 60 : null, 1)} lots/min`),
      warming,
      title: [THROUGHPUT_TITLE, theoryAvail(kpi)].filter(Boolean).join(". "),
    },
    wip: {
      value: num(kpi.wip_pizzas, 0),
      sub: sub(`${num(kpi.wip_now, 0)} lots`),
      warming,
      title: `${WIP_TITLE} Excl. BANK ${kpi.wip_bank ?? 0}, scrap ${kpi.wip_failed ?? 0}.`,
    },
    cycle: {
      value: num(toMin(ct.per_pizza_mean_s), 2),
      sub: sub(`p50 ${num(toMin(ct.per_pizza_p50_s), 2)} / p95 ${num(toMin(ct.per_pizza_p95_s), 2)}`),
      warming,
      title: "Cycle time per pizza (min/pizza): lot cycle time / qty, weighted by qty. Little's Law still uses the pizza-weighted lot cycle time W.",
    },
    little: littleView(kpi.little, kpi.theory, kpi),
  };
}

function setCard(el, card) {
  if (!el) return;
  el.querySelector(".kpi-value").textContent = card.value;
  el.querySelector(".kpi-sub").textContent = card.sub;
  el.classList.toggle("warming", card.warming);
  if (card.title) el.title = card.title;
}

// Live-badge override text (COMP-01, UI-SPEC 3): compare.js sets this to
// "Compare R{n}/3" while a round runs or awaits "Next round", and clears it
// (null) otherwise, so there is a single writer of #live-badge's text.
let badgeOverride = null;
export function setBadgeOverride(text) {
  badgeOverride = text || null;
}

function badgeView(state) {
  const run = state.run || {};
  if (run.state === "none" || !run.state) return { text: "Idle", cls: "idle" };
  if (run.state === "paused" || (state.line && state.line.paused)) return { text: "Paused", cls: "paused" };
  return { text: "Live", cls: "live" };
}

// Banner after a host-sleep auto pause; hidden again once hostSleep clears (resume/reset).
function renderHostSleep(el, anchor, state) {
  let banner = el("host-sleep-banner");
  if (!banner && anchor && anchor.parentNode) {
    banner = anchor.ownerDocument.createElement("span");
    banner.id = "host-sleep-banner";
    banner.className = "badge paused";
    anchor.parentNode.insertBefore(banner, anchor.nextSibling);
  }
  if (!banner) return;
  const hs = state.hostSleep;
  banner.hidden = !hs;
  banner.textContent = hs
    ? `偵測到主機睡眠(約 ${Math.round(hs.slept_s)} 秒),已自動暫停,請按「繼續」恢復`
    : "";
}

export function render(ctx, state) {
  const { el } = ctx;
  const view = formatKpi(state.kpi);
  setCard(el("kpi-throughput"), view.throughput);
  setCard(el("kpi-wip"), view.wip);
  const wipEl = el("kpi-wip");
  if (wipEl && view.wip.title) wipEl.title = view.wip.title;
  setCard(el("kpi-cycle"), view.cycle);

  const chip = el("little-chip");
  if (chip) {
    chip.querySelector(".little-main").textContent = view.little.text;
    chip.querySelector(".little-theory").textContent = view.little.theory;
    chip.title = view.little.title;
    chip.classList.remove("ok", "warn", "warming");
    chip.classList.add(view.little.tone);
  }

  const dueChip = el("due-chip");
  if (dueChip) {
    const due = dueView(state.kpi && state.kpi.due);
    dueChip.querySelector(".due-main").textContent = due.text;
    dueChip.querySelector(".due-theory").textContent = due.theory;
    dueChip.title = due.title;
    dueChip.classList.remove("ok", "warn", "warming");
    dueChip.classList.add(due.tone);
  }

  const run = state.run || {};
  const runState = run.state || "none";
  const badge = badgeView(state);
  const live = el("live-badge");
  if (live) {
    live.textContent = badgeOverride || badge.text;
    live.classList.remove("live", "paused", "idle");
    live.classList.add(badge.cls);
  }
  renderHostSleep(el, live, state);
  const runId = el("run-id");
  if (runId) runId.textContent = run.run_id || "";

  // The main Start/Pause/Resume button is owned by controls.js (label + lock).
  // Reset is also locked while a comparison round is running or awaiting
  // "Next round" (controls.js locks the rule switch/rate/+Order the same way;
  // both sides compute the same formula from isComparisonLocked so neither fights).
  const locked = isComparisonLocked(state);
  const reset = el("btn-reset");
  if (reset) {
    reset.disabled = runState === "none" || locked;
    reset.title = locked ? COMPARISON_LOCKED_TITLE : "";
  }
  const rate = el("rate-value");
  if (rate && typeof run.arrival_rate_per_min === "number") rate.textContent = formatPizzaRate(run.arrival_rate_per_min);

  const empty = el("empty-run");
  if (empty) empty.classList.toggle("hidden", runState !== "none");
}

function renderConnection(ctx, state, startedAt, now) {
  const { el, store } = ctx;
  const conn = el("conn-state");
  if (conn) {
    const reconnecting = state.connection === "reconnecting";
    conn.textContent = reconnecting ? "Reconnecting…" : "";
    conn.classList.toggle("warn", reconnecting);
  }
  const noData = state.lastDataAt == null ? now - startedAt > 5000 : store.isStale(now);
  const scene = el("scene");
  if (scene) scene.classList.toggle("dim", store.isStale(now));
  const banner = el("error-banner");
  if (banner) {
    banner.textContent = noData ? BANNER_TEXT : "";
    banner.classList.toggle("hidden", !noData);
  }
}

function initTabs(ctx) {
  const tabs = ctx.el("tabs");
  if (!tabs) return;
  const wrap = ctx.el("tabs-wrap");
  const toggle = ctx.el("tabs-toggle");
  const buttons = [...tabs.querySelectorAll("[role=tab]")];
  let expanded = true;

  const setExpanded = (next) => {
    if (next === expanded) return;
    expanded = next;
    if (wrap) wrap.classList.toggle("collapsed", !expanded);
    if (toggle) {
      const label = expanded ? "Collapse panel" : "Expand panel";
      toggle.setAttribute("aria-expanded", expanded ? "true" : "false");
      toggle.setAttribute("aria-label", label);
      toggle.title = label;
    }
  };

  const select = (name) => {
    for (const b of buttons) {
      const on = b.dataset.tab === name;
      b.classList.toggle("active", on);
      b.setAttribute("aria-selected", on ? "true" : "false");
      const panel = ctx.el(`tab-${b.dataset.tab}`);
      if (panel) panel.classList.toggle("hidden", !on);
    }
    setExpanded(true);
  };
  // The whole head bar toggles collapse; a click on another tab switches to it (and expands).
  const head = ctx.el("tabs-head");
  const onHead = (e) => {
    const tab = e.target.closest && e.target.closest("[role=tab]");
    if (tab && !tab.classList.contains("active")) select(tab.dataset.tab);
    else setExpanded(!expanded);
  };
  if (head) head.addEventListener("click", onHead);
  else {
    for (const b of buttons) b.addEventListener("click", () => select(b.dataset.tab));
    if (toggle) toggle.addEventListener("click", () => setExpanded(!expanded));
  }
}

function initKpiTrends(ctx) {
  const pairs = [
    ["kpi-throughput", "kpi-trend-throughput"],
    ["kpi-wip", "kpi-trend-wip"],
    ["kpi-cycle", "kpi-trend-cycle"],
  ].map(([c, p]) => ({ card: ctx.el(c), panel: ctx.el(p) })).filter((x) => x.card && x.panel);
  let current = null;
  const hide = (x) => {
    if (current !== x) return;
    x.panel.classList.add("hidden");
    current = null;
  };
  const show = (x) => {
    if (current && current !== x) hide(current);
    x.panel.classList.remove("hidden");
    current = x;
    const r = x.card.getBoundingClientRect();
    const w = x.panel.offsetWidth;
    const h = x.panel.offsetHeight;
    let top = r.bottom + 8;
    if (top + h > window.innerHeight - 8) top = Math.max(8, r.top - h - 8);
    const left = Math.max(8, Math.min(r.left, window.innerWidth - w - 8));
    x.panel.style.left = `${left}px`;
    x.panel.style.top = `${top}px`;
    requestAnimationFrame(() => window.dispatchEvent(new Event("resize")));
  };
  for (const x of pairs) {
    x.card.addEventListener("mouseenter", () => show(x));
    x.card.addEventListener("focus", () => show(x));
    x.card.addEventListener("mouseleave", () => hide(x));
    x.card.addEventListener("blur", () => hide(x));
  }
}

export function init(ctx) {
  const startedAt = Date.now();
  initTabs(ctx);
  initKpiTrends(ctx);
  const flush = () => {
    const state = ctx.store.getState();
    if (state.dirty) {
      state.dirty = false;
      render(ctx, state);
    }
    renderConnection(ctx, state, startedAt, Date.now());
  };
  render(ctx, ctx.store.getState());
  setInterval(flush, FLUSH_MS);
}
