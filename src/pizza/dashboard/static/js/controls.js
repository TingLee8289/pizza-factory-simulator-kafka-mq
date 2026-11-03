// Run lifecycle controls, live rate slider, rule switch and the Start/Reset modals (DASH-03).
import { postJson, errorReason } from "./api.js";
import { showToast } from "./toast.js";
import { isComparisonLocked } from "./store.js";
import { MIN_PIZZA_RATE, MAX_PIZZA_RATE, toLotsPerMin, toPizzasPerMin } from "./rate.js";

const PRODUCTS = ["margherita", "pepperoni", "veggie"];
const FOCUSABLE = 'button:not([disabled]), input:not([disabled]), select:not([disabled]), [href], [tabindex]:not([tabindex="-1"])';

// --- Rule switch (SCHED-03, UI-SPEC section 1) ---
export const RULES = ["fifo", "edd", "spt"];
export const RULE_TITLES = {
  fifo: "FIFO - first in, first out",
  edd: "EDD - earliest due date first",
  spt: "SPT - shortest processing time at the station first",
};
export const LOCKED_TITLE = "Locked while a comparison is running";
export const RULE_ACK_MS = 3000;
export const NO_ACK_TEXT = "No response from the scheduler. The rule may not have changed - check the rule switch.";
export const RULE_ERROR_TEXT = "Could not change the dispatch rule. Check that RabbitMQ is running and try again.";

// Pending-ack timer with an injectable clock (default: the real
// setTimeout/clearTimeout), so controls.test.mjs can fast-forward past
// RULE_ACK_MS without depending on node:test's global timer mock (D-04:
// the clicked rule button stays pending at most 3 s before an error toast).
// Wrapped, not bare: calling window.setTimeout as clock.setTimeout(...) throws "Illegal invocation".
const BROWSER_CLOCK = {
  setTimeout: (fn, ms) => setTimeout(fn, ms),
  clearTimeout: (id) => clearTimeout(id),
};

export function createPendingAck({ ms = RULE_ACK_MS, onTimeout, clock = BROWSER_CLOCK } = {}) {
  let timer = null;
  let current = null;
  return {
    get current() {
      return current;
    },
    start(key) {
      if (timer !== null) clock.clearTimeout(timer);
      current = key;
      timer = clock.setTimeout(() => {
        timer = null;
        current = null;
        if (onTimeout) onTimeout(key);
      }, ms);
    },
    clear() {
      if (timer !== null) clock.clearTimeout(timer);
      timer = null;
      current = null;
    },
  };
}

// Pure view model for the three rule-switch buttons. The active segment is
// always fact-derived (never from the click, T-07-41); `pending` disables the
// clicked button until the ack fact arrives or the 3 s timeout fires; `locked`
// disables all three (but keeps the active segment's accent) while a
// comparison round is running or awaiting "Next round".
export function ruleViewModel({ active, pending, locked } = {}) {
  const out = {};
  for (const r of RULES) {
    const isPending = pending === r;
    out[r] = {
      active: r === active,
      pending: isPending,
      disabled: Boolean(locked) || isPending,
      title: locked ? LOCKED_TITLE : RULE_TITLES[r],
    };
  }
  return out;
}

export function ruleToast(rule) {
  return `Dispatch rule is now ${String(rule).toUpperCase()}. Applies to the next release.`;
}

export function ruleFeedLine(rule) {
  return `RULE ${String(rule).toUpperCase()}`;
}

// Pure disabled/title pairs for the controls that lock while a comparison
// round is active (Open Question 3 amendment): Reset, Stop line, + Order and
// the rate slider. Combine with any existing run-state-based disabling via
// `||` so this never un-disables something the run state already disabled.
export function lockedControls(locked) {
  const title = locked ? LOCKED_TITLE : "";
  return {
    reset: { disabled: Boolean(locked), title },
    stop: { disabled: Boolean(locked), title },
    order: { disabled: Boolean(locked), title },
    rate: { disabled: Boolean(locked), title },
  };
}

export function buildStartBody(values) {
  const v = values || {};
  const rawSeed = v.seed === undefined || v.seed === null ? "" : String(v.seed).trim();
  let seed = null;
  if (rawSeed !== "") {
    seed = Number(rawSeed);
    if (!Number.isInteger(seed)) return { error: "Seed must be a whole number" };
  }
  const rate = Number(v.rate);
  if (!Number.isInteger(rate) || rate < MIN_PIZZA_RATE || rate > MAX_PIZZA_RATE) {
    return { error: `Arrival rate must be a whole number between ${MIN_PIZZA_RATE} and ${MAX_PIZZA_RATE} pizzas/min` };
  }
  const weights = {};
  let total = 0;
  for (const p of PRODUCTS) {
    const w = Number((v.weights || {})[p]);
    if (!Number.isFinite(w) || w < 0) return { error: `Weight for ${p} must be zero or more` };
    weights[p] = w;
    total += w;
  }
  if (total <= 0) return { error: "At least one product weight must be above zero" };
  const body = { seed, arrival_rate_per_min: toLotsPerMin(rate), weights };
  if (v.randomBreakdowns === true) body.random_breakdowns = true;
  return body;
}

export function makeDebounced(fn, ms) {
  let timer = null;
  return (value) => {
    if (timer !== null) clearTimeout(timer);
    timer = setTimeout(() => {
      timer = null;
      fn(value);
    }, ms);
  };
}

// Pure index math for the Tab trap: wraps in both directions.
export function nextFocusIndex(count, current, shift) {
  if (count <= 0) return -1;
  if (current < 0) return shift ? count - 1 : 0;
  return shift ? (current - 1 + count) % count : (current + 1) % count;
}

export function trapFocus(container, onClose) {
  function handler(event) {
    if (event.key === "Escape") {
      if (event.preventDefault) event.preventDefault();
      onClose();
      return;
    }
    if (event.key !== "Tab") return;
    const items = Array.from(container.querySelectorAll(FOCUSABLE));
    if (items.length === 0) return;
    const doc = container.ownerDocument;
    const idx = items.indexOf(doc ? doc.activeElement : null);
    const next = nextFocusIndex(items.length, idx, Boolean(event.shiftKey));
    if (idx === -1 || (event.shiftKey && idx === 0) || (!event.shiftKey && idx === items.length - 1)) {
      event.preventDefault();
      items[next].focus();
    }
  }
  container.addEventListener("keydown", handler);
  return () => container.removeEventListener("keydown", handler);
}

// One main button: Start (no run) -> Pause (running) -> Resume (paused).
const MAIN_LABELS = { none: "Start", running: "Pause", paused: "Resume" };

export function buttonState(runState, locked = false) {
  const s = runState || "none";
  // Pause and Resume both call comparison-guarded endpoints, so lock them together.
  const lockedMain = s !== "none" && Boolean(locked);
  return {
    mainLabel: MAIN_LABELS[s] || "Start",
    mainDisabled: lockedMain,
    mainTitle: lockedMain ? LOCKED_TITLE : "",
    resetDisabled: s === "none",
    rateDisabled: s === "none",
  };
}

export function init(ctx) {
  const { store, el, document: doc } = ctx;
  const backdrop = el("modal-backdrop");
  let active = null; // { modal, trigger, release }

  function fail(err) {
    showToast({ text: `${errorReason(err)}.`, kind: "error" }, doc);
  }

  function openModal(modal, trigger, focusEl) {
    if (active) closeModal();
    doc.dispatchEvent(new CustomEvent("modal-open"));
    backdrop.classList.remove("hidden");
    modal.classList.remove("hidden");
    const release = trapFocus(modal, () => closeModal());
    active = { modal, trigger, release };
    (focusEl || modal.querySelector(FOCUSABLE)).focus();
  }

  function closeModal() {
    if (!active) return;
    const { modal, trigger, release } = active;
    active = null;
    release();
    modal.classList.add("hidden");
    backdrop.classList.add("hidden");
    if (trigger && trigger.focus) trigger.focus();
  }

  if (backdrop) backdrop.addEventListener("click", closeModal);

  // --- Start / Pause / Resume (one button, label follows run state) ---
  const startModal = el("start-modal");
  const mainButton = el("btn-start");
  async function onMainClick(event) {
    const state = store.getState().run.state;
    if (state === "running") {
      try {
        await postJson("/api/run/pause", {});
        showToast({ text: "Line paused. In-progress lots are frozen." }, doc);
      } catch (err) {
        fail(err);
      }
      return;
    }
    if (state === "paused") {
      try {
        await postJson("/api/run/resume", {});
        showToast({ text: "Line resumed." }, doc);
      } catch (err) {
        fail(err);
      }
      return;
    }
    openModal(startModal, event.currentTarget, el("start-seed"));
  }
  if (mainButton) mainButton.addEventListener("click", onMainClick);
  const emptyStart = el("empty-start");
  if (emptyStart) emptyStart.addEventListener("click", (event) => openModal(startModal, event.currentTarget, el("start-seed")));
  const startClose = el("start-close");
  if (startClose) startClose.addEventListener("click", closeModal);

  const startRate = el("start-rate");
  const startRateValue = el("start-rate-value");
  if (startRate && startRateValue) {
    startRate.addEventListener("input", () => {
      startRateValue.textContent = `${Number(startRate.value)} pizzas/min`;
    });
  }

  const startForm = el("start-form");
  if (startForm) {
    startForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      const weights = {};
      for (const input of startForm.querySelectorAll(".weight-input")) {
        weights[input.dataset.product] = input.value;
      }
      const body = buildStartBody({ seed: el("start-seed").value, rate: el("start-rate").value, weights, randomBreakdowns: Boolean(el("start-random-breakdowns") && el("start-random-breakdowns").checked) });
      if (body.error) {
        showToast({ text: `${body.error}.`, kind: "error" }, doc);
        return;
      }
      try {
        const res = await postJson("/api/run/start", body);
        closeModal();
        const seed = res && res.seed !== undefined && res.seed !== null ? res.seed : body.seed;
        showToast({ text: `Run started. Seed ${seed === null || seed === undefined ? "random" : seed}.` }, doc);
      } catch (err) {
        fail(err);
      }
    });
  }

  // --- Reset ---
  const resetModal = el("reset-modal");
  const reset = el("btn-reset");
  if (reset) reset.addEventListener("click", (e) => openModal(resetModal, e.currentTarget, el("reset-cancel")));
  const cancel = el("reset-cancel");
  if (cancel) cancel.addEventListener("click", closeModal);
  const confirm = el("reset-confirm");
  if (confirm) {
    confirm.addEventListener("click", async () => {
      try {
        await postJson("/api/run/reset", {});
        closeModal();
        showToast({ text: "Run reset. No active run." }, doc);
      } catch (err) {
        fail(err);
      }
    });
  }

  // --- Rate slider (live, 300 ms trailing debounce) ---
  const slider = el("rate-slider");
  const rateValue = el("rate-value");
  if (slider) {
    const send = makeDebounced(async (value) => {
      try {
        await postJson("/api/rate", { arrival_rate_per_min: toLotsPerMin(value) });
      } catch (err) {
        fail(err);
      }
    }, 300);
    slider.addEventListener("input", () => {
      const value = Number(slider.value);
      if (rateValue) rateValue.textContent = `${value} pizzas/min`;
      send(value);
    });
  }

  // --- Rule switch: live FIFO/EDD/SPT, fact-derived active state (SCHED-03) ---
  const ruleSwitch = el("rule-switch");

  function renderRuleButtons() {
    if (!ruleSwitch) return;
    const state = store.getState();
    const vm = ruleViewModel({ active: state.rule, pending: rulePending.current, locked: isComparisonLocked(state) });
    for (const b of ruleSwitch.querySelectorAll("[data-rule]")) {
      const v = vm[b.dataset.rule];
      if (!v) continue;
      b.classList.toggle("active", v.active);
      b.disabled = v.disabled;
      b.title = v.title;
    }
  }

  const rulePending = createPendingAck({
    onTimeout: () => {
      showToast({ kind: "error", text: NO_ACK_TEXT }, doc);
      renderRuleButtons();
    },
  });

  if (ruleSwitch) {
    for (const b of ruleSwitch.querySelectorAll("[data-rule]")) {
      b.addEventListener("click", async () => {
        if (b.disabled) return;
        const rule = b.dataset.rule;
        rulePending.start(rule);
        renderRuleButtons();
        try {
          await postJson("/api/rule", { rule });
        } catch (err) {
          rulePending.clear();
          showToast({ kind: "error", text: RULE_ERROR_TEXT }, doc);
          renderRuleButtons();
        }
      });
    }
    store.onFact((fact) => {
      if (!fact || fact.kind !== "rule_changed") return;
      rulePending.clear();
      showToast({ text: ruleToast(fact.rule) }, doc);
      renderRuleButtons();
    });
  }

  // --- State-dependent enablement (hud.js owns labels; this owns the rule
  // switch and the slider; the slider also locks while a comparison runs) ---
  function sync() {
    const state = store.getState();
    const run = state.run;
    const locked = isComparisonLocked(state);
    const st = buttonState(run.state, locked);
    const lock = lockedControls(locked);
    if (mainButton) {
      mainButton.textContent = st.mainLabel;
      mainButton.disabled = st.mainDisabled;
      mainButton.title = st.mainTitle;
    }
    if (slider) {
      slider.disabled = st.rateDisabled || lock.rate.disabled;
      slider.title = lock.rate.disabled ? lock.rate.title : "";
    }
    if (slider && typeof run.arrival_rate_per_min === "number" && doc.activeElement !== slider) {
      slider.value = String(toPizzasPerMin(run.arrival_rate_per_min));
    }
    renderRuleButtons();
  }
  store.subscribe(sync);
  sync();
}
