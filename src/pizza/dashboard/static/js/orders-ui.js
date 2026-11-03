// Manual order popover (ORDER-03, D-07). Numeric priority 1..99 (Phase 07.2, D-14).
import { postJson, getJson, errorReason } from "./api.js";
import { showToast } from "./toast.js";
import { isComparisonLocked } from "./store.js";
import { parsePriority, PRIORITY_ERROR, PRIORITY_DEFAULT } from "./priority.js";

const LOCKED_TITLE = "Locked while a comparison is running";

const PRODUCTS = ["margherita", "pepperoni", "veggie"];
const MAX_QTY = 99;
const pad = (n) => String(n).padStart(2, "0");

// "2026/10/19 14:30" (typed) or "2026-10-19T14:30" (ISO local) -> "2026-10-19T14:30:00+08:00"
const DUE_RE = /^(\d{4})[/-](\d{1,2})[/-](\d{1,2})(?:[T ](\d{1,2}):(\d{2})(?::(\d{2}))?)?$/;

export const BUFFER_MIN = 0.5;
export const BUFFER_MAX = 3;
export const BUFFER_DEFAULT = 1.5;
export const DEBOUNCE_MS = 300;
const BUFFER_ERROR = "Buffer must be between 0.5 and 3";
const DEGRADED_SUFFIX = " (a station is down; estimate may be early)";

function parseBuffer(raw) {
  if (raw === "" || raw === null || raw === undefined) return null;
  const k = Number(raw);
  if (!Number.isFinite(k) || k < BUFFER_MIN || k > BUFFER_MAX) return null;
  return k;
}

export function nextDueMode(state, event) {
  switch (event) {
    case "userInput": return "manual";
    case "reset":
    case "cleared": return state === "manual" ? "auto" : state;
    case "fetchStart": return state === "manual" ? "manual" : "loading";
    case "estimateOk": return state === "manual" ? "manual" : "auto";
    case "estimateFail": return state === "manual" ? "manual" : "unavailable";
    default: return state;
  }
}

export function estimateQuery({ product, qty, bufferK, priority = PRIORITY_DEFAULT }) {
  if (!PRODUCTS.includes(product)) return { error: "Unknown product" };
  const q = Number(qty);
  if (qty === "" || !Number.isInteger(q) || q < 1 || q > MAX_QTY) {
    return { error: `Quantity must be a whole number from 1 to ${MAX_QTY}` };
  }
  const k = parseBuffer(bufferK);
  if (k === null) return { error: BUFFER_ERROR };
  const pr = parsePriority(priority);
  if (pr === null) return { error: PRIORITY_ERROR };
  return `/api/order/estimate?product=${encodeURIComponent(product)}&qty=${q}` +
    `&buffer_k=${encodeURIComponent(String(k))}&priority=${pr}`;
}

export function formatDueLocal(iso) {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "";
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ` +
    `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function formatReadout(reply, nowMs) {
  const eta = new Date(reply.eta_ts);
  if (Number.isNaN(eta.getTime())) return "";
  const clock = `${pad(eta.getHours())}:${pad(eta.getMinutes())}:${pad(eta.getSeconds())}`;
  const secs = Math.max(0, Math.round((eta.getTime() - nowMs) / 1000));
  const text = `Estimated finish ${clock} (in ${pad(Math.floor(secs / 60))}:${pad(secs % 60)})`;
  return reply.degraded ? text + DEGRADED_SUFFIX : text;
}

// Debounced, abortable estimate controller. All side effects are injected.
export function createPrefill({
  fetchEstimate, now = Date.now, setTimer, clearTimer, onState, onDue, onReadout,
  makeAbort = () => new AbortController(), debounceMs = DEBOUNCE_MS,
}) {
  let mode = "auto";
  let params = null;
  let timer = null;
  let ctl = null;
  let reqId = 0;

  const setMode = (event) => {
    mode = nextDueMode(mode, event);
    if (onState) onState(mode);
  };

  function abort() {
    if (timer !== null) { clearTimer(timer); timer = null; }
    if (ctl) { ctl.abort(); ctl = null; }
    reqId += 1;
    if (mode === "loading") setMode("estimateFail");
  }

  async function run() {
    timer = null;
    if (params === null) return;
    if (ctl) ctl.abort();
    const mine = ++reqId;
    ctl = makeAbort();
    const signal = ctl.signal;
    setMode("fetchStart");
    let reply;
    try {
      reply = await fetchEstimate(params, signal);
    } catch (err) {
      if (mine !== reqId || signal.aborted) return;
      setMode("estimateFail");
      return;
    }
    if (mine !== reqId || signal.aborted) return;
    const wasManual = mode === "manual";
    setMode("estimateOk");
    if (!wasManual && onDue) onDue(formatDueLocal(reply.due_ts));
    if (onReadout) onReadout(formatReadout(reply, now()));
  }

  function trigger(p, { immediate = false } = {}) {
    if (p !== undefined) params = p;
    if (timer !== null) { clearTimer(timer); timer = null; }
    if (ctl) { ctl.abort(); ctl = null; }
    reqId += 1;
    if (immediate) { run(); return; }
    timer = setTimer(run, debounceMs);
  }

  return {
    trigger,
    abort,
    getMode: () => mode,
    userInput() { setMode("userInput"); },
    reset() { setMode("reset"); trigger(undefined, { immediate: true }); },
    cleared() { setMode("cleared"); trigger(undefined, { immediate: true }); },
  };
}

export function toAwareIso(local) {
  const raw = typeof local === "string" ? local.trim() : "";
  if (raw === "") return null;
  let d;
  const m = DUE_RE.exec(raw);
  if (m) {
    const y = Number(m[1]);
    const mo = Number(m[2]);
    const day = Number(m[3]);
    const h = m[4] == null ? 0 : Number(m[4]);
    const mi = m[5] == null ? 0 : Number(m[5]);
    const s = m[6] == null ? 0 : Number(m[6]);
    d = new Date(y, mo - 1, day, h, mi, s, 0);
    const same =
      d.getFullYear() === y && d.getMonth() === mo - 1 && d.getDate() === day &&
      d.getHours() === h && d.getMinutes() === mi && d.getSeconds() === s;
    if (!same) return null;
  } else {
    d = new Date(raw);
    if (Number.isNaN(d.getTime())) return null;
  }
  const off = -d.getTimezoneOffset();
  const sign = off >= 0 ? "+" : "-";
  const abs = Math.abs(off);
  return (
    `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}` +
    `T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}` +
    `${sign}${pad(Math.floor(abs / 60))}:${pad(abs % 60)}`
  );
}

export function buildOrderBody({ product, qty, due, priority = PRIORITY_DEFAULT, bufferK, dueMode }) {
  if (!PRODUCTS.includes(product)) return { error: "Unknown product" };
  const q = Number(qty);
  if (!Number.isInteger(q) || q < 1 || q > MAX_QTY) return { error: `Quantity must be a whole number from 1 to ${MAX_QTY}` };
  const pr = parsePriority(priority);
  if (pr === null) return { error: PRIORITY_ERROR };
  const body = { product, qty: q, priority: pr };
  if (dueMode === "auto") {
    // PD-7: Auto submits no due_ts so the server freezes a fresh ETA at submit time.
    const k = parseBuffer(bufferK);
    if (k === null) return { error: BUFFER_ERROR };
    body.buffer_k = k;
    return body;
  }
  const raw = typeof due === "string" ? due.trim() : "";
  if (raw !== "") {
    const iso = toAwareIso(raw);
    if (iso === null) return { error: "Due date is not valid" };
    body.due_ts = iso;
  }
  return body;
}

export function init(ctx) {
  const { store, el, document: doc } = ctx;
  const btn = el("btn-order");
  const pop = el("order-popover");
  const form = el("order-form");
  if (!btn || !pop || !form) return;

  const FOCUSABLE = 'button:not([disabled]), input:not([disabled]), select:not([disabled])';
  let open = false;

  const dueEl = el("order-due");
  const badge = el("order-due-badge");
  const resetBtn = el("order-due-reset");
  const bufferEl = el("order-buffer");
  const helpEl = el("order-buffer-help");
  const readoutEl = el("order-eta-readout");
  const priorityEl = el("order-priority");
  const priorityValueEl = el("order-priority-value");
  const HELP_AUTO = "Due = now + estimate x buffer";
  const BADGES = {
    auto: ["Auto", "chip auto"],
    manual: ["Manual", "chip"],
    loading: ["Estimating...", "chip dashed"],
    unavailable: ["Auto unavailable", "chip warn"],
  };

  function renderState(mode) {
    if (badge) {
      const [text, cls] = BADGES[mode] || BADGES.auto;
      badge.textContent = text;
      badge.className = cls;
    }
    if (resetBtn) resetBtn.classList.toggle("hidden", mode !== "manual");
    if (helpEl) {
      helpEl.textContent = mode === "manual"
        ? "Buffer applies only in Auto"
        : mode === "unavailable"
          ? "Estimate unavailable. Leave blank to let the server set it, or enter a date."
          : HELP_AUTO;
    }
    if (mode === "unavailable" && readoutEl) readoutEl.textContent = "";
  }

  const prefill = createPrefill({
    fetchEstimate: (url, signal) => getJson(url, { signal }),
    now: Date.now,
    setTimer: (fn, ms) => setTimeout(fn, ms),
    clearTimer: (h) => clearTimeout(h),
    onState: renderState,
    onDue: (text) => { if (dueEl) dueEl.value = text; },
    onReadout: (text) => { if (readoutEl) readoutEl.textContent = text; },
  });

  function readEstimateParams() {
    return estimateQuery({
      product: el("order-product").value,
      qty: el("order-qty").value,
      bufferK: bufferEl ? bufferEl.value : BUFFER_DEFAULT,
      priority: priorityEl ? priorityEl.value : PRIORITY_DEFAULT,
    });
  }

  function requestEstimate({ immediate = false } = {}) {
    const q = readEstimateParams();
    if (q.error) {
      // Invalid inputs: do not call the server; the submit path will report the reason.
      prefill.abort();
      return;
    }
    prefill.trigger(q, { immediate });
  }

  for (const id of ["order-product", "order-qty"]) {
    const node = el(id);
    if (!node) continue;
    node.addEventListener("input", () => requestEstimate());
    node.addEventListener("change", () => requestEstimate());
  }
  if (priorityEl) {
    const onPriority = () => {
      if (priorityValueEl) priorityValueEl.textContent = String(Number(priorityEl.value));
      requestEstimate();
    };
    priorityEl.addEventListener("input", onPriority);
    priorityEl.addEventListener("change", onPriority);
  }
  if (bufferEl) {
    const onBuffer = () => { if (prefill.getMode() !== "manual") requestEstimate(); };
    bufferEl.addEventListener("input", onBuffer);
    bufferEl.addEventListener("change", onBuffer);
  }
  if (dueEl) {
    dueEl.addEventListener("input", () => {
      if (dueEl.value.trim() === "") {
        prefill.cleared();
      } else if (prefill.getMode() !== "manual") {
        prefill.userInput();
      }
    });
  }
  if (resetBtn) {
    resetBtn.addEventListener("click", () => {
      const q = readEstimateParams();
      if (q.error) return;
      prefill.trigger(q);
      prefill.reset();
    });
  }
  renderState("auto");

  function close(restoreFocus = true) {
    if (!open) return;
    open = false;
    prefill.abort();
    pop.classList.add("hidden");
    btn.setAttribute("aria-expanded", "false");
    if (restoreFocus) btn.focus();
  }

  function show() {
    // Keep the right edge inside the viewport.
    const win = doc.defaultView;
    const width = 320;
    const rect = btn.getBoundingClientRect();
    if (win) {
      const left = Math.max(12, Math.min(rect.right - width, win.innerWidth - width - 12));
      pop.style.left = `${left}px`;
      pop.style.right = "auto";
    }
    open = true;
    pop.classList.remove("hidden");
    btn.setAttribute("aria-expanded", "true");
    el("order-product").focus();
    // D-07: opening the form prefills Due and returns to Auto.
    const q = readEstimateParams();
    if (q.error) return;
    if (prefill.getMode() === "manual") {
      prefill.trigger(q);
      prefill.reset();
    } else {
      prefill.trigger(q, { immediate: true });
    }
  }

  btn.setAttribute("aria-haspopup", "dialog");
  btn.setAttribute("aria-expanded", "false");
  btn.addEventListener("click", (event) => {
    event.stopPropagation();
    if (open) close();
    else show();
  });

  doc.addEventListener("click", (event) => {
    if (open && !pop.contains(event.target) && event.target !== btn) close(false);
  });
  doc.addEventListener("modal-open", () => close(false));

  doc.addEventListener("keydown", (event) => {
    if (!open) return;
    if (event.key === "Escape") {
      event.preventDefault();
      event.stopPropagation(); // popover closes before the selection is cleared
      close();
      return;
    }
    if (event.key !== "Tab") return;
    const items = Array.from(pop.querySelectorAll(FOCUSABLE));
    if (items.length === 0) return;
    const idx = items.indexOf(doc.activeElement);
    if (event.shiftKey && idx <= 0) {
      event.preventDefault();
      items[items.length - 1].focus();
    } else if (!event.shiftKey && idx === items.length - 1) {
      event.preventDefault();
      items[0].focus();
    }
  }, true);

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const body = buildOrderBody({
      product: el("order-product").value,
      qty: el("order-qty").value,
      due: el("order-due").value,
      priority: priorityEl ? priorityEl.value : PRIORITY_DEFAULT,
      bufferK: bufferEl ? bufferEl.value : BUFFER_DEFAULT,
      dueMode: prefill.getMode() === "manual" ? "manual" : "auto",
    });
    const reject = (reason) => showToast({
      text: `Lot not placed. ${reason}. Fix the field and try again.`,
      kind: "error",
    }, doc);
    if (body.error) {
      reject(body.error);
      return;
    }
    try {
      const res = await postJson("/api/order", body);
      close();
      showToast({ text: `Lot ${res && res.order_id ? res.order_id : ""} placed.`.replace("  ", " ") }, doc);
    } catch (err) {
      reject(errorReason(err));
    }
  });

  function sync() {
    const state = store.getState();
    const none = state.run.state === "none";
    const locked = isComparisonLocked(state);
    btn.disabled = none || locked;
    btn.title = locked ? LOCKED_TITLE : none ? "Start a run first" : "";
    if (none || locked) close(false);
  }
  store.subscribe(sync);
  sync();
}
