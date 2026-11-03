import test from "node:test";
import assert from "node:assert/strict";
import {
  SPEEDS, MAX_ANIM_DT, speedViewModel, animationDt, init,
} from "../../src/pizza/dashboard/static/js/speed.js";
import { createStore } from "../../src/pizza/dashboard/static/js/store.js";

test("speedViewModel lists six buttons and highlights only the active one", () => {
  const vm = speedViewModel({ active: 8 });
  assert.deepEqual(vm.map((b) => b.scale), [1, 2, 4, 8]);
  assert.deepEqual(vm.map((b) => b.label), ["1×", "2×", "4×", "8×"]);
  assert.deepEqual(vm.filter((b) => b.active).map((b) => b.scale), [8]);
});

test("unknown or missing active falls back to 1x", () => {
  for (const active of [undefined, null, 3, 0, -1, "x", NaN]) {
    const vm = speedViewModel({ active });
    assert.deepEqual(vm.filter((b) => b.active).map((b) => b.scale), [1], String(active));
  }
  assert.deepEqual(speedViewModel().filter((b) => b.active).map((b) => b.scale), [1]);
});

test("buttons are never disabled", () => {
  for (const active of SPEEDS) {
    assert.ok(speedViewModel({ active }).every((b) => b.disabled === false));
  }
});

test("animationDt multiplies by scale and caps a single frame", () => {
  assert.equal(animationDt(0.016, 1), 0.016);
  assert.ok(Math.abs(animationDt(0.016, 8) - 0.128) < 1e-12);
  assert.equal(animationDt(0.1, 16), MAX_ANIM_DT);
  assert.equal(animationDt(-1, 4), 0);
  assert.equal(animationDt(0.05, 0), 0.05);
  assert.equal(animationDt(0.05, NaN), 0.05);
});

test("store restores the scale from the first snapshot and follows ticks", () => {
  const store = createStore();
  assert.equal(store.getState().timeScale, 1);
  store.replace({ seq: 1, time_scale: 8, run: { state: "none", run_id: null } });
  assert.equal(store.getState().timeScale, 8);
  store.merge({ seq: 2, time_scale: 4 });
  assert.equal(store.getState().timeScale, 4);
  store.merge({ seq: 3 });
  assert.equal(store.getState().timeScale, 4);
  store.merge({ seq: 4, time_scale: "garbage" });
  assert.equal(store.getState().timeScale, 1);
  store.replace({ seq: 5, run: { state: "none", run_id: null } });
  assert.equal(store.getState().timeScale, 1);
});

// --- init wiring with a tiny DOM stub ---
function makeButton(scale) {
  const classes = new Set();
  const attrs = {};
  const listeners = {};
  return {
    dataset: { scale: String(scale) },
    disabled: true,
    title: "",
    classList: {
      toggle: (name, on) => (on ? classes.add(name) : classes.delete(name)),
      contains: (name) => classes.has(name),
    },
    setAttribute: (k, v) => { attrs[k] = v; },
    getAttribute: (k) => attrs[k],
    addEventListener: (type, fn) => { listeners[type] = fn; },
    click: () => listeners.click(),
  };
}

function setup() {
  const buttons = SPEEDS.map(makeButton);
  const host = { querySelectorAll: () => buttons };
  const store = createStore();
  init({ store, el: (id) => (id === "speed-switch" ? host : null) });
  return { buttons, store };
}

const activeScales = (buttons) => buttons.filter((b) => b.classList.contains("active")).map((b) => b.dataset.scale);

test("init highlights 1x, enables every button and mirrors store changes", () => {
  const { buttons, store } = setup();
  assert.deepEqual(activeScales(buttons), ["1"]);
  assert.ok(buttons.every((b) => b.disabled === false));
  store.replace({ seq: 1, time_scale: 4, run: { state: "running", run_id: "r" } });
  assert.deepEqual(activeScales(buttons), ["4"]);
  assert.equal(buttons.find((b) => b.dataset.scale === "4").getAttribute("aria-pressed"), "true");
});

test("click posts the scale and highlights on success", async () => {
  const calls = [];
  globalThis.fetch = async (path, init) => {
    calls.push({ path, body: init.body });
    return { ok: true, status: 200, json: async () => ({ scale: 8 }) };
  };
  const { buttons, store } = setup();
  await buttons.find((b) => b.dataset.scale === "8").click();
  await new Promise((r) => setTimeout(r, 0));
  assert.equal(calls[0].path, "/api/time_scale");
  assert.equal(calls[0].body, '{"scale":8}');
  assert.equal(store.getState().timeScale, 8);
  assert.deepEqual(activeScales(buttons), ["8"]);
});

test("failed click toasts and keeps the backend-reported highlight", async () => {
  globalThis.fetch = async () => ({ ok: false, status: 503, json: async () => ({ detail: "x" }) });
  const toasts = [];
  const host = { children: [], appendChild(el) { toasts.push(el.textContent); this.children.push(el); }, removeChild() {} };
  globalThis.document = {
    getElementById: (id) => (id === "toasts" ? host : null),
    createElement: () => ({
      className: "",
      textContent: "",
      children: [],
      setAttribute() {},
      appendChild(child) { this.textContent += child.textContent || ""; },
      addEventListener() {},
      parentNode: null,
    }),
  };
  const { buttons, store } = setup();
  store.replace({ seq: 1, time_scale: 4, run: { state: "none", run_id: null } });
  await buttons.find((b) => b.dataset.scale === "8").click();
  await new Promise((r) => setTimeout(r, 0));
  assert.equal(store.getState().timeScale, 4);
  assert.deepEqual(activeScales(buttons), ["4"]);
  assert.equal(toasts.length, 1);
  delete globalThis.document;
});
