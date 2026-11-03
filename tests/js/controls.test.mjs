import test, { mock } from "node:test";
import assert from "node:assert/strict";
import {
  buildStartBody, makeDebounced, nextFocusIndex, trapFocus, buttonState,
  ruleViewModel, ruleToast, ruleFeedLine, lockedControls, LOCKED_TITLE,
  createPendingAck, RULE_ACK_MS, NO_ACK_TEXT, RULE_ERROR_TEXT,
} from "../../src/pizza/dashboard/static/js/controls.js";

const ok = { seed: "", rate: "3", weights: { margherita: "1", pepperoni: "2", veggie: "0" } };

test("buildStartBody coerces and maps blank seed to null", () => {
  assert.deepEqual(buildStartBody(ok), {
    seed: null, arrival_rate_per_min: 1.5, weights: { margherita: 1, pepperoni: 2, veggie: 0 },
  });
  assert.equal(buildStartBody({ ...ok, seed: "42" }).seed, 42);
});

test("buildStartBody sends random_breakdowns only when true", () => {
  assert.equal(buildStartBody({ ...ok, randomBreakdowns: true }).random_breakdowns, true);
  assert.equal("random_breakdowns" in buildStartBody({ ...ok, randomBreakdowns: false }), false);
});

test("buildStartBody rejects invalid input", () => {
  assert.ok(buildStartBody({ ...ok, rate: "0" }).error);
  assert.ok(buildStartBody({ ...ok, rate: "13" }).error);
  assert.ok(buildStartBody({ ...ok, rate: "2.5" }).error);
  assert.ok(buildStartBody({ ...ok, rate: "abc" }).error);
  assert.ok(buildStartBody({ ...ok, seed: "1.5" }).error);
  assert.ok(buildStartBody({ ...ok, weights: { ...ok.weights, veggie: "-1" } }).error);
  assert.ok(buildStartBody({ ...ok, weights: { margherita: "0", pepperoni: "0", veggie: "0" } }).error);
});

test("makeDebounced fires once with the latest value", () => {
  mock.timers.enable({ apis: ["setTimeout"] });
  const calls = [];
  const d = makeDebounced((v) => calls.push(v), 300);
  d(1); mock.timers.tick(200); d(2); mock.timers.tick(200);
  assert.deepEqual(calls, []);
  mock.timers.tick(100);
  assert.deepEqual(calls, [2]);
  mock.timers.reset();
});

test("nextFocusIndex wraps both ways", () => {
  assert.equal(nextFocusIndex(3, 2, false), 0);
  assert.equal(nextFocusIndex(3, 0, true), 2);
  assert.equal(nextFocusIndex(3, 1, false), 2);
  assert.equal(nextFocusIndex(0, -1, false), -1);
});

test("trapFocus cycles Tab and closes on Esc", () => {
  const focused = [];
  const mk = (n) => ({ focus: () => { focused.push(n); doc.activeElement = items[n]; } });
  const items = [mk(0), mk(1), mk(2)];
  const doc = { activeElement: items[2] };
  let handler = null;
  const container = {
    ownerDocument: doc,
    querySelectorAll: () => items,
    addEventListener: (_t, h) => { handler = h; },
    removeEventListener: () => { handler = null; },
  };
  let closed = 0;
  const release = trapFocus(container, () => { closed += 1; });
  let prevented = 0;
  handler({ key: "Tab", shiftKey: false, preventDefault: () => { prevented += 1; } });
  assert.deepEqual(focused, [0]);
  handler({ key: "Tab", shiftKey: true, preventDefault: () => { prevented += 1; } });
  assert.deepEqual(focused, [0, 2]);
  handler({ key: "Escape", preventDefault: () => {} });
  assert.equal(closed, 1);
  assert.equal(prevented, 2);
  release();
  assert.equal(handler, null);
});

test("buttonState machine: one main button labelled Start/Pause/Resume", () => {
  assert.deepEqual(buttonState("none"), {
    mainLabel: "Start", mainDisabled: false, mainTitle: "", resetDisabled: true, rateDisabled: true,
  });
  assert.equal(buttonState("running").mainLabel, "Pause");
  assert.equal(buttonState("running").mainDisabled, false);
  assert.equal(buttonState("paused").mainLabel, "Resume");
  assert.equal(buttonState("paused").mainDisabled, false);
  assert.equal(buttonState("running").resetDisabled, false);
});

test("buttonState locks the main button while a comparison runs (not in none)", () => {
  assert.equal(buttonState("running", true).mainDisabled, true);
  assert.equal(buttonState("running", true).mainTitle, LOCKED_TITLE);
  assert.equal(buttonState("paused", true).mainDisabled, true);
  assert.equal(buttonState("none", true).mainDisabled, false);
});

test("ruleViewModel marks active/pending/enabled per segment", () => {
  const vm = ruleViewModel({ active: "edd", pending: "spt", locked: false });
  assert.equal(vm.edd.active, true);
  assert.equal(vm.edd.disabled, false);
  assert.equal(vm.spt.pending, true);
  assert.equal(vm.spt.disabled, true);
  assert.equal(vm.fifo.active, false);
  assert.equal(vm.fifo.disabled, false);
});

test("ruleViewModel locked disables all three, keeps active accent and sets the lock title", () => {
  const vm = ruleViewModel({ active: "edd", pending: null, locked: true });
  assert.equal(vm.fifo.disabled, true);
  assert.equal(vm.edd.disabled, true);
  assert.equal(vm.spt.disabled, true);
  assert.equal(vm.edd.active, true);
  assert.equal(vm.fifo.title, LOCKED_TITLE);
  assert.equal(vm.edd.title, LOCKED_TITLE);
});

test("ruleToast and ruleFeedLine copy exactly", () => {
  assert.equal(ruleToast("spt"), "Dispatch rule is now SPT. Applies to the next release.");
  assert.equal(ruleFeedLine("edd"), "RULE EDD");
});

test("createPendingAck resolves to the no-ack error after 3000 ms using an injectable clock", () => {
  const scheduled = [];
  const clock = {
    setTimeout: (fn, ms) => { const id = scheduled.length; scheduled.push({ fn, ms, id }); return id; },
    clearTimeout: (id) => { const s = scheduled.find((x) => x.id === id); if (s) s.cleared = true; },
  };
  let timedOutWith = null;
  const ack = createPendingAck({ ms: RULE_ACK_MS, onTimeout: (key) => { timedOutWith = key; }, clock });
  ack.start("edd");
  assert.equal(ack.current, "edd");
  assert.equal(scheduled.length, 1);
  assert.equal(scheduled[0].ms, RULE_ACK_MS);
  scheduled[0].fn();
  assert.equal(timedOutWith, "edd");
  assert.equal(ack.current, null);
});

test("createPendingAck.clear cancels the pending timer before it fires", () => {
  const scheduled = [];
  const clock = {
    setTimeout: (fn, ms) => { scheduled.push({ fn, ms, cleared: false }); return scheduled.length - 1; },
    clearTimeout: (id) => { scheduled[id].cleared = true; },
  };
  const fire = (id) => { if (!scheduled[id].cleared) scheduled[id].fn(); };
  let fired = false;
  const ack = createPendingAck({ onTimeout: () => { fired = true; }, clock });
  ack.start("spt");
  ack.clear();
  assert.equal(ack.current, null);
  assert.equal(scheduled[0].cleared, true);
  fire(0);
  assert.equal(fired, false);
});

test("error copy for no-ack and REST failure match the UI-SPEC exactly", () => {
  assert.equal(NO_ACK_TEXT, "No response from the scheduler. The rule may not have changed - check the rule switch.");
  assert.equal(RULE_ERROR_TEXT, "Could not change the dispatch rule. Check that RabbitMQ is running and try again.");
});

test("lockedControls disables reset/stop/order/rate with the lock title, and is a no-op when unlocked", () => {
  const locked = lockedControls(true);
  for (const key of ["reset", "stop", "order", "rate"]) {
    assert.equal(locked[key].disabled, true);
    assert.equal(locked[key].title, LOCKED_TITLE);
  }
  const unlocked = lockedControls(false);
  for (const key of ["reset", "stop", "order", "rate"]) {
    assert.equal(unlocked[key].disabled, false);
    // combining via `||` with any existing run-state-based disabled value
    // must leave that value untouched when unlocked.
    assert.equal(true || unlocked[key].disabled, true);
    assert.equal(false || unlocked[key].disabled, false);
  }
});
