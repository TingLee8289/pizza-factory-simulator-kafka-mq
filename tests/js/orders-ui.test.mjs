import test from "node:test";
import assert from "node:assert/strict";
import {
  buildOrderBody, toAwareIso, nextDueMode, estimateQuery, formatDueLocal, createPrefill,
} from "../../src/pizza/dashboard/static/js/orders-ui.js";

test("valid order with blank due omits due_ts", () => {
  assert.deepEqual(buildOrderBody({ product: "veggie", qty: "3", due: "" }), {
    product: "veggie", qty: 3, priority: 50,
  });
});

test("qty bounds and integer check", () => {
  for (const qty of ["0", "100", "1.5", "abc", ""]) {
    assert.ok(buildOrderBody({ product: "veggie", qty, due: ""}).error, qty);
  }
  assert.equal(buildOrderBody({ product: "veggie", qty: "99", due: ""}).qty, 99);
  assert.equal(buildOrderBody({ product: "veggie", qty: "1", due: ""}).qty, 1);
});

test("unknown product is rejected", () => {
  assert.ok(buildOrderBody({ product: "hawaiian", qty: 1, due: ""}).error);
});

test("due is converted to an aware ISO string preserving the instant", () => {
  const local = "2026-10-19T14:30";
  const iso = toAwareIso(local);
  assert.match(iso, /^2026-10-19T14:30:00[+-]\d{2}:\d{2}$/);
  assert.equal(new Date(iso).getTime(), new Date(local).getTime());
  const body = buildOrderBody({ product: "pepperoni", qty: 2, due: local, priority: 5 });
  assert.equal(body.due_ts, iso);
  assert.equal(body.priority, 5);
});

test("typed due uses the yyyy/MM/dd HH:mm placeholder format", () => {
  const iso = toAwareIso("2026/02/09 08:05");
  assert.match(iso, /^2026-02-09T08:05:00[+-]\d{2}:\d{2}$/);
  assert.equal(new Date(iso).getTime(), new Date(2026, 1, 9, 8, 5).getTime());
  const body = buildOrderBody({ product: "veggie", qty: 1, due: "2026/02/09 08:05"});
  assert.equal(body.due_ts, iso);
});

test("typed due rejects impossible dates and times", () => {
  assert.equal(toAwareIso("2026/02/31 08:05"), null);
  assert.equal(toAwareIso("2026/10/19 25:00"), null);
  assert.equal(toAwareIso("2026/10/19 14:75"), null);
  assert.ok(buildOrderBody({ product: "veggie", qty: 1, due: "2026/02/31"}).error);
});

test("invalid due is an error", () => {
  assert.ok(buildOrderBody({ product: "veggie", qty: 1, due: "garbage"}).error);
});

test("nextDueMode transitions", () => {
  assert.equal(nextDueMode("auto", "userInput"), "manual");
  assert.equal(nextDueMode("manual", "reset"), "auto");
  assert.equal(nextDueMode("manual", "cleared"), "auto");
  assert.equal(nextDueMode("auto", "estimateOk"), "auto");
  assert.equal(nextDueMode("loading", "estimateOk"), "auto");
  assert.equal(nextDueMode("loading", "estimateFail"), "unavailable");
  assert.equal(nextDueMode("manual", "estimateOk"), "manual");
  assert.equal(nextDueMode("manual", "estimateFail"), "manual");
  assert.equal(nextDueMode("unavailable", "userInput"), "manual");
  assert.equal(nextDueMode("auto", "bogus"), "auto");
});

test("estimateQuery encodes and validates", () => {
  assert.equal(
    estimateQuery({ product: "veggie", qty: "3", bufferK: "1.5", priority: 7 }),
    "/api/order/estimate?product=veggie&qty=3&buffer_k=1.5&priority=7",
  );
  assert.ok(estimateQuery({ product: "x", qty: 1, bufferK: 1}).error);
  assert.ok(estimateQuery({ product: "veggie", qty: 1.5, bufferK: 1}).error);
  assert.ok(estimateQuery({ product: "veggie", qty: 100, bufferK: 1}).error);
  for (const k of [0.4, 3.1, NaN, "abc"]) {
    assert.equal(estimateQuery({ product: "veggie", qty: 1, bufferK: k}).error, "Buffer must be between 0.5 and 3");
  }
});

test("formatDueLocal and seconds-aware toAwareIso round-trip", () => {
  const d = new Date(2026, 1, 9, 8, 5, 7);
  const s = formatDueLocal(d.toISOString());
  assert.equal(s, "2026-02-09 08:05:07");
  assert.equal(new Date(toAwareIso(s)).getTime(), d.getTime());
  assert.equal(toAwareIso("2026/02/09 08:05:61"), null);
});

test("buildOrderBody due modes", () => {
  const base = { product: "veggie", qty: 1, priority: 50 };
  const a = buildOrderBody({ ...base, due: "2026-02-09 08:05:07", bufferK: "1.2", dueMode: "auto" });
  assert.deepEqual(a, { ...base, buffer_k: 1.2 });
  const m = buildOrderBody({ ...base, due: "2026-02-09 08:05:07", bufferK: "1.2", dueMode: "manual" });
  assert.ok(m.due_ts);
  assert.equal("buffer_k" in m, false);
  assert.equal(buildOrderBody({ ...base, due: "", bufferK: 9, dueMode: "auto" }).error, "Buffer must be between 0.5 and 3");
  assert.deepEqual(buildOrderBody({ ...base, due: "", bufferK: 9 }), base);
});

function harness() {
  const timers = [];
  const fetches = [];
  const log = { states: [], dues: [], readouts: [] };
  const NOW = Date.parse("2026-10-21T10:00:00Z");
  const p = createPrefill({
    fetchEstimate: (params, signal) => new Promise((resolve, reject) => {
      fetches.push({ params, signal, resolve, reject });
    }),
    now: () => NOW,
    setTimer: (fn) => { timers.push({ fn, live: true }); return timers.length - 1; },
    clearTimer: (h) => { if (timers[h]) timers[h].live = false; },
    onState: (s) => log.states.push(s),
    onDue: (v) => log.dues.push(v),
    onReadout: (t) => log.readouts.push(t),
  });
  const fire = () => { for (const t of timers) if (t.live) { t.live = false; t.fn(); } };
  return { p, fetches, log, fire, NOW };
}
const reply = (NOW, extra = {}) => ({
  eta_ts: new Date(NOW + 125000).toISOString(),
  due_ts: new Date(NOW + 150000).toISOString(),
  degraded: false,
  ...extra,
});
const tick = () => new Promise((r) => setImmediate(r));

test("prefill debounces, aborts previous, ignores stale", async () => {
  const { p, fetches, log, fire, NOW } = harness();
  p.trigger({ n: 1 }); p.trigger({ n: 2 }); p.trigger({ n: 3 });
  fire();
  assert.equal(fetches.length, 1);
  assert.deepEqual(fetches[0].params, { n: 3 });
  p.trigger({ n: 4 });
  fire();
  assert.equal(fetches.length, 2);
  assert.equal(fetches[0].signal.aborted, true);
  fetches[0].resolve(reply(NOW));
  await tick();
  assert.equal(log.dues.length, 0);
  fetches[1].resolve(reply(NOW));
  await tick();
  assert.equal(log.dues.length, 1);
  assert.equal(log.dues[0], formatDueLocal(new Date(NOW + 150000).toISOString()));
  assert.match(log.readouts.at(-1), /^Estimated finish \d{2}:\d{2}:\d{2} \(in 02:05\)$/);
  assert.equal(p.getMode(), "auto");
});

test("prefill failure -> unavailable; degraded suffix; manual not overwritten", async () => {
  const h = harness();
  h.p.trigger({}); h.fire();
  h.fetches[0].reject(new Error("boom"));
  await tick();
  assert.equal(h.p.getMode(), "unavailable");
  assert.equal(h.log.states.at(-1), "unavailable");
  assert.equal(h.log.dues.length, 0);

  const g = harness();
  g.p.trigger({}); g.fire();
  g.fetches[0].resolve(reply(g.NOW, { degraded: true }));
  await tick();
  assert.match(g.log.readouts.at(-1), /\(a station is down; estimate may be early\)$/);

  const m = harness();
  m.p.trigger({}); m.fire();
  m.p.userInput();
  assert.equal(m.p.getMode(), "manual");
  m.fetches[0].resolve(reply(m.NOW));
  await tick();
  assert.equal(m.log.dues.length, 0);
  assert.equal(m.log.readouts.length, 1);
  assert.equal(m.p.getMode(), "manual");
});

test("reset and cleared return to auto and refetch", async () => {
  const h = harness();
  h.p.trigger({ n: 1 });
  h.p.userInput();
  h.p.reset();
  assert.equal(h.p.getMode(), "loading");
  h.fire();
  assert.equal(h.fetches.length, 1);
  h.fetches[0].resolve(reply(h.NOW));
  await tick();
  assert.equal(h.log.dues.length, 1);
  h.p.userInput();
  h.p.cleared();
  h.fire();
  assert.equal(h.fetches.length, 2);
  assert.equal(h.p.getMode(), "loading");
});

test("priority validation: integer 1..99 only", () => {
  const ok = (priority) => buildOrderBody({ product: "veggie", qty: 1, due: "", priority });
  assert.equal(ok("1").priority, 1);
  assert.equal(ok(99).priority, 99);
  for (const bad of [0, 100, 1.5, "abc", NaN, "", -3, true]) {
    assert.ok(ok(bad).error, String(bad));
  }
  assert.ok(estimateQuery({ product: "veggie", qty: 1, bufferK: 1, priority: 0 }).error);
  assert.match(estimateQuery({ product: "veggie", qty: 1, bufferK: 1 }), /priority=50$/);
});
