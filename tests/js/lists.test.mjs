import test from "node:test";
import assert from "node:assert/strict";
import { stationRows, lotRows, statusChip, emptyCopy, EMPTY_LOTS, nextLotSort, lotSortDesc, holdOrder } from "../../src/pizza/dashboard/static/js/lists.js";
import { isHighPriority, HIGH_PRIORITY_MAX } from "../../src/pizza/dashboard/static/js/priority.js";
import { matchSearch } from "../../src/pizza/dashboard/static/js/search.js";
import { feedLine, pushFeed, FEED_CAP } from "../../src/pizza/dashboard/static/js/feed.js";

const lots = [
  { order_id: "ord-b", product: "veggie", qty: 1, state: "running", station: "bake", steps_done: 4, created_ts: "2026-01-01T10:00:02Z" },
  { order_id: "ord-a", product: "margherita", qty: 2, state: "queued", station: "spin", steps_done: 0, created_ts: "2026-01-01T10:00:01Z", priority: 5 },
  { order_id: "ord-z", product: "veggie", qty: 1, state: "done", station: null, steps_done: 7, created_ts: "2026-01-01T09:00:00Z" },
  { order_id: "ord-p", product: "veggie", qty: 1, state: "pre_release", station: null, steps_done: 0, created_ts: "2026-01-01T10:00:03Z" },
];
const state = (extra = {}) => ({
  selection: null,
  line: { stations: { bake: { status: "busy", queue: 4 }, spin: { status: "idle", queue: 3 } }, lots },
  kpi: { stations: { bake: { utilization: 0.8 } } },
  ...extra,
});

test("stationRows: route order, amber at 4 not 3, utilization from kpi only", () => {
  const rows = stationRows(state());
  assert.deepEqual(rows.map((r) => r.name), ["spin", "sauce", "cheese", "toppings", "bake", "slice", "package"]);
  assert.equal(rows[0].queueTone, "normal");
  assert.equal(rows[4].queueTone, "amber");
  assert.equal(rows[4].utilization, 0.8);
  assert.equal(rows[0].utilization, null);
  assert.equal(rows[1].status, "offline");
});

test("lotRows: excludes done, sorted, progress, pre-release label, selection", () => {
  const rows = lotRows(state({ selection: { kind: "lot", id: "ord-b" } }));
  assert.deepEqual(rows.map((r) => r.id), ["ord-a", "ord-b", "ord-p"]);
  assert.equal(rows[1].progress, 4 / 7);
  assert.equal(rows[1].selected, true);
  assert.equal(rows[0].priority, 5);
  assert.equal(rows[1].priority, 50);
  assert.equal(rows[2].station, "pre-release");
});

test("lotRows: empty", () => {
  assert.deepEqual(lotRows({ line: { lots: [] } }), []);
  assert.deepEqual(lotRows({}), []);
});

test("matchSearch: exact station precedence, substring, trim, case", () => {
  const s = { line: { lots: [{ order_id: "bake-lot-1" }, { order_id: "ord-9" }] } };
  assert.deepEqual(matchSearch("bake", s), { kind: "station", id: "bake" });
  assert.deepEqual(matchSearch("  ORD-9 ", s), { kind: "lot", id: "ord-9" });
  assert.deepEqual(matchSearch("bake-l", s), { kind: "lot", id: "bake-lot-1" });
  assert.deepEqual(matchSearch("top", s), { kind: "station", id: "toppings" });
  assert.deepEqual(matchSearch("ack", s), { kind: "station", id: "package" });
  assert.equal(matchSearch("", s), null);
  assert.equal(matchSearch("nothing", s), null);
});

test("matchSearch: regex metacharacters are literal", () => {
  const s = { line: { lots: [{ order_id: "ord-1" }] } };
  assert.equal(matchSearch(".*", s), null);
  assert.equal(matchSearch("(", s), null);
  assert.equal(matchSearch("[a-z]+", s), null);
});

test("feedLine formats and filters", () => {
  const f = { kind: "op_started", ts: "2026-01-01T10:00:05Z", station: "bake", order_id: "o-1" };
  assert.equal(feedLine(f), "10:00:05  bake  started  o-1");
  assert.equal(feedLine({ ...f, kind: "op_completed" }), "10:00:05  bake  completed  o-1");
  assert.match(feedLine({ kind: "station_status", ts: f.ts, station: "bake", status: "busy" }), /status busy/);
  assert.match(feedLine({ ...f, kind: "order_lifecycle" }), /  lot  /);
  assert.equal(feedLine({ ...f, kind: "other" }), "");
  assert.equal(feedLine(null), "");
});

test("statusChip maps statuses", () => {
  assert.deepEqual(statusChip("busy"), { text: "Busy", tone: "ok" });
  assert.deepEqual(statusChip("idle"), { text: "Idle", tone: "idle" });
  assert.deepEqual(statusChip("down"), { text: "Down", tone: "danger" });
  assert.deepEqual(statusChip("offline"), { text: "Offline", tone: "amber" });
  assert.deepEqual(statusChip("zzz"), { text: "Offline", tone: "amber" });
});

test("stationRows throughputText converts pizzas/s to pizzas/min, dash when absent", () => {
  const st = { line: { stations: {}, lots: [] }, kpi: { stations: { bake: { pizzas_per_s: 0.05 } } } };
  const rows = stationRows(st);
  assert.equal(rows.find((r) => r.name === "bake").throughputText, "3.0 pizzas/min");
  assert.equal(rows[0].throughputText, "—");
});

test("stationRows availability, retryWait, utilDown", () => {
  const st = {
    line: {
      stations: { bake: { status: "down", queue: 0 } },
      lots: [{ order_id: "r", state: "queued", station: "bake", visit: 2, retry_since_ts: "t", dispatched: true }],
    },
    kpi: { stations: { bake: { availability: 0.92 } } },
  };
  const rows = stationRows(st);
  const bake = rows.find((r) => r.name === "bake");
  assert.equal(bake.availText, "92%");
  assert.equal(bake.retryWait, true);
  st.line.lots = [{ order_id: "c", state: "queued", station: "bake", visit: 2, retry_since_ts: "t", cooling: true, dispatched: false }];
  assert.equal(stationRows(st).find((r) => r.name === "bake").retryWait, true);
  // WR-02: past the hold TTL the lot is back in the queue (not cooling, not dispatched)
  st.line.lots = [{ order_id: "q", state: "queued", station: "bake", visit: 2, retry_since_ts: "t", cooling: false, dispatched: false }];
  assert.equal(stationRows(st).find((r) => r.name === "bake").retryWait, true);
  st.line.lots = [{ order_id: "n", state: "queued", station: "bake", visit: 1, retry_since_ts: null, dispatched: false }];
  assert.equal(stationRows(st).find((r) => r.name === "bake").retryWait, false);
  assert.equal(bake.utilDown, true);
  assert.equal(rows[0].availText, "—");
  assert.equal(rows[0].availability, null);
});

test("lotRows filters and chips", () => {
  const L = [
    { order_id: "a", state: "queued", station: "spin", created_ts: "1" },
    { order_id: "b", state: "banked", station: "bake", created_ts: "2", bank: { reason: "capacity", source: "auto_down" } },
    { order_id: "c", state: "queued", station: "bake", created_ts: "3", stocker: { kind: "overflow", station: "bake" } },
    { order_id: "d", state: "queued", station: "bake", created_ts: "4", stocker: { kind: "station_down", station: "bake" } },
    { order_id: "e", state: "queued", station: "bake", created_ts: "5", stocker: { kind: "station_offline", station: "bake" } },
    { order_id: "f", state: "failed", station: "bake", created_ts: "6", failed: { station: "bake", visit: 3 } },
    { order_id: "g", state: "queued", station: "bake", created_ts: "7", visit: 2, retry_since_ts: "t", dispatched: true },
  ];
  const s = { line: { lots: L } };
  assert.equal(lotRows(s, "all").length, 7);
  assert.deepEqual(lotRows(s, "active").map((r) => r.id), ["a", "c", "d", "e", "g"]);
  assert.deepEqual(lotRows(s, "bank").map((r) => r.id), ["b"]);
  assert.deepEqual(lotRows(s, "failed").map((r) => r.id), ["f"]);
  const all = Object.fromEntries(lotRows(s, "all").map((r) => [r.id, r]));
  assert.deepEqual(all.b.chip, { text: "BANK", tone: "idle", dashed: true });
  assert.equal(all.b.sub, "auto");
  assert.equal(all.c.chip.text, "WAIT");
  assert.equal(all.d.chip.text, "WAIT");
  assert.equal(all.d.chip.tone, "amber");
  assert.equal(all.e.chip.text, "WAIT");
  assert.deepEqual(all.f.chip, { text: "SCRAP", tone: "danger", dashed: false });
  assert.equal(all.f.chip.tone, "danger");
  assert.equal(all.f.failStation, "bake");
  assert.equal(all.f.visit, 3);
  assert.equal(all.g.retryBadge, "R2");
});

test("emptyCopy", () => {
  assert.equal(emptyCopy("failed").heading, "No scrap lots");
  assert.equal(emptyCopy("failed").body, "Lots that exhaust their rework limit are scrapped; they appear here and in the dead-letter queue.");
  assert.equal(emptyCopy("bank").heading, "No banked lots");
  assert.equal(emptyCopy("all").heading, EMPTY_LOTS);
});

test("feed keeps last 5", () => {
  let lines = [];
  for (let i = 0; i < 8; i += 1) lines = pushFeed(lines, `l${i}`);
  assert.equal(lines.length, FEED_CAP);
  assert.deepEqual(lines, ["l3", "l4", "l5", "l6", "l7"]);
  assert.equal(pushFeed(lines, ""), lines);
});

test("isHighPriority: danger chip threshold is priority <= 10", () => {
  assert.equal(HIGH_PRIORITY_MAX, 10);
  assert.equal(isHighPriority(10), true);
  assert.equal(isHighPriority(11), false);
  assert.equal(isHighPriority(1), true);
});

test("stationRows WIP counts the queue plus the batch running at the station", () => {
  const st = {
    line: { stations: { bake: { status: "busy", queue: 2, current_lot: "L-1" }, spin: { status: "idle", queue: 0, current_lot: null } }, lots: [] },
    kpi: { stations: {} },
  };
  const rows = stationRows(st);
  const bake = rows.find((r) => r.name === "bake");
  const spin = rows.find((r) => r.name === "spin");
  assert.equal(bake.wip, 3);
  assert.equal(bake.queue, 2);
  assert.equal(spin.wip, 0);
});

const SORT_LOTS = [
  { order_id: "L10", product: "veggie", state: "running", station: "bake", steps_done: 4, created_ts: "1", priority: 30 },
  { order_id: "L9", product: "margherita", state: "queued", station: "spin", steps_done: 0, created_ts: "2", priority: 5 },
  { order_id: "L11", product: "pepperoni", state: "queued", station: "bake", steps_done: 4, created_ts: "3", stocker: { kind: "overflow" } },
  { order_id: "L2", product: "veggie", state: "pre_release", station: null, steps_done: 0, created_ts: "4" },
  { order_id: "L3", product: "margherita", state: "banked", station: "toppings", steps_done: 3, created_ts: "5", priority: 5 },
  { order_id: "L4", product: "pepperoni", state: "failed", station: "cheese", steps_done: 2, created_ts: "6", failed: { station: "cheese" } },
];
const sorted = (key, flipped = false) => lotRows({ line: { lots: SORT_LOTS } }, "all", key ? { key, flipped } : null).map((r) => r.id);

test("lotRows sorts by each column's first-click order with created_ts ties", () => {
  assert.deepEqual(sorted(null), ["L10", "L9", "L11", "L2", "L3", "L4"]);
  assert.deepEqual(sorted("id"), ["L2", "L3", "L4", "L9", "L10", "L11"]);
  assert.deepEqual(sorted("product"), ["L9", "L3", "L11", "L4", "L10", "L2"]);
  assert.deepEqual(sorted("priority"), ["L9", "L3", "L10", "L11", "L2", "L4"]);
  assert.deepEqual(sorted("progress"), ["L10", "L11", "L3", "L4", "L9", "L2"]);
  assert.deepEqual(sorted("step"), ["L2", "L9", "L4", "L3", "L10", "L11"]);
  assert.deepEqual(sorted("status"), ["L10", "L9", "L11", "L2", "L3", "L4"]);
});

test("lotRows flipped sort reverses the column but keeps created_ts ties ascending", () => {
  assert.deepEqual(sorted("id", true), ["L11", "L10", "L9", "L4", "L3", "L2"]);
  assert.deepEqual(sorted("priority", true), ["L11", "L2", "L4", "L10", "L9", "L3"]);
  assert.deepEqual(sorted("progress", true), ["L9", "L2", "L4", "L3", "L10", "L11"]);
});

test("nextLotSort cycles first -> flipped -> default; lotSortDesc gives the arrow", () => {
  const a = nextLotSort(null, "priority");
  assert.deepEqual(a, { key: "priority", flipped: false });
  assert.equal(lotSortDesc(a), false);
  const b = nextLotSort(a, "priority");
  assert.deepEqual(b, { key: "priority", flipped: true });
  assert.equal(lotSortDesc(b), true);
  assert.equal(nextLotSort(b, "priority"), null);
  assert.deepEqual(nextLotSort(b, "id"), { key: "id", flipped: false });
  assert.equal(lotSortDesc({ key: "progress", flipped: false }), true);
  assert.equal(nextLotSort(null, "nope"), null);
  assert.equal(lotSortDesc(null), false);
});

test("holdOrder keeps the previous row order, appends new rows and drops gone ones", () => {
  const rows = ["c", "a", "d", "b"].map((id) => ({ id }));
  assert.deepEqual(holdOrder(rows, ["a", "b", "x", "c"]).map((r) => r.id), ["a", "b", "c", "d"]);
  assert.deepEqual(holdOrder(rows, []).map((r) => r.id), ["c", "a", "d", "b"]);
});
