import test from "node:test";
import assert from "node:assert/strict";
import { stepStartedTs, lotScale, rankOf, diffLots, createLots, zoneOf, lotVisual, stockerLabel, stockerTitle, scrapLabel } from "../../src/pizza/dashboard/static/js/lots3d.js";
import { partsFor } from "../../src/pizza/dashboard/static/js/lots-instancing.js";
import * as THREE from "../../src/pizza/dashboard/static/vendor/three.module.js";
import {
  createRegistry, buildStations, buildStocker, STATION_NAMES, stationSlot, truckSlot, stockerSlot,
  stationX, queueSlot, STOCKER_LABEL_OPTS, ENTRY_X, EXIT_X, LANE_OUT_Z, LANE_RET_Z, HOP_H, BELT_Y, STATION_GAP,
  COLORS, buildScrapZone, scrapSlot, scrapEntryX, scrapBeltY, routeToScrap,
  SCRAP_Z, SCRAP_Y, SCRAP_X0, SCRAP_RAMP_X0, SCRAP_X1, SCRAP_X2, SCRAP_HEAD_Y, SCRAP_W,
  SCRAP_APRON_X1, SCRAP_APRON_Z1, SCRAP_BIN_X, SCRAP_BIN_R, SCRAP_BIN_H, SCRAP_LABEL_OPTS,
  SCRAP_SPUR_Z0, SCRAP_SPUR_Z1, SCRAP_SPUR_W, SCRAP_SPUR_MAX_X,
} from "../../src/pizza/dashboard/static/js/scene-objects.js";

const lot = (id, state, station, steps_done = 0) => ({ order_id: id, product: "margherita", qty: 1, priority: 50, state, station, steps_done });

test("rankOf orders the line position and treats done as terminal", () => {
  assert.equal(rankOf(lot("a", "pre_release", null)), -1);
  assert.equal(rankOf(lot("a", "queued", "spin")), 0);
  assert.equal(rankOf(lot("a", "running", "spin")), 1);
  assert.equal(rankOf(lot("a", "queued", "sauce")), 2);
  assert.equal(rankOf(lot("a", "done", null)), Infinity);
});

test("diffLots spawns new, updates changed, exits done and removes missing", () => {
  const known = new Map([
    ["same", { exiting: false, snap: { state: "queued", station: "spin", steps_done: 0 } }],
    ["moved", { exiting: false, snap: { state: "queued", station: "spin", steps_done: 0 } }],
    ["fin", { exiting: false, snap: { state: "running", station: "package", steps_done: 6 } }],
    ["gone", { exiting: false, snap: null }],
  ]);
  const d = diffLots(known, [
    lot("same", "queued", "spin"),
    lot("moved", "running", "spin"),
    lot("fin", "done", null, 7),
    lot("new", "pre_release", null),
    lot("old-done", "done", null, 7),
  ]);
  assert.deepEqual(d.spawn.map((l) => l.order_id), ["new"]);
  assert.deepEqual(d.update.map((l) => l.order_id), ["moved"]);
  assert.deepEqual(d.exit.map((l) => l.order_id), ["fin"]);
  assert.deepEqual(d.remove, ["gone"]);
});

test("a lot with a pending exit animation is not removed by a snapshot that lacks it", () => {
  const known = new Map([["rider", { exiting: true, snap: null }], ["plain", { exiting: false, snap: null }]]);
  const d = diffLots(known, []);
  assert.deepEqual(d.remove, ["plain"]);
});

// ---- scene-level smoke test against the real vendored three ----
function fakeDocument() {
  const ctx2d = new Proxy({}, { get: () => () => {}, set: () => true });
  globalThis.document = {
    createElement: () => ({ width: 0, height: 0, getContext: () => ctx2d }),
  };
}

function setup() {
  fakeDocument();
  const reg = createRegistry();
  const scene = new THREE.Scene();
  const { handles } = buildStations(THREE, reg, STATION_NAMES);
  const selected = [];
  const store = { select: (s) => selected.push(s) };
  const lots = createLots(THREE, scene, handles, store, reg);
  return { lots, handles, scene, selected, reg };
}

test("facts move a lot through the line, ride to the truck and get pooled", () => {
  const { lots, handles } = setup();
  lots.applyFact({ kind: "order_lifecycle", state: "created", order_id: "A", product: "pepperoni", qty: 2 });
  lots.applyFact({ kind: "order_lifecycle", state: "released", order_id: "A", product: "pepperoni", qty: 2 });
  lots.applyFact({ kind: "op_started", station: "spin", order_id: "A", product: "pepperoni", qty: 2 });
  for (let i = 0; i < 60; i++) lots.update(1 / 30, false);
  const e = lots.entries.get("A");
  assert.ok(Math.abs(e.pos.x - stationSlot(0).x) < 0.1);
  assert.ok(handles[0].queue === 0);
  lots.applyFact({ kind: "op_completed", station: "spin", order_id: "A" });
  assert.equal(e.layers, 1);
  assert.equal(e.loc.kind, "queue");
  assert.equal(e.loc.i, 1);
  lots.applyFact({ kind: "op_completed", station: "package", order_id: "A" });
  assert.equal(e.exiting, true);
  // a snapshot without the lot must not remove it mid-ride
  lots.reconcile({ connection: "live", line: { lots: [] } });
  assert.ok(lots.entries.has("A"));
  for (let i = 0; i < 400 && lots.entries.has("A"); i++) lots.update(1 / 30, false);
  assert.equal(lots.entries.has("A"), false);
  assert.ok(Math.abs(truckSlot().x - 29.5) < 1e-9);
});

test("paused update does not move lots", () => {
  const { lots } = setup();
  lots.applyFact({ kind: "order_lifecycle", state: "created", order_id: "P", product: "veggie", qty: 1 });
  lots.update(1 / 30, true);
  const e = lots.entries.get("P");
  lots.applyFact({ kind: "op_started", station: "bake", order_id: "P" });
  const before = e.pos.x;
  for (let i = 0; i < 30; i++) lots.update(1 / 30, true);
  assert.equal(e.pos.x, before);
});

test("stale snapshot behind the event stream does not pull a lot back", () => {
  const { lots } = setup();
  lots.applyFact({ kind: "op_started", station: "sauce", order_id: "S", product: "veggie", qty: 1 });
  lots.applyFact({ kind: "op_completed", station: "sauce", order_id: "S" });
  const e = lots.entries.get("S");
  assert.equal(e.loc.i, 2);
  lots.reconcile({ connection: "live", line: { lots: [lot("S", "running", "sauce", 1)] } });
  assert.equal(e.loc.i, 2);
});

test("selection of a lot that disappears clears the selection through the store", () => {
  const { lots, selected } = setup();
  lots.applyFact({ kind: "order_lifecycle", state: "created", order_id: "Z", product: "veggie", qty: 1 });
  lots.update(1 / 30, false);
  lots.setSelection({ kind: "lot", id: "Z" });
  lots.update(1 / 30, false);
  lots.reconcile({ connection: "live", line: { lots: [] } });
  lots.update(1 / 30, false);
  assert.deepEqual(selected, [null]);
});

test("zoneOf maps lot states to scene zones", () => {
  assert.equal(zoneOf({ state: "queued", stocker: { kind: "overflow" } }), "stocker");
  assert.equal(zoneOf({ state: "banked" }), "stocker");
  assert.equal(zoneOf({ state: "failed" }), "scrap");
  assert.equal(zoneOf({ state: "queued" }), "queue");
  assert.equal(zoneOf({ state: "running" }), "run");
  assert.equal(zoneOf({ state: "pre_release" }), "gate");
  assert.equal(zoneOf({ state: "done" }), "exit");
});

test("lotVisual variants", () => {
  assert.deepEqual(lotVisual({ state: "queued", stocker: { kind: "overflow" } }), { opacity: 0.5, outline: "solid", tint: null });
  assert.deepEqual(lotVisual({ state: "queued", stocker: { kind: "station_down" } }), { opacity: 0.5, outline: "solid", tint: "amber" });
  assert.deepEqual(lotVisual({ state: "queued", stocker: { kind: "station_offline" } }), { opacity: 0.5, outline: "solid", tint: "amber" });
  assert.deepEqual(lotVisual({ state: "banked" }), { opacity: 0.5, outline: "dashed" });
  assert.deepEqual(
    lotVisual({ state: "queued", visit: 2, retry_since_ts: "t", dispatched: true }),
    { tint: "amber", badge: "R2" },
  );
  assert.deepEqual(
    lotVisual({ state: "queued", visit: 2, retry_since_ts: "t", cooling: true, dispatched: false }),
    { tint: "amber", badge: "R2" },
  );
  // WR-02: back in the queue after the hold TTL keeps the rework badge
  assert.deepEqual(
    lotVisual({ state: "queued", visit: 2, retry_since_ts: "t", cooling: false, dispatched: false }),
    { tint: "amber", badge: "R2" },
  );
  assert.deepEqual(lotVisual({ state: "queued" }), { opacity: 1, outline: null, tint: null, badge: null });
});

test("stockerLabel and stockerTitle", () => {
  assert.equal(stockerLabel({ stocker_overflow: 2, stocker_down: 1, bank: 4 }), "Stocker  WAIT 3 / BANK 4");
  assert.equal(stockerLabel({}), "Stocker - empty");
  assert.equal(
    stockerTitle({ stocker_overflow: 2, stocker_down: 1, bank: 4 }),
    "WAIT 3 counts in WIP (2 queue full, 1 station down/offline). BANK 4 does not.",
  );
});

test("diffLots flags zone changes even when state/station/steps are equal", () => {
  const known = new Map([
    ["a", { exiting: false, snap: { state: "queued", station: "bake", steps_done: 2, zone: "queue" } }],
    ["b", { exiting: false, snap: { state: "queued", station: "bake", steps_done: 2, zone: "stocker" } }],
    ["c", { exiting: false, snap: { state: "running", station: "bake", steps_done: 2, zone: "run" } }],
    ["d", { exiting: true, snap: { state: "queued", station: "bake", steps_done: 2, zone: "queue" } }],
  ]);
  const d = diffLots(known, [
    { ...lot("a", "queued", "bake", 2), stocker: { kind: "overflow", station: "bake" } },
    lot("b", "queued", "bake", 2),
    lot("c", "failed", "bake", 2),
    { ...lot("d", "queued", "bake", 2), stocker: { kind: "overflow" } },
  ]);
  assert.deepEqual(d.update.map((l) => l.order_id), ["a", "b", "c"]);
  assert.equal(rankOf(lot("x", "banked", "bake")), 8);
  assert.equal(rankOf(lot("x", "failed", null)), -1);
});

test("scrapSlot piles failed lots inside the can and is deterministic", () => {
  const a = stockerSlot("overflow", 0);
  assert.deepEqual(a, stockerSlot("overflow", 0));
  assert.ok(a.z < -3);
  assert.ok(stockerSlot("overflow", 1).x > a.x);
  const rows = new Set(["overflow", "station_down", "station_offline"].map((k) => stockerSlot(k, 0).z));
  assert.equal(rows.size, 3);

  assert.deepEqual(scrapSlot(0), scrapSlot(0));
  for (let k = 0; k < 8; k++) {
    const s = scrapSlot(k);
    assert.ok(Math.abs(s.x - SCRAP_BIN_X) < SCRAP_BIN_R);
    assert.ok(Math.abs(s.z - SCRAP_Z) < SCRAP_BIN_R);
    assert.ok(s.y > 0.5);
    assert.ok(s.y < SCRAP_BIN_H + 0.4);
  }
  // same layer, different slots
  assert.notEqual(scrapSlot(0).x, scrapSlot(1).x);
  assert.notEqual(scrapSlot(0).z, scrapSlot(2).z);
  // the second layer stacks on top instead of spreading out
  assert.ok(scrapSlot(4).y > scrapSlot(0).y);
  assert.deepEqual(scrapSlot(8), scrapSlot(7));
  assert.deepEqual(scrapSlot(99), scrapSlot(7));
});

test("scrap belt profile and entry points stay on the deck", () => {
  assert.equal(scrapBeltY(SCRAP_X0), SCRAP_Y);
  assert.equal(scrapBeltY(SCRAP_RAMP_X0), SCRAP_Y);
  assert.equal(scrapBeltY(SCRAP_X1), SCRAP_HEAD_Y);
  assert.equal(scrapBeltY(SCRAP_X2), SCRAP_HEAD_Y);
  const junctions = [];
  for (let i = 0; i < STATION_NAMES.length; i++) {
    junctions.push(scrapEntryX(i));
    assert.ok(scrapEntryX(i) >= SCRAP_X0 + SCRAP_SPUR_W / 2);
    // every station junction stays on the flat deck, clear of the incline and the can
    assert.ok(scrapEntryX(i) <= SCRAP_SPUR_MAX_X);
    assert.ok(scrapEntryX(i) < SCRAP_RAMP_X0);
  }
  // junctions are distinct so the spurs never overlap each other
  assert.equal(new Set(junctions).size, junctions.length);
  for (let a = 0; a < junctions.length; a++) {
    for (let b = a + 1; b < junctions.length; b++) {
      assert.ok(Math.abs(junctions[a] - junctions[b]) > SCRAP_SPUR_W + 0.2);
    }
  }
  // every junction sits at its own station x: uniform STATION_GAP spacing, no step-back
  for (let i = 0; i < junctions.length; i++) assert.equal(scrapEntryX(i), stationX(i));
  for (let i = 1; i < junctions.length; i++) {
    assert.ok(Math.abs(junctions[i] - junctions[i - 1] - STATION_GAP) < 1e-9);
  }
  // the flat deck reaches past the package junction, so no step-back is ever needed
  assert.ok(SCRAP_SPUR_MAX_X >= stationX(STATION_NAMES.length - 1));
  // the can sits east of the package station's junction
  assert.ok(SCRAP_BIN_X - SCRAP_BIN_R > scrapEntryX(STATION_NAMES.length - 1));
  assert.equal(scrapEntryX(-1), SCRAP_X0 + SCRAP_SPUR_W / 2);
  // the head hangs over the can mouth
  assert.ok(SCRAP_X1 >= SCRAP_BIN_X - SCRAP_BIN_R);
  assert.ok(SCRAP_X2 <= SCRAP_BIN_X + SCRAP_BIN_R);
});

test("scene applies stocker and scrap snapshots and clears the ghost outline on release", () => {
  const { lots } = setup();
  lots.reconcile({
    connection: "live",
    line: {
      lots: [
        { ...lot("W", "queued", "bake", 4), stocker: { kind: "station_down", station: "bake" } },
        { ...lot("B", "banked", "bake", 4), bank: { reason: "capacity", source: "manual" } },
        { ...lot("F", "failed", "bake", 4), failed: { station: "bake", visit: 3 } },
      ],
    },
  });
  assert.equal(lots.entries.get("W").loc.kind, "stocker");
  assert.equal(lots.entries.get("W").loc.i, "station_down");
  assert.equal(lots.entries.get("B").loc.i, "bank");
  assert.equal(lots.entries.get("F").loc.kind, "scrap");
  // a failed lot already in the first live snapshot lands in the can, it does not fly across
  assert.ok(!lots.entries.get("F").travel);
  for (let i = 0; i < 3; i++) lots.update(1 / 30, false);
  assert.equal(lots.entries.get("W").vis.outline, "solid");
  assert.equal(lots.stats().outlines, 2); // W (solid amber) + B (dashed)
  lots.reconcile({ connection: "live", line: { lots: [lot("W", "queued", "bake", 4)] } });
  lots.update(1 / 30, false);
  assert.equal(lots.entries.get("W").loc.kind, "queue");
  assert.equal(lots.entries.get("W").vis.outline, null);
  lots.update(1 / 30, false);
  assert.equal(lots.stats().outlines, 0); // W released, B and F left the snapshot
});

test("buildStocker renders warehouse shell with shelves inside the floor footprint", () => {
  const reg = createRegistry();
  const { group, setLabel } = buildStocker(THREE, reg);
  const meshes = group.children.filter((c) => c.isMesh);
  assert.ok(meshes.length > 2);
  const floor = meshes[0];
  const fx = floor.scale.x / 2;
  const fz = floor.scale.z / 2;
  for (const m of meshes) {
    assert.ok(m.position.x - m.scale.x / 2 >= floor.position.x - fx - 1e-6);
    assert.ok(m.position.x + m.scale.x / 2 <= floor.position.x + fx + 1e-6);
    assert.ok(m.position.z - m.scale.z / 2 >= floor.position.z - fz - 1e-6);
    assert.ok(m.position.z + m.scale.z / 2 <= floor.position.z + fz + 1e-6);
  }
  // every stockerSlot lot sits within the floor footprint (on a shelf)
  for (const kind of ["overflow", "station_down", "station_offline", "bank"]) {
    for (const k of [0, 11, 12, 35]) {
      const s = stockerSlot(kind, k);
      assert.ok(Math.abs(s.x - floor.position.x) < fx);
      assert.ok(Math.abs(s.z - floor.position.z) < fz);
    }
  }
  setLabel("Stocker  WAIT 1 / BANK 0", "t");
  assert.equal(group.userData.title, "t");
  assert.equal(group.userData.text, "Stocker  WAIT 1 / BANK 0");
});

// ---- stocker conveyor travel ----
function stockedLot() {
  const ctx = setup();
  const { lots } = ctx;
  const f = (kind, extra = {}) => lots.applyFact({ kind, order_id: "T", product: "margherita", qty: 1, ...extra });
  f("order_lifecycle", { state: "created" });
  f("order_lifecycle", { state: "released" });
  f("op_started", { station: "spin" });
  f("op_completed", { station: "spin" });
  f("op_started", { station: "sauce" });
  f("op_completed", { station: "sauce" });
  for (let i = 0; i < 120; i++) lots.update(1 / 30, false);
  return { ...ctx, f, e: lots.entries.get("T") };
}
const near = (p, x, z) => Math.hypot(p.x - x, p.z - z);

test("scrap route rides the station spur, crosses the deck, climbs the head and tips into the can", () => {
  const from = { x: stationX(2), y: SCRAP_Y, z: 0 };
  const r = routeToScrap(2, from, 0);
  assert.deepEqual(r[0], from);
  // spur top in front of the station, then down to the deck junction
  assert.equal(r[1].x, scrapEntryX(2));
  assert.equal(r[1].z, SCRAP_SPUR_Z0);
  assert.equal(r[2].x, scrapEntryX(2));
  assert.equal(r[2].z, SCRAP_Z);
  assert.equal(r[2].y, SCRAP_Y);
  // flat run then the climb to the head
  assert.ok(r.some((p) => p.x === SCRAP_RAMP_X0 && p.y === SCRAP_Y));
  assert.ok(r.some((p) => p.x === SCRAP_X1 && p.y === SCRAP_HEAD_Y));
  // discharge lip, then down into the can
  const lip = r[r.length - 2];
  assert.equal(lip.x, SCRAP_X2);
  assert.equal(lip.y, SCRAP_HEAD_Y);
  assert.deepEqual(r[r.length - 1], scrapSlot(0));
  assert.ok(r[r.length - 1].y < SCRAP_HEAD_Y);
  const mid = r.slice(1, r.length - 1);
  for (let j = 1; j < mid.length; j++) assert.ok(mid[j].x >= mid[j - 1].x - 1e-9);
  // the east stations ride their stepped-back spur and still end in the can
  const r6 = routeToScrap(6, { x: stationX(6), y: SCRAP_Y, z: 0 }, 5);
  assert.equal(r6[1].x, scrapEntryX(6));
  assert.equal(r6[1].z, SCRAP_SPUR_Z0);
  assert.ok(scrapEntryX(6) < SCRAP_RAMP_X0);
  assert.deepEqual(r6[r6.length - 1], scrapSlot(5));
  // a Stocker release skips the spur and enters at the west end
  const rs = routeToScrap(-1, { x: -24, y: SCRAP_Y, z: -13 }, 2);
  assert.equal(rs[1].x, SCRAP_X0 + SCRAP_SPUR_W / 2);
  assert.equal(rs[1].z, SCRAP_Z);
});

test("a stocked lot rides spur, to-Stocker lane and entry into the stocker slot", () => {
  const { lots, f, e } = stockedLot();
  assert.equal(e.loc.kind, "queue");
  f("lot_stocked", { station: "cheese", stock_kind: "overflow" });
  assert.ok(e.travel);
  let sawLane = false;
  for (let i = 0; i < 180; i++) {
    lots.update(1 / 30, false);
    if (near(e.pos, ENTRY_X, LANE_OUT_Z) < 0.6) sawLane = true;
  }
  assert.ok(sawLane);
  const slot = stockerSlot("overflow", 0);
  assert.ok(near(e.pos, slot.x, slot.z) < 0.15);
  assert.equal(e.travel, null);
});

test("a released lot rides the return lane over a hop and drops into its queue slot", () => {
  const { lots, f, e } = stockedLot();
  f("lot_stocked", { station: "cheese", stock_kind: "overflow" });
  for (let i = 0; i < 180; i++) lots.update(1 / 30, false);
  f("lot_unstocked", { station: "cheese" });
  assert.ok(e.travel);
  let sawExit = false;
  let sawHop = false;
  for (let i = 0; i < 180; i++) {
    lots.update(1 / 30, false);
    const p = e.pos;
    if (near(p, EXIT_X, LANE_RET_Z) < 0.6) sawExit = true;
    if (p.y > BELT_Y + 0.02 + HOP_H - 0.05) sawHop = true;
  }
  assert.ok(sawExit);
  assert.ok(sawHop);
  const slot = queueSlot(2, 0);
  assert.ok(near(e.pos, slot.x, slot.z) < 0.15);
  assert.equal(e.travel, null);
});

test("the first live snapshot places a stocked lot instantly without travel", () => {
  const { lots } = setup();
  const stocked = { ...lot("S", "queued", "cheese", 2), stocker: { kind: "overflow" } };
  lots.reconcile({ connection: "live", line: { lots: [stocked] } });
  const e = lots.entries.get("S");
  assert.ok(!e.travel);
  lots.update(1 / 30, false);
  const slot = stockerSlot("overflow", 0);
  assert.ok(near(e.pos, slot.x, slot.z) < 1e-9);
});

test("normal moves never create a travel", () => {
  const { lots } = setup();
  const f = (kind, extra = {}) => lots.applyFact({ kind, order_id: "N", product: "margherita", qty: 1, ...extra });
  f("order_lifecycle", { state: "created" });
  f("order_lifecycle", { state: "released" });
  const steps = [["op_started", "spin"], ["op_completed", "spin"], ["op_started", "sauce"], ["op_completed", "sauce"]];
  for (const [kind, station] of steps) {
    f(kind, { station });
    const e = lots.entries.get("N");
    for (let i = 0; i < 20; i++) { assert.ok(!e.travel); lots.update(1 / 30, false); }
  }
  f("op_completed", { station: "package" });
  const e = lots.entries.get("N");
  for (let i = 0; i < 20; i++) { assert.ok(!e.travel); lots.update(1 / 30, false); }
});

test("paused update does not advance a lot that is mid-route", () => {
  const { lots, f, e } = stockedLot();
  f("lot_stocked", { station: "cheese", stock_kind: "overflow" });
  for (let i = 0; i < 10; i++) lots.update(1 / 30, false);
  const before = { ...e.pos };
  for (let i = 0; i < 30; i++) lots.update(1 / 30, true);
  assert.deepEqual({ ...e.pos }, before);
});

test("stocker label text fits the label canvas", () => {
  const { maxChars, fontPx } = STOCKER_LABEL_OPTS;
  const texts = [
    stockerLabel({}),
    stockerLabel({ stocker_overflow: 5, bank: 0 }),
    stockerLabel({ stocker_overflow: 99, stocker_down: 99, bank: 99 }),
  ];
  for (const t of texts) {
    assert.ok(t.length <= maxChars, `${t} exceeds maxChars`);
    // average glyph width of a 600 weight sans font is about 0.55 em; usable canvas width is 240 px
    assert.ok(t.length * fontPx * 0.55 <= 240, `${t} is wider than the canvas`);
  }
});

// ---- scrap conveyor travel ----
test("a failed lot rides the scrap conveyor into the can and stays there", () => {
  const { lots } = setup();
  const f = (kind, extra = {}) => lots.applyFact({ kind, order_id: "D", product: "margherita", qty: 1, ...extra });
  f("order_lifecycle", { state: "created" });
  f("order_lifecycle", { state: "released" });
  f("op_started", { station: "cheese" });
  for (let i = 0; i < 60; i++) lots.update(1 / 30, false);
  f("order_lifecycle", { state: "failed", station: "cheese" });
  const e = lots.entries.get("D");
  assert.equal(e.loc.kind, "scrap");
  assert.ok(e.travel);
  let sawDeck = false;
  for (let i = 0; i < 400; i++) {
    lots.update(1 / 30, false);
    const p = e.pos;
    if (Math.abs(p.z - SCRAP_Z) < 0.06 && Math.abs(p.y - SCRAP_Y) < 0.06) sawDeck = true;
  }
  assert.ok(sawDeck);
  assert.equal(e.travel, null);
  const slot = scrapSlot(0);
  assert.ok(near(e.pos, slot.x, slot.z) < 0.2);
  assert.ok(e.pos.y > 0.5);
  // it stays in the can as a visible scrap pile instead of leaving for the truck
  for (let i = 0; i < 60; i++) lots.update(1 / 30, false);
  assert.equal(lots.entries.has("D"), true);
  assert.equal(e.exiting, false);
});

test("paused update freezes a lot riding to the trash can", () => {
  const { lots } = setup();
  const f = (kind, extra = {}) => lots.applyFact({ kind, order_id: "Q", product: "margherita", qty: 1, ...extra });
  f("order_lifecycle", { state: "created" });
  f("order_lifecycle", { state: "released" });
  f("op_started", { station: "spin" });
  for (let i = 0; i < 20; i++) lots.update(1 / 30, false);
  f("order_lifecycle", { state: "failed", station: "spin" });
  const e = lots.entries.get("Q");
  for (let i = 0; i < 10; i++) lots.update(1 / 30, false);
  const before = { ...e.pos };
  for (let i = 0; i < 30; i++) lots.update(1 / 30, true);
  assert.deepEqual({ ...e.pos }, before);
});

test("buildScrapZone renders the scrap conveyor, the station spurs and the can", () => {
  fakeDocument();
  const { group, setLabel } = buildScrapZone(THREE, createRegistry());
  assert.ok(group.children.filter((c) => c.isMesh).length >= 8);
  const allowed = new Set(Object.values(COLORS));
  group.traverse((o) => {
    if (o.isMesh) assert.ok(allowed.has(o.material.color.getHex()));
  });
  // one spur deck per station, each parked on its junction x and clear of the deck frame
  for (let i = 0; i < STATION_NAMES.length; i++) {
    const x = scrapEntryX(i);
    const spur = group.children.filter(
      (c) => c.isMesh && Math.abs(c.position.x - x) < 1e-6 && c.position.z < SCRAP_Z - 1.8,
    );
    assert.ok(spur.length >= 3, `station ${i} is missing its spur`);
    for (const m of spur) assert.ok(m.position.z + m.scale.z / 2 <= SCRAP_Z - (SCRAP_W + 0.3) / 2 + 1e-6);
  }
  // the floor (apron + spur ground strips) is deliberately not red; red stays on the structure
  for (const m of group.children) {
    if (m.isMesh && m.position.y <= 0.06) assert.notEqual(m.material.color.getHex(), COLORS.lightDown);
  }
  // the can sits inside the neutral floor apron, east of the package station
  assert.ok(SCRAP_BIN_X + SCRAP_BIN_R <= SCRAP_APRON_X1);
  assert.ok(SCRAP_Z + SCRAP_BIN_R <= SCRAP_APRON_Z1);
  setLabel("SCRAP / DLQ 3");
  assert.equal(group.userData.text, "SCRAP / DLQ 3");
  setLabel("SCRAP / DLQ 3"); // unchanged text is a no-op, not an error
  assert.equal(group.userData.text, "SCRAP / DLQ 3");
});

test("scrap label text fits the label canvas", () => {
  assert.equal(scrapLabel({}), "SCRAPPED 0 lots / 0 pizzas");
  assert.equal(scrapLabel({ failed: 3, failed_pizzas: 7 }), "SCRAPPED 3 lots / 7 pizzas");
  const { maxChars, fontPx } = SCRAP_LABEL_OPTS;
  const texts = [scrapLabel({}), scrapLabel({ failed: 7 }), scrapLabel({ failed: 99, failed_pizzas: 999 })];
  for (const t of texts) {
    assert.ok(t.length <= maxChars, `${t} exceeds maxChars`);
    assert.ok(t.length * fontPx * 0.55 <= 240, `${t} is wider than the canvas`);
  }
});

test("a return trip in flight rides on to its station when op_started arrives mid-route", () => {
  const { lots } = setup();
  lots.applyFact({ kind: "order_lifecycle", state: "created", order_id: "R", product: "margherita", qty: 1 });
  lots.applyFact({ kind: "order_lifecycle", state: "released", order_id: "R", product: "margherita", qty: 1 });
  lots.applyFact({ kind: "op_started", station: "spin", order_id: "R", product: "margherita", qty: 1 });
  for (let i = 0; i < 60; i++) lots.update(1 / 30, false);
  const e = lots.entries.get("R");
  lots.applyFact({ kind: "lot_stocked", station: "spin", order_id: "R", stock_kind: "station_down" });
  for (let i = 0; i < 120; i++) lots.update(1 / 30, false);
  assert.equal(e.loc.kind, "stocker");
  lots.applyFact({ kind: "lot_unstocked", station: "spin", order_id: "R" });
  assert.equal(e.returning, true);
  assert.ok(e.travel !== null);
  // op_dispatched lands the lot in the queue slot first, then op_started arrives while it is still on the return lane
  lots.applyFact({ kind: "op_started", station: "spin", order_id: "R", product: "margherita", qty: 1 });
  assert.equal(e.loc.kind, "run");
  assert.equal(e.loc.i, 0);
  assert.ok(e.travel !== null, "return trip must not be cancelled by op_started");
  assert.equal(e.returning, true);
  for (let i = 0; i < 600 && e.travel; i++) lots.update(1 / 30, false);
  assert.equal(e.travel, null);
  assert.equal(e.returning, false);
  for (let i = 0; i < 200; i++) lots.update(1 / 30, false);
  assert.ok(Math.abs(e.pos.x - stationSlot(0).x) < 0.1);
});

test("lotScale enlarges only high-priority lots (priority <= 10)", () => {
  assert.equal(lotScale(10), 1.18 * 1.4);
  assert.equal(lotScale(1), 1.18 * 1.4);
  assert.equal(lotScale(11), 1.4);
  assert.equal(lotScale(50), 1.4);
});

test("stepStartedTs prefers the summary field and falls back to legacy steps", () => {
  assert.equal(stepStartedTs({ step_started_ts: "A", station: "spin", steps: { spin: { started_ts: "B" } } }), "A");
  assert.equal(stepStartedTs({ station: "spin", steps: { spin: { started_ts: "B" } } }), "B");
  assert.equal(stepStartedTs({ station: "spin" }), null);
  assert.equal(stepStartedTs(null), null);
});

test("300 spawned lots stay within a bounded pool count and one instance per part", () => {
  const { lots, scene } = setup();
  const list = [];
  for (let i = 0; i < 300; i++) list.push({ ...lot(`L${i}`, "queued", STATION_NAMES[i % 7], i % 8), qty: 1 + (i % 4) });
  lots.reconcile({ connection: "live", line: { lots: list } });
  for (let i = 0; i < 5; i++) lots.update(1 / 30, false);
  const st = lots.stats();
  assert.ok(st.pools <= 60, `pools ${st.pools}`);
  let expected = 0;
  for (const e of lots.entries.values()) expected += partsFor(e).length;
  assert.equal(st.instances, expected);
  let meshes = 0;
  scene.traverse((o) => { if (o.isInstancedMesh) meshes++; });
  assert.equal(meshes, st.pools);
});

test("a settled scene uploads nothing", () => {
  const { lots } = setup();
  lots.reconcile({ connection: "live", line: { lots: [lot("S1", "queued", "spin", 0), lot("S2", "running", "bake", 4)] } });
  for (let i = 0; i < 400; i++) lots.update(1 / 30, false);
  const before = lots.stats().dirtyUploads;
  assert.ok(before > 0);
  for (let i = 0; i < 20; i++) lots.update(1 / 30, false);
  assert.equal(lots.stats().dirtyUploads, before);
});

test("pick returns the lot under the ray, or a station, nearest first", () => {
  const { lots, handles } = setup();
  lots.reconcile({
    connection: "live",
    line: { lots: [lot("PA", "queued", "spin", 0), lot("PB", "queued", "bake", 0)] },
  });
  for (let i = 0; i < 400; i++) lots.update(1 / 30, false);
  const cam = new THREE.PerspectiveCamera(40, 2, 0.1, 500);
  cam.position.set(0, 60, 0.01);
  cam.lookAt(0, 0, 0);
  cam.updateMatrixWorld(true);
  cam.updateProjectionMatrix();
  const canvas = { getBoundingClientRect: () => ({ left: 0, top: 0, width: 1000, height: 500 }) };
  const project = (x, y, z) => {
    const v = new THREE.Vector3(x, y, z).project(cam);
    return [(v.x + 1) / 2 * 1000, (1 - v.y) / 2 * 500];
  };
  for (const id of ["PA", "PB"]) {
    const e = lots.entries.get(id);
    const [cx, cy] = project(e.pos.x, e.pos.y + 0.2, e.pos.z);
    assert.deepEqual(lots.pick(cx, cy, canvas, cam), { kind: "lot", id });
  }
  // empty ground far from any lot or station
  const [ex, ey] = project(0, 0, 40);
  assert.equal(lots.pick(ex, ey, canvas, cam), null);
  // a station body away from the lots resolves to the station
  handles[3].group.updateMatrixWorld(true);
  const [sx, sy] = project(handles[3].group.position.x, 1.5, handles[3].group.position.z);
  const hit = lots.pick(sx, sy, canvas, cam);
  assert.ok(hit && hit.kind !== "lot");
});
