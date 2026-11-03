import test from "node:test";
import assert from "node:assert/strict";
import * as THREE from "../../src/pizza/dashboard/static/vendor/three.module.js";
import {
  createRegistry, buildLanes, stationX, stockerSlot, COLORS, BELT_Y,
  spurX, dropX, ENTRY_X, EXIT_X, LANE_OUT_Z, LANE_RET_Z, HOP_H, HOP_HALF,
  routeToStocker, routeFromStocker, isStockerReturn, pathLength, pointAt, travelDuration, easeInOut,
} from "../../src/pizza/dashboard/static/js/scene-objects.js";

const from = { x: -5, y: BELT_Y + 0.02, z: 0 };

test("spur and drop x follow the station x and ports lie inside the Stocker", () => {
  for (let i = 0; i < 7; i++) {
    assert.equal(spurX(i), stationX(i) - 3.6);
    assert.equal(dropX(i), stationX(i) - 2.4);
  }
  assert.equal(ENTRY_X, spurX(0));
  assert.ok(EXIT_X < ENTRY_X);
  const lo = stockerSlot("overflow", 0).x - 0.5;
  const hi = stockerSlot("overflow", 11).x + 0.5;
  assert.ok(ENTRY_X > lo && ENTRY_X < hi);
  assert.ok(EXIT_X > lo && EXIT_X < hi);
});

test("routeToStocker goes spur, to-Stocker lane, entry", () => {
  const r = routeToStocker(3, from);
  assert.deepEqual(r[0], from);
  assert.notEqual(r[0], from);
  assert.equal(r[1].x, spurX(3));
  assert.equal(r[1].z, -0.4);
  assert.equal(r[2].z, LANE_OUT_Z);
  assert.equal(r[3].x, ENTRY_X);
  assert.equal(r[3].z, LANE_OUT_Z);
  const end = r[r.length - 1];
  assert.equal(end.x, ENTRY_X);
  assert.equal(end.z, -12.2);
  // station 0 has no west leg: duplicate point dropped
  assert.equal(routeToStocker(0, from).length, r.length - 1);
});

test("routeFromStocker rides the return lane over one hop per spur and drops into the station", () => {
  for (let i = 0; i < 7; i++) {
    const r = routeFromStocker(i, from);
    assert.deepEqual(r[0], from);
    assert.equal(r[1].x, EXIT_X);
    assert.equal(r[1].z, -12.2);
    assert.equal(r[2].z, LANE_RET_Z);
    const raised = r.filter((p) => p.y > BELT_Y + 0.02 + HOP_H - 1e-9);
    assert.equal(raised.length, 2 * (i + 1));
    for (let j = 0; j <= i; j++) {
      assert.ok(raised.some((p) => Math.abs(p.x - spurX(j)) < HOP_HALF));
    }
    const end = r[r.length - 1];
    assert.equal(end.x, dropX(i));
    assert.equal(end.z, 0);
    assert.equal(r[r.length - 2].z, -0.4);
  }
});

test("isStockerReturn: only Stocker to queue or run uses the return lane", () => {
  const stocker = { kind: "stocker", i: "station_down" };
  assert.equal(isStockerReturn(stocker, "queue"), true);
  assert.equal(isStockerReturn(stocker, "run"), true);
  assert.equal(isStockerReturn(stocker, "scrap"), false);
  assert.equal(isStockerReturn(stocker, "stocker"), false);
  assert.equal(isStockerReturn({ kind: "queue", i: 2 }, "run"), false);
  assert.equal(isStockerReturn({ kind: "run", i: 2 }, "queue"), false);
  assert.equal(isStockerReturn(null, "run"), false);
});

test("a Stocker lot dispatched to a station still ends on the return lane at that station", () => {
  const r = routeFromStocker(3, from);
  const end = r[r.length - 1];
  assert.equal(end.x, dropX(3));
  assert.ok(r.some((p) => p.z === LANE_RET_Z && p.y > BELT_Y + 0.02));
});

test("path helpers", () => {
  const pts = [{ x: 0, y: 0, z: 0 }, { x: 3, y: 0, z: 0 }, { x: 3, y: 0, z: 4 }];
  assert.equal(pathLength(pts), 7);
  assert.deepEqual(pointAt(pts, 1.5), { x: 1.5, y: 0, z: 0 });
  assert.deepEqual(pointAt(pts, 5), { x: 3, y: 0, z: 2 });
  assert.deepEqual(pointAt(pts, -1), { x: 0, y: 0, z: 0 });
  assert.deepEqual(pointAt(pts, 99), { x: 3, y: 0, z: 4 });
  assert.notEqual(pointAt(pts, 99), pts[2]);
  assert.equal(travelDuration(1), 1);
  assert.equal(travelDuration(27), 3);
  assert.equal(travelDuration(500), 5);
  assert.equal(easeInOut(0), 0);
  assert.equal(easeInOut(0.5), 0.5);
  assert.equal(easeInOut(1), 1);
});

test("buildLanes builds meshes using only COLORS tokens", () => {
  const reg = createRegistry();
  const g = buildLanes(THREE, reg);
  assert.ok(g.children.length > 20);
  assert.equal(g.userData.id, undefined);
  const allowed = new Set(Object.values(COLORS));
  g.traverse((o) => {
    if (o.isMesh) assert.ok(allowed.has(o.material.color.getHex()));
  });
});
