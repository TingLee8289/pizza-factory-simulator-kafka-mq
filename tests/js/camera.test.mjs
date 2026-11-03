import test from "node:test";
import assert from "node:assert/strict";
import {
  createCameraRig, cameraPosition, orthoExtents, focus, viewOffsetPx, wrapAngle,
} from "../../src/pizza/dashboard/static/js/camera.js";

const mk = (o = {}) => createCameraRig({ target: { x: 3, z: -2.5 }, azimuth: 0.3, elevation: 0.62, distance: 80, viewHeight: 36, ...o });

test("pan clamps to x +/-30 and z +/-14", () => {
  const r = mk();
  r.pan(1000, 1000);
  assert.equal(r.target.x, 30);
  assert.equal(r.target.z, 14);
  r.pan(-1000, -1000);
  assert.equal(r.target.x, -30);
  assert.equal(r.target.z, -14);
});

test("zoom clamps to 0.6..3.2", () => {
  const r = mk();
  r.zoomBy(100);
  assert.equal(r.zoom, 3.2);
  r.zoomBy(0.0001);
  assert.equal(r.zoom, 0.6);
});

test("rotate wraps azimuth into (-pi, pi]", () => {
  const r = mk();
  r.rotate(Math.PI * 2 + 0.1);
  assert.ok(Math.abs(r.azimuth - 0.4) < 1e-9);
  r.rotate(Math.PI);
  assert.ok(r.azimuth > -Math.PI && r.azimuth <= Math.PI);
  assert.equal(wrapAngle(-Math.PI), Math.PI);
  assert.equal(wrapAngle(Math.PI), Math.PI);
});

test("home restores the initial state", () => {
  const r = mk();
  r.pan(10, 5); r.zoomBy(2); r.rotate(1);
  r.home();
  assert.deepEqual(r.target, { x: 3, z: -2.5 });
  assert.equal(r.zoom, 1);
  assert.equal(r.azimuth, 0.3);
});

test("cameraPosition follows target, azimuth, elevation and distance", () => {
  const r = mk();
  const p = cameraPosition(r);
  assert.ok(Math.abs(p.y - 80 * Math.sin(0.62)) < 1e-9);
  assert.ok(Math.abs(p.x - (3 + 80 * Math.cos(0.62) * Math.sin(0.3))) < 1e-9);
  assert.ok(Math.abs(p.z - (-2.5 + 80 * Math.cos(0.62) * Math.cos(0.3))) < 1e-9);
  const len = Math.hypot(p.x - 3, p.y, p.z + 2.5);
  assert.ok(Math.abs(len - 80) < 1e-9);
});

test("orthoExtents scale with viewHeight / zoom and aspect", () => {
  const r = mk();
  let e = orthoExtents(r, 2);
  assert.deepEqual(e, { left: -36, right: 36, top: 18, bottom: -18 });
  r.zoomBy(2);
  e = orthoExtents(r, 2);
  assert.deepEqual(e, { left: -18, right: 18, top: 9, bottom: -9 });
});

test("focus easing is deterministic, monotone, clamped", () => {
  const r = mk();
  const a = focus(r, { x: 12.4, z: 0 }, 1.6, 0.1, 6);
  const b = focus(r, { x: 12.4, z: 0 }, 1.6, 0.1, 6);
  assert.deepEqual(a, b);
  assert.ok(a.target.x > 3 && a.target.x < 12.4);
  assert.ok(a.zoom > 1 && a.zoom < 1.6);
  const far = focus(r, { x: 999, z: 999 }, 99, 100, 6);
  assert.equal(far.target.x, 30);
  assert.equal(far.target.z, 14);
  assert.equal(far.zoom, 3.2);
  assert.equal(far.done, true);
});

test("reducedMotion jumps straight to the goal", () => {
  const r = mk({ reducedMotion: true });
  const n = focus(r, { x: 12.4, z: 0 }, 1.6, 0.001, 6);
  assert.deepEqual(n.target, { x: 12.4, z: 0 });
  assert.equal(n.zoom, 1.6);
  r.focusOn({ x: -6.2, z: 0 }, 1.6);
  assert.equal(r.target.x, -6.2);
  assert.equal(r.goal, null);
});

test("rig.update eases focusOn goal until done and pan cancels it", () => {
  const r = mk();
  r.focusOn({ x: 12.4, z: 0 }, 1.6);
  assert.ok(r.goal);
  for (let i = 0; i < 600 && r.goal; i++) r.update(1 / 60);
  assert.equal(r.goal, null);
  assert.equal(r.target.x, 12.4);
  r.focusOn({ x: 0, z: 0 }, 1);
  r.pan(1, 0);
  assert.equal(r.goal, null);
});

test("viewOffsetPx stores and clears the panel shift", () => {
  const r = mk();
  assert.equal(viewOffsetPx(r, 120), 120);
  assert.equal(r.offsetPx, 120);
  assert.equal(viewOffsetPx(r, 0), 0);
  assert.equal(r.offsetPx, 0);
});
