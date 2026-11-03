import test from "node:test";
import assert from "node:assert/strict";
import * as THREE from "../../src/pizza/dashboard/static/vendor/three.module.js";
import {
  stageOf, partsFor, writeTRS, lotPickBox, mergeColored, createPool, buildStageGeometries,
  buildOutlineGeometry, clampQty,
} from "../../src/pizza/dashboard/static/js/lots-instancing.js";

const near = (a, b) => Math.abs(a - b) < 1e-9;

test("stageOf maps completed layers to cumulative stages", () => {
  assert.deepEqual([0, 1, 2, 3, 4, 5, 6, 7].map(stageOf), ["ball", "base", "sauce", "cheese", "top", "baked", "cut", "cut"]);
});

test("partsFor stacks one part per layer and adds the box only when packaged", () => {
  const p = partsFor({ layers: 0, qty: 2 });
  assert.equal(p.length, 2);
  assert.ok(p.every((x) => x.stage === "ball"));
  assert.ok(near(p[0].y, 0.06) && near(p[1].y, 0.2));
  const q = partsFor({ layers: 7, qty: 3 });
  assert.equal(q.length, 4);
  const box = q[3];
  assert.ok(box.box);
  assert.ok(near(box.sy, 0.1 + 3 * 0.14) && near(box.y, box.sy / 2));
  assert.equal(partsFor({ layers: 6, qty: 1 }).some((x) => x.box), false);
});

test("partsFor clamps qty to 1..4 and layers above 7 behave as 7", () => {
  assert.equal(partsFor({ layers: 1, qty: 99 }).length, 4);
  assert.equal(partsFor({ layers: 1, qty: 0 }).length, 1);
  assert.equal(clampQty(-3), 1);
  const q = partsFor({ layers: 12, qty: 2 });
  assert.equal(q.length, 3);
  assert.equal(q[0].stage, "cut");
});

test("writeTRS writes a column-major scale + translation matrix", () => {
  const arr = new Float32Array(32);
  writeTRS(arr, 1, 1, 2, 3, 4, 5, 6);
  assert.deepEqual([...arr.slice(16, 32)], [4, 0, 0, 0, 0, 5, 0, 0, 0, 0, 6, 0, 1, 2, 3, 1]);
  assert.equal(arr[0], 0);
});

test("lotPickBox spans the stack footprint and height", () => {
  const b = lotPickBox({ x: 10, y: 0.3, z: -2 }, 2, 3);
  assert.ok(near(b.minX, 10 - 0.94) && near(b.maxX, 10 + 0.94));
  assert.ok(near(b.minZ, -2 - 0.94) && near(b.maxZ, -2 + 0.94));
  assert.ok(near(b.minY, 0.3) && near(b.maxY, 0.3 + (0.1 + 3 * 0.14) * 2));
});

test("mergeColored merges parts into one non-indexed colored geometry", () => {
  const a = new THREE.BoxGeometry(1, 1, 1);
  const b = new THREE.SphereGeometry(0.5, 8, 6);
  const nA = a.toNonIndexed().attributes.position.count;
  const nB = b.toNonIndexed().attributes.position.count;
  const geo = mergeColored(THREE, [
    { geo: a, matrix: new THREE.Matrix4(), color: 0xff0000 },
    { geo: b, matrix: new THREE.Matrix4().makeTranslation(0, 2, 0), color: 0x00ff00 },
  ]);
  assert.equal(geo.index, null);
  assert.equal(geo.attributes.position.count, nA + nB);
  assert.equal(geo.attributes.normal.count, nA + nB);
  assert.equal(geo.attributes.color.count, nA + nB);
  assert.equal(geo.attributes.color.getX(0) > 0.9, true);
  assert.equal(geo.attributes.color.getY(nA) > 0, true);
});

test("stage geometries are cached and grow cumulatively", () => {
  const sg = buildStageGeometries(THREE, null);
  assert.equal(sg.get("base"), sg.get("base"));
  const c = (s) => sg.get(s).attributes.position.count;
  assert.ok(c("sauce") > c("base"));
  assert.ok(c("cheese") > c("sauce"));
  assert.ok(c("top") > c("cheese"));
  assert.ok(c("cut") > c("baked"));
  assert.notEqual(sg.get("top", 0xa42424), sg.get("top", 0x3f8f3f));
});

test("outline geometry builds for every qty and style", () => {
  for (let q = 1; q <= 4; q++) {
    for (const style of ["solid", "dashed"]) {
      const g = buildOutlineGeometry(THREE, q, style);
      assert.ok(g.attributes.position.count > 0);
    }
  }
  const solid = buildOutlineGeometry(THREE, 1, "solid").attributes.position.count;
  const dashed = buildOutlineGeometry(THREE, 1, "dashed").attributes.position.count;
  assert.ok(dashed > solid);
});

test("createPool doubles capacity, preserves matrices and swaps the mesh in its parent", () => {
  const pool = createPool(THREE, new THREE.BoxGeometry(1, 1, 1), new THREE.MeshBasicMaterial(), { initial: 2 });
  const parent = new THREE.Group();
  parent.add(pool.mesh);
  assert.equal(pool.mesh.frustumCulled, false);
  const i0 = pool.add();
  pool.set(i0, 7, 8, 9, 1, 1, 1);
  pool.add();
  const first = pool.mesh;
  assert.equal(pool.capacity, 2);
  const i2 = pool.add();
  assert.equal(i2, 2);
  assert.equal(pool.capacity, 4);
  assert.notEqual(pool.mesh, first);
  assert.equal(parent.children.length, 1);
  assert.equal(parent.children[0], pool.mesh);
  assert.equal(pool.mesh.count, 3);
  assert.equal(pool.mesh.instanceMatrix.array[12], 7);
  pool.setCount(0);
  assert.equal(pool.mesh.count, 0);
  pool.dispose();
  assert.equal(parent.children.length, 0);
});
