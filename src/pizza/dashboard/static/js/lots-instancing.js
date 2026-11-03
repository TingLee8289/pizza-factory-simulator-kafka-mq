// Instancing helpers for the lots in the 3D scene. A lot is drawn as up to 4 pizza layers plus a
// box, each one an instance of a merged, vertex-colored stage geometry, so thousands of lots cost a
// roughly constant number of draw calls. Pure helpers (stageOf, partsFor, writeTRS, lotPickBox) have
// no THREE dependency; the geometry builders and the pool take THREE as a parameter.

const LAYER_STEP = 0.14;
const LAYER_BASE_Y = 0.06;
const DOUGH = 0xe9cf93;
const BAKED = 0xc98a3d;
const SAUCE = 0xd9402b;
const CHEESE = 0xffd35a;
const CUT = 0x5a3a18;
const GREEN = 0x3f8f3f;
const DOTS = [[0.1, 0.08], [-0.12, 0.1], [0.02, -0.15], [-0.1, -0.08], [0.16, -0.06]];

// Stage of one pizza layer for a given completed-layers count L (cumulative: includes earlier stages).
export function stageOf(L) {
  if (L <= 0) return "ball";
  if (L === 1) return "base";
  if (L === 2) return "sauce";
  if (L === 3) return "cheese";
  if (L === 4) return "top";
  if (L === 5) return "baked";
  return "cut";
}

// Stages that contain topping dots (colour depends on the product).
export function stageHasDots(stage) {
  return stage === "top" || stage === "baked" || stage === "cut";
}

export function clampQty(qty) {
  return Math.max(1, Math.min(4, Math.floor(Number(qty) || 1)));
}

// Box height for a qty (lot-local, unscaled).
export function boxHeight(qty) {
  return 0.1 + clampQty(qty) * LAYER_STEP;
}

// Instances a lot needs, in lot-local unscaled units: one stage part per layer, plus the box when packaged.
export function partsFor({ layers, qty }) {
  const q = clampQty(qty);
  const L = Math.max(0, Math.min(7, layers || 0));
  const stage = stageOf(L);
  const parts = [];
  for (let k = 0; k < q; k++) parts.push({ stage, y: LAYER_BASE_Y + k * LAYER_STEP });
  if (L >= 7) {
    const sy = boxHeight(q);
    parts.push({ stage: "box", box: true, y: sy / 2, sy });
  }
  return parts;
}

// Write a column-major TRS (translation + axis scale, no rotation) 4x4 at arr[i*16..].
export function writeTRS(arr, i, x, y, z, sx, sy, sz) {
  const o = i * 16;
  arr[o] = sx; arr[o + 1] = 0; arr[o + 2] = 0; arr[o + 3] = 0;
  arr[o + 4] = 0; arr[o + 5] = sy; arr[o + 6] = 0; arr[o + 7] = 0;
  arr[o + 8] = 0; arr[o + 9] = 0; arr[o + 10] = sz; arr[o + 11] = 0;
  arr[o + 12] = x; arr[o + 13] = y; arr[o + 14] = z; arr[o + 15] = 1;
}

// Analytic pick box of a lot at pos with uniform scale (approximates the pizza stack / box).
export function lotPickBox(pos, scale, qty) {
  const h = boxHeight(qty) * scale;
  const r = 0.47 * scale;
  return {
    minX: pos.x - r, maxX: pos.x + r,
    minY: pos.y, maxY: pos.y + h,
    minZ: pos.z - r, maxZ: pos.z + r,
  };
}

// Merge [{geo, matrix, color}] into one non-indexed geometry with position, normal and color attributes.
export function mergeColored(THREE, parts) {
  const chunks = parts.map((p) => {
    let geo = p.geo.index ? p.geo.toNonIndexed() : p.geo.clone();
    geo = geo.applyMatrix4(p.matrix);
    return { geo, color: new THREE.Color(p.color) };
  });
  const total = chunks.reduce((n, c) => n + c.geo.attributes.position.count, 0);
  const pos = new Float32Array(total * 3);
  const nor = new Float32Array(total * 3);
  const col = new Float32Array(total * 3);
  let at = 0;
  for (const c of chunks) {
    const n = c.geo.attributes.position.count;
    pos.set(c.geo.attributes.position.array, at * 3);
    nor.set(c.geo.attributes.normal.array, at * 3);
    for (let v = 0; v < n; v++) {
      col[(at + v) * 3] = c.color.r;
      col[(at + v) * 3 + 1] = c.color.g;
      col[(at + v) * 3 + 2] = c.color.b;
    }
    at += n;
    c.geo.dispose();
  }
  const out = new THREE.BufferGeometry();
  out.setAttribute("position", new THREE.BufferAttribute(pos, 3));
  out.setAttribute("normal", new THREE.BufferAttribute(nor, 3));
  out.setAttribute("color", new THREE.BufferAttribute(col, 3));
  return out;
}

// Fixed shred layout so every lot's cheese looks the same (seeded LCG, same as the old per-lot mesh).
function shredLayout() {
  let seed = 7;
  const rnd = () => (seed = (seed * 16807) % 2147483647) / 2147483647;
  return Array.from({ length: 18 }, () => {
    const a = rnd() * Math.PI * 2;
    const r = Math.sqrt(rnd()) * 0.22;
    return { x: Math.cos(a) * r, z: Math.sin(a) * r, rot: rnd() * Math.PI, lift: rnd() * 0.01 };
  });
}

// Cached merged stage geometry per (stage, topping colour). Geometry is for one layer at local y=0.
export function buildStageGeometries(THREE, reg) {
  const shreds = shredLayout();
  const src = {
    ball: new THREE.SphereGeometry(0.2, 14, 10),
    disc: new THREE.CylinderGeometry(0.34, 0.34, 0.07, 20),
    sauce: new THREE.CylinderGeometry(0.29, 0.29, 0.02, 20),
    shred: new THREE.BoxGeometry(0.2, 0.012, 0.035),
    dot: new THREE.SphereGeometry(0.045, 8, 6),
    cut: new THREE.BoxGeometry(0.72, 0.012, 0.016),
    box: new THREE.BoxGeometry(0.82, 1, 0.82),
  };
  const cache = new Map();
  const mat = new THREE.Matrix4();
  const rot = new THREE.Matrix4();
  const at = (x, y, z, ry = 0) => {
    mat.makeTranslation(x, y, z);
    if (ry) mat.multiply(rot.makeRotationY(ry));
    return mat.clone();
  };

  function build(stage, topColor) {
    const parts = [];
    if (stage === "box") return mergeColored(THREE, [{ geo: src.box, matrix: new THREE.Matrix4(), color: 0xc9a46a }]);
    if (stage === "ball") {
      parts.push({ geo: src.ball, matrix: at(0, 0.1, 0), color: DOUGH });
      return mergeColored(THREE, parts);
    }
    const baked = stage === "baked" || stage === "cut";
    parts.push({ geo: src.disc, matrix: at(0, 0, 0), color: baked ? BAKED : DOUGH });
    if (stage !== "base") parts.push({ geo: src.sauce, matrix: at(0, 0.045, 0), color: SAUCE });
    if (stage !== "base" && stage !== "sauce") {
      for (const s of shreds) parts.push({ geo: src.shred, matrix: at(s.x, 0.06 + s.lift, s.z, s.rot), color: CHEESE });
    }
    if (stageHasDots(stage)) {
      DOTS.forEach(([x, z], j) => {
        parts.push({ geo: src.dot, matrix: at(x, 0.08, z), color: j % 2 === 1 ? GREEN : topColor });
      });
    }
    if (stage === "cut") {
      for (const a of [0, Math.PI / 2, Math.PI / 4, -Math.PI / 4]) {
        parts.push({ geo: src.cut, matrix: at(0, 0.075, 0, a), color: CUT });
      }
    }
    return mergeColored(THREE, parts);
  }

  return {
    get(stage, topColor = GREEN) {
      const key = stageHasDots(stage) ? `${stage}:${topColor}` : stage;
      let geo = cache.get(key);
      if (!geo) {
        geo = build(stage, topColor);
        if (reg) reg.add(geo);
        cache.set(key, geo);
      }
      return geo;
    },
  };
}

// Outline of the 0.9 x bh x 0.9 box as thin bars (an InstancedMesh cannot draw line segments).
// Dashed style dashes the 8 horizontal edges; verticals stay solid. Bottom of the box is at y = 0.
export function buildOutlineGeometry(THREE, qty, style, thickness = 0.012) {
  const bh = boxHeight(qty);
  const W = 0.9;
  const t = thickness;
  const parts = [];
  const m = new THREE.Matrix4();
  const bar = (len, axis, cx, cy, cz) => {
    const sx = axis === "x" ? len : t;
    const sy = axis === "y" ? len : t;
    const sz = axis === "z" ? len : t;
    parts.push({
      geo: new THREE.BoxGeometry(sx, sy, sz),
      matrix: m.clone().makeTranslation(cx, cy, cz),
      color: 0xffffff,
    });
  };
  const run = (axis, len, cx, cy, cz) => {
    if (style !== "dashed") { bar(len, axis, cx, cy, cz); return; }
    const dash = 0.12;
    const gap = 0.08;
    for (let s = 0; s < len; s += dash + gap) {
      const d = Math.min(dash, len - s);
      const c = s + d / 2 - len / 2;
      bar(d, axis, axis === "x" ? c : cx, cy, axis === "z" ? c : cz);
    }
  };
  for (const y of [0, bh]) {
    for (const s of [-1, 1]) {
      run("x", W, 0, y, s * W / 2);
      run("z", W, s * W / 2, y, 0);
    }
  }
  for (const sx of [-1, 1]) for (const sz of [-1, 1]) bar(bh, "y", sx * W / 2, bh / 2, sz * W / 2);
  const geo = mergeColored(THREE, parts);
  for (const p of parts) p.geo.dispose();
  return geo;
}

// Growable InstancedMesh. Capacity doubles by allocating a fresh mesh, copying the matrices and
// swapping it in its parent. `count` is the live instance count; call setCount/add then flag the upload.
export function createPool(THREE, geometry, material, { castShadow = false, initial = 64 } = {}) {
  let cap = Math.max(1, initial);
  function make(n) {
    const mesh = new THREE.InstancedMesh(geometry, material, n);
    mesh.count = 0;
    mesh.frustumCulled = false;
    mesh.castShadow = castShadow;
    mesh.instanceMatrix.setUsage(THREE.DynamicDrawUsage);
    return mesh;
  }
  const pool = {
    mesh: make(cap),
    count: 0,
    get capacity() { return cap; },
    ensure(n) {
      if (n <= cap) return;
      let next = cap;
      while (next < n) next *= 2;
      const old = pool.mesh;
      const mesh = make(next);
      mesh.instanceMatrix.array.set(old.instanceMatrix.array);
      mesh.count = pool.count;
      if (old.parent) {
        const parent = old.parent;
        parent.remove(old);
        parent.add(mesh);
      }
      old.dispose();
      cap = next;
      pool.mesh = mesh;
    },
    add() {
      pool.ensure(pool.count + 1);
      pool.count += 1;
      pool.mesh.count = pool.count;
      return pool.count - 1;
    },
    set(i, x, y, z, sx, sy, sz) {
      writeTRS(pool.mesh.instanceMatrix.array, i, x, y, z, sx, sy, sz);
    },
    setCount(n) {
      pool.ensure(n);
      pool.count = n;
      pool.mesh.count = n;
    },
    dispose() {
      if (pool.mesh.parent) pool.mesh.parent.remove(pool.mesh);
      pool.mesh.dispose();
    },
  };
  return pool;
}
