// Static scene builders. Every builder takes THREE as a parameter so this module has no
// top-level imports and the layout helpers stay importable from node tests.
// Layout numbers are the locked values from 05-UI-SPEC "Scene (locked)".
export const STATION_NAMES = ["spin", "sauce", "cheese", "toppings", "bake", "slice", "package"];
export const STATION_GAP = 6.2;
export const GATE_X = -27.5;
export const TRUCK_X = 29.5;
export const BELT_Y = 0.3;

export const COLORS = {
  belt: 0x7c869c,
  body: 0xffffff,
  canopy: 0xe3e9f6,
  dark: 0x3b4559,
  bandIdle: 0xc9d3e6,
  bandActive: 0x2f6bff,
  lightBusy: 0x12a46a,
  lightIdle: 0x9aa5b8,
  lightDown: 0xe5484d,
  ground: 0xeef3fb,
  clear: 0xe4ecf8,
  badge: 0x2f6bff,
  badgeHot: 0xe5484d,
  warn: 0xe08a00, // same value as the --color-warn token
  mint: 0x7fd1ae, // same value as the tree leaf color
};

// Stocker parking area behind the stations (z negative). One FIFO row per kind.
export const STOCKER_KINDS = ["overflow", "station_down", "station_offline", "bank"];
// X0 moved west (-21 to -24) to make room for the separate entry and exit ports of the lanes.
const STOCKER_X0 = -24.0;
const STOCKER_Z0 = -13.0;
const STOCKER_COLS = 12;
const STOCKER_DEPTH = 6.6;
export const STOCKER_FRONT_Z = STOCKER_Z0 - 2.2 + STOCKER_DEPTH / 2; // slab front edge (-11.9)

export function stockerSlot(kind, k) {
  const row = Math.max(0, STOCKER_KINDS.indexOf(kind));
  const lap = Math.floor(k / STOCKER_COLS) % 3;
  return {
    x: STOCKER_X0 + (k % STOCKER_COLS) * 0.78,
    y: BELT_Y + 0.02 + 0.1,
    z: STOCKER_Z0 - row * 1.4 - lap * 0.45,
  };
}

// Scrap / DLQ zone: the old red tray in front of the line is now a conveyor that feeds one
// trash can at its east end. Every failed lot lands on the deck, rides east, climbs the head
// and drops into the can.
export const SCRAP_Z = 4.4; // deck centre line, same as the old red tray
export const SCRAP_W = 1.6; // deck width along z
export const SCRAP_X0 = -3 * STATION_GAP - 0.5; // west end of the deck: spin junction less half the spur width, nothing runs past the spin spur
export const SCRAP_BIN_X = TRUCK_X + 1.2; // truck column (30.7), in front of the truck, off the main belt (z 4.4 vs belt +-0.75)
export const SCRAP_X2 = SCRAP_BIN_X - 0.2; // discharge lip (30.5), above the can mouth
export const SCRAP_X1 = SCRAP_X2 - 0.8; // top of the incline (belt head, 29.7), ramp run stays 4.4 long
export const SCRAP_RAMP_X0 = SCRAP_X1 - 4.4; // flat deck ends (past the package junction at 18.6), incline starts (25.3)
export const SCRAP_Y = BELT_Y + 0.02; // flat deck carry height
export const SCRAP_HEAD_Y = 1.3; // head carry height
export const SCRAP_APRON_X1 = SCRAP_BIN_X + 2.6; // floor apron east edge (33.3), covers the can
export const SCRAP_APRON_Z1 = 6.2; // floor apron near edge
export const SCRAP_BIN_Z = SCRAP_Z;
export const SCRAP_BIN_R = 1.2; // half width / depth of the square can
export const SCRAP_BIN_H = 1.05; // rim height
export const SCRAP_STACK_MAX = 8; // slots inside the can

// Per-station spurs: a short deck in front of every station that feeds the scrap belt.
// Junctions stay on the flat section; stations east of it step their junction back so no
// spur ever crosses the incline, the head or the can (order is preserved, so spurs never cross).
export const SCRAP_SPUR_Z0 = 1.0; // spur top, just outside the main line belt
export const SCRAP_SPUR_Z1 = SCRAP_Z - (SCRAP_W + 0.3) / 2; // abuts the deck frame near edge (3.45)
export const SCRAP_SPUR_W = 1.0;
export const SCRAP_SPUR_MAX_X = SCRAP_RAMP_X0 - 1.2; // junctions stay 1.2 clear of the incline

// Carry height of the scrap deck at x: flat until the ramp, then rising to the head.
export function scrapBeltY(x) {
  if (x <= SCRAP_RAMP_X0) return SCRAP_Y;
  const t = Math.min(1, (x - SCRAP_RAMP_X0) / (SCRAP_X1 - SCRAP_RAMP_X0));
  return SCRAP_Y + (SCRAP_HEAD_Y - SCRAP_Y) * t;
}

// Where a failing lot's spur meets the deck: every junction sits at its own station x, so all
// seven spurs share the uniform STATION_GAP spacing (the flat deck now runs past package).
export function scrapEntryX(i) {
  if (i < 0) return SCRAP_X0 + SCRAP_SPUR_W / 2; // Stocker releases enter at the west end, no spur
  return Math.max(SCRAP_X0 + SCRAP_SPUR_W / 2, Math.min(SCRAP_SPUR_MAX_X, stationX(i)));
}

// Stack slots inside the can, 2x2 per layer and at most two layers, clamped so the pile cannot grow.
export function scrapSlot(k) {
  const n = Math.max(0, Math.min(SCRAP_STACK_MAX - 1, k));
  const col = n % 2;
  const row = Math.floor(n / 2) % 2;
  const layer = Math.floor(n / 4);
  return {
    x: SCRAP_BIN_X - 0.45 + col * 0.9,
    y: 0.78 + layer * 0.1,
    z: SCRAP_Z - 0.35 + row * 0.7,
  };
}

// Route for a failed lot: down the station's spur, across the deck, up the head, into the can.
export function routeToScrap(i, from, k = 0) {
  const entryX = scrapEntryX(i);
  const points = [{ x: from.x, y: from.y, z: from.z }];
  if (i >= 0) {
    // ride the station spur south onto the deck (Stocker releases skip it and enter west)
    points.push({ x: entryX, y: SCRAP_Y, z: SCRAP_SPUR_Z0 });
    points.push({ x: entryX, y: SCRAP_Y, z: SCRAP_Z });
  } else {
    points.push({ x: entryX, y: SCRAP_Y, z: SCRAP_Z });
  }
  if (entryX < SCRAP_RAMP_X0) points.push({ x: SCRAP_RAMP_X0, y: SCRAP_Y, z: SCRAP_Z });
  points.push(
    { x: SCRAP_X1, y: SCRAP_HEAD_Y, z: SCRAP_Z },
    { x: SCRAP_X2, y: SCRAP_HEAD_Y, z: SCRAP_Z },
    scrapSlot(k),
  );
  return dedupe(points);
}

export const stationX = (i) => (i - 3) * STATION_GAP;

// ----- Stocker conveyor lanes (to-Stocker lane, return lane, spurs, drops, hops) -----
export const LANE_OUT_Z = -6.4;
export const LANE_RET_Z = -4.6;
export const LANE_W = 0.8;
export const HOP_H = 0.45;
export const HOP_HALF = 0.9;
export const SPUR_DX = -3.6;
export const DROP_DX = -2.4;
export const spurX = (i) => stationX(i) + SPUR_DX;
export const dropX = (i) => stationX(i) + DROP_DX;
export const ENTRY_X = spurX(0);
export const EXIT_X = ENTRY_X - 1.6;
const LANE_Y = BELT_Y + 0.02; // belt level, same y as queue slots
const PORT_Z = -12.2;
const HOP_RAMP = 0.4;

function dedupe(points) {
  const out = [];
  for (const p of points) {
    const q = out[out.length - 1];
    if (!q || q.x !== p.x || q.y !== p.y || q.z !== p.z) out.push({ x: p.x, y: p.y, z: p.z });
  }
  return out;
}

export function routeToStocker(i, from) {
  const yIn = BELT_Y + 0.12;
  return dedupe([
    { x: from.x, y: from.y, z: from.z },
    { x: spurX(i), y: LANE_Y, z: -0.4 },
    { x: spurX(i), y: LANE_Y, z: LANE_OUT_Z },
    { x: ENTRY_X, y: LANE_Y, z: LANE_OUT_Z },
    { x: ENTRY_X, y: yIn, z: PORT_Z },
  ]);
}

export function routeFromStocker(i, from) {
  const pts = [
    { x: from.x, y: from.y, z: from.z },
    { x: EXIT_X, y: BELT_Y + 0.12, z: PORT_Z },
    { x: EXIT_X, y: LANE_Y, z: LANE_RET_Z },
  ];
  for (let j = 0; j <= i; j++) {
    const sx = spurX(j);
    pts.push(
      { x: sx - HOP_HALF, y: LANE_Y, z: LANE_RET_Z },
      { x: sx - HOP_HALF + HOP_RAMP, y: LANE_Y + HOP_H, z: LANE_RET_Z },
      { x: sx + HOP_HALF - HOP_RAMP, y: LANE_Y + HOP_H, z: LANE_RET_Z },
      { x: sx + HOP_HALF, y: LANE_Y, z: LANE_RET_Z },
    );
  }
  pts.push(
    { x: dropX(i), y: LANE_Y, z: LANE_RET_Z },
    { x: dropX(i), y: LANE_Y, z: -0.4 },
    { x: dropX(i), y: LANE_Y, z: 0 },
  );
  return dedupe(pts);
}

// A lot leaving the Stocker for a queue slot or a station rides the return lane (routeFromStocker).
export function isStockerReturn(prev, kind) {
  return Boolean(prev) && prev.kind === "stocker" && (kind === "queue" || kind === "run");
}

export function pathLength(points) {
  let len = 0;
  for (let k = 1; k < points.length; k++) {
    const a = points[k - 1];
    const b = points[k];
    len += Math.hypot(b.x - a.x, b.y - a.y, b.z - a.z);
  }
  return len;
}

export function pointAt(points, s) {
  const total = pathLength(points);
  let rest = Math.max(0, Math.min(total, s));
  for (let k = 1; k < points.length; k++) {
    const a = points[k - 1];
    const b = points[k];
    const seg = Math.hypot(b.x - a.x, b.y - a.y, b.z - a.z);
    if (rest <= seg && seg > 0) {
      const f = rest / seg;
      return { x: a.x + (b.x - a.x) * f, y: a.y + (b.y - a.y) * f, z: a.z + (b.z - a.z) * f };
    }
    rest -= seg;
  }
  const last = points[points.length - 1];
  return { x: last.x, y: last.y, z: last.z };
}

export const travelDuration = (len) => Math.max(1, Math.min(5, len / 9));
export const easeInOut = (t) => (t < 0.5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2);

// Queue slots on the belt in front of (left of) a station. k = arrival order.
export function queueSlot(i, k) {
  const row = Math.floor(k / 4);
  const col = k % 4;
  const zs = [0, 0.85, -0.85];
  return {
    x: stationX(i) - 2.0 - col * 0.78,
    y: BELT_Y + 0.02 + (row >= 3 ? 0.14 * (row - 2) : 0),
    z: zs[row % 3],
  };
}

// The running lot sits under the station canopy.
export function stationSlot(i) {
  return { x: stationX(i), y: BELT_Y + 0.02, z: 0 };
}

export const gateSlot = () => ({ x: GATE_X - 2, y: BELT_Y + 0.02, z: 0 });
export const truckSlot = () => ({ x: TRUCK_X, y: BELT_Y + 0.9, z: 0 });

// Registry collecting every geometry, material and texture so teardown can dispose them.
export function createRegistry() {
  const items = new Set();
  return {
    add(x) { items.add(x); return x; },
    disposeAll() {
      for (const x of items) if (x && typeof x.dispose === "function") x.dispose();
      items.clear();
    },
  };
}

function makeKit(THREE, reg) {
  const geo = {
    box: reg.add(new THREE.BoxGeometry(1, 1, 1)),
    cyl: reg.add(new THREE.CylinderGeometry(1, 1, 1, 20)),
    cone: reg.add(new THREE.ConeGeometry(1, 1, 16)),
    sph: reg.add(new THREE.SphereGeometry(1, 12, 10)),
  };
  const mat = (color, extra = {}) =>
    reg.add(new THREE.MeshStandardMaterial({ color, roughness: 0.85, metalness: 0, ...extra }));
  const mesh = (g, m, sx, sy, sz, x, y, z, parent, shadow = true) => {
    const o = new THREE.Mesh(g, m);
    o.scale.set(sx, sy, sz);
    o.position.set(x, y, z);
    o.castShadow = shadow;
    o.receiveShadow = true;
    if (parent) parent.add(o);
    return o;
  };
  return { geo, mat, mesh };
}

// Small deterministic generator so decor placement is stable between reloads.
function mulberry32(a) {
  return function next() {
    a |= 0; a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// Tile colours for the bake oven, sampled from the mosaic reference (teal, navy, red, orange, yellow, cream, grey).
const TILE_COLORS = [0x1f4e6b, 0x2d6f80, 0x26407a, 0xd8402f, 0xe8803a, 0xf0c05a, 0xf3e4c0, 0x8c8f94, 0x5fa39a];

// Flat-shaded mosaic: every triangle gets one random tile colour from the palette.
function mosaicGeometry(THREE, reg, geometry, palette, rand) {
  const g = reg.add(geometry.toNonIndexed());
  geometry.dispose();
  const count = g.attributes.position.count;
  const colors = new Float32Array(count * 3);
  const c = new THREE.Color();
  for (let i = 0; i < count; i += 3) {
    c.set(palette[Math.floor(rand() * palette.length)]);
    for (let k = 0; k < 3; k++) c.toArray(colors, (i + k) * 3);
  }
  g.setAttribute("color", new THREE.BufferAttribute(colors, 3));
  return g;
}

function drawSprite(THREE, reg, canvas, w, h) {
  const tex = reg.add(new THREE.CanvasTexture(canvas));
  tex.colorSpace = THREE.SRGBColorSpace;
  const sm = reg.add(new THREE.SpriteMaterial({ map: tex, transparent: true, depthTest: false }));
  const sprite = new THREE.Sprite(sm);
  sprite.scale.set(w, h, 1);
  sprite.renderOrder = 10;
  return { sprite, tex, canvas };
}

function roundRect(g, x, y, w, h, r) {
  g.beginPath();
  g.moveTo(x + r, y);
  g.arcTo(x + w, y, x + w, y + h, r);
  g.arcTo(x + w, y + h, x, y + h, r);
  g.arcTo(x, y + h, x, y, r);
  g.arcTo(x, y, x + w, y, r);
  g.closePath();
}

// Text goes through canvas fillText only (never HTML), see threat T-05-48.
export function makeLabelSprite(THREE, reg, text, opts = {}) {
  const canvas = document.createElement("canvas");
  canvas.width = 256;
  canvas.height = 64;
  const g = canvas.getContext("2d");
  const maxChars = opts.maxChars || 18;
  const fontPx = opts.fontPx || 28;
  const paint = (t, accent) => {
    g.clearRect(0, 0, 256, 64);
    g.fillStyle = "rgba(255,255,255,0.92)";
    roundRect(g, 4, 8, 248, 48, 24);
    g.fill();
    g.lineWidth = 3;
    g.strokeStyle = opts.stroke || (accent ? "#2f6bff" : "#c9d3e6");
    g.stroke();
    g.fillStyle = "#16213a";
    g.font = `600 ${fontPx}px system-ui, sans-serif`;
    g.textAlign = "center";
    g.textBaseline = "middle";
    g.fillText(String(t).slice(0, maxChars), 128, 33);
  };
  paint(text, Boolean(opts.accent));
  const s = drawSprite(THREE, reg, canvas, opts.width || 4.4, (opts.width || 4.4) / 4);
  s.setText = (t, accent) => { paint(t, Boolean(accent)); s.tex.needsUpdate = true; };
  return s;
}

function makeBadgeSprite(THREE, reg) {
  const canvas = document.createElement("canvas");
  canvas.width = 64;
  canvas.height = 64;
  const g = canvas.getContext("2d");
  const paint = (n) => {
    g.clearRect(0, 0, 64, 64);
    g.fillStyle = n >= 4 ? "#e5484d" : "#2f6bff";
    g.beginPath();
    g.arc(32, 32, 28, 0, Math.PI * 2);
    g.fill();
    g.fillStyle = "#ffffff";
    g.font = "700 32px system-ui, sans-serif";
    g.textAlign = "center";
    g.textBaseline = "middle";
    g.fillText(String(n), 32, 34);
  };
  const s = drawSprite(THREE, reg, canvas, 1.15, 1.15);
  s.paint = paint;
  return s;
}

export function buildGround(THREE, reg) {
  const m = reg.add(new THREE.MeshStandardMaterial({ color: COLORS.ground, roughness: 0.95 }));
  const ground = new THREE.Mesh(reg.add(new THREE.PlaneGeometry(260, 260)), m);
  ground.rotation.x = -Math.PI / 2;
  ground.receiveShadow = true;
  return ground;
}

export function buildBelt(THREE, reg) {
  const { geo, mat, mesh } = makeKit(THREE, reg);
  const group = new THREE.Group();
  const c = document.createElement("canvas");
  c.width = 64;
  c.height = 16;
  const g = c.getContext("2d");
  g.fillStyle = "#ffffff";
  g.fillRect(0, 0, 64, 16);
  g.fillStyle = "#c4cbd9";
  g.fillRect(0, 0, 8, 16);
  g.fillRect(32, 0, 8, 16);
  const tex = reg.add(new THREE.CanvasTexture(c));
  tex.wrapS = tex.wrapT = THREE.RepeatWrapping;
  tex.repeat.set(24, 1);
  const top = reg.add(new THREE.MeshStandardMaterial({ color: COLORS.belt, roughness: 0.9, map: tex }));
  mesh(geo.box, mat(COLORS.dark), 66, 0.22, 1.9, 1.5, 0.11, 0, group);
  mesh(geo.box, top, 66, 0.06, 1.5, 1.5, 0.26, 0, group, false);
  return { group, texture: tex };
}

// Signal-tower lamps: lit colour and dimmed colour per state (red down, yellow idle, green busy).
const LAMP_ON = { down: COLORS.lightDown, idle: 0xf5c542, busy: COLORS.lightBusy };
const LAMP_OFF = { down: 0x5a3033, idle: 0x5c4a22, busy: 0x244f3e };

export function buildStations(THREE, reg, names = STATION_NAMES) {
  const { geo, mat, mesh } = makeKit(THREE, reg);
  const group = new THREE.Group();
  const bodyMat = mat(COLORS.body);
  const canopyMat = mat(COLORS.canopy);
  const darkMat = mat(COLORS.dark);
  const glowMat = mat(0xff8a3d);
  const handles = [];

  names.forEach((name, i) => {
    const g = new THREE.Group();
    g.position.set(stationX(i), 0, 0);
    g.userData = { kind: "station", id: name, index: i };
    group.add(g);
    const big = name === "bake";
    const W = big ? 3.4 : 2.6;
    const bandMat = mat(COLORS.bandIdle);
    const lamps = {};
    const spinners = [];
    let glow = null;

    mesh(geo.box, bodyMat, W, 1.9, 1.5, 0, 1.15, -1.9, g);
    mesh(geo.box, bandMat, W + 0.1, 0.22, 1.6, 0, 2.0, -1.9, g);
    mesh(geo.box, canopyMat, W - 0.2, 0.16, 2.4, 0, 1.55, -0.55, g);
    mesh(geo.box, darkMat, 0.14, 1.25, 0.14, -W / 2 + 0.3, 0.85, 0.55, g);
    mesh(geo.box, darkMat, 0.14, 1.25, 0.14, W / 2 - 0.3, 0.85, 0.55, g);
    // Signal tower on the right side: housing with three lamps, red (down) on top, yellow (idle), green (busy) at the bottom.
    // Back right corner, clear of the station props (items stay at x <= 0.8 and z near -1.9).
    const towerX = W / 2 - 0.25;
    const towerZ = -2.2;
    mesh(geo.cyl, mat(0xf4f6fa), 0.14, 0.8, 0.14, towerX, 2.5, towerZ, g);
    [["down", 2.76], ["idle", 2.5], ["busy", 2.24]].forEach(([key, y]) => {
      lamps[key] = reg.add(new THREE.MeshStandardMaterial({ color: LAMP_OFF[key], emissive: 0x000000, roughness: 0.4 }));
      mesh(geo.cyl, lamps[key], 0.155, 0.22, 0.155, towerX, y, towerZ, g, false);
    });
    // Lights one lamp by state; null turns them all off.
    const showLamp = (key) => {
      for (const k of Object.keys(lamps)) {
        const on = k === key;
        lamps[k].color.setHex(on ? LAMP_ON[k] : LAMP_OFF[k]);
        lamps[k].emissive.setHex(on ? LAMP_ON[k] : 0x000000);
        lamps[k].emissiveIntensity = on ? 0.5 : 0;
      }
    };
    mesh(geo.box, darkMat, 1.8, 0.1, 0.1, 0, 3.1, -1.9, g, false);
    const barMat = mat(COLORS.bandActive);
    const bar = mesh(geo.box, barMat, 1.8, 0.11, 0.11, 0, 3.1, -1.89, g, false);
    bar.visible = false;

    if (name === "spin") {
      // Round flat dough skin lying on a flour-dusted board; it spins in place.
      mesh(geo.box, mat(0xf7f3e8), 1.4, 0.08, 1.2, 0, 2.2, -1.9, g); // flour board on the band, kept clear of the lamp tower
      const skin = mesh(geo.cyl, mat(0xf0dca6, { roughness: 0.9 }), 0.5, 0.04, 0.5, 0, 2.3, -1.9, g);
      spinners.push({ o: skin, speed: 2.5 });
    } else if (name === "sauce") {
      // Wooden bowl of tomato sauce with a wooden spoon, after the reference photo.
      const woodMat = mat(0xa8763e, { roughness: 0.7 });
      const bowlX = -0.5;
      const bowlZ = -1.9;
      mesh(geo.cyl, woodMat, 0.45, 0.42, 0.45, bowlX, 2.32, bowlZ, g); // wooden bowl
      mesh(geo.cyl, mat(0xc8321f, { roughness: 0.6 }), 0.37, 0.05, 0.37, bowlX, 2.55, bowlZ, g, false); // sauce surface
      const spoon = new THREE.Group();
      spoon.position.set(bowlX + 0.05, 2.6, bowlZ);
      spoon.rotation.z = 0.35; // handle rises out of the bowl to the right
      g.add(spoon);
      mesh(geo.sph, woodMat, 0.2, 0.05, 0.14, -0.1, 0, 0, spoon); // scoop
      mesh(geo.box, woodMat, 0.8, 0.07, 0.07, 0.35, 0, 0, spoon); // handle
      mesh(geo.cyl, darkMat, 0.08, 0.7, 0.08, -0.5, 1.5, -1.2, g);
    } else if (name === "cheese") {
      // Cheese wedge with holes, after the reference picture: an extruded triangle, holes bulge out of the front face.
      const wedge = new THREE.Group();
      wedge.position.set(0, 2.3, -1.9);
      g.add(wedge);
      const profile = new THREE.Shape([
        new THREE.Vector2(-0.6, 0), new THREE.Vector2(0.6, 0),
        new THREE.Vector2(0.6, 0.62), new THREE.Vector2(-0.6, 0.24),
      ]);
      const body = reg.add(new THREE.ExtrudeGeometry(profile, { depth: 0.5, bevelEnabled: false }));
      body.translate(0, 0, -0.25);
      mesh(body, mat(0xffd24a, { roughness: 0.5 }), 1, 1, 1, 0, 0, 0, wedge);
      const holeMat = mat(0xf2b531, { roughness: 0.5 });
      [[-0.35, 0.12, 0.06], [-0.05, 0.15, 0.08], [0.25, 0.2, 0.07], [0.05, 0.3, 0.06], [0.45, 0.4, 0.05]].forEach(([x, y, r]) =>
        mesh(geo.sph, holeMat, r, r, r * 0.35, x, y, 0.25, wedge, false));
      mesh(geo.box, darkMat, 0.16, 0.6, 0.16, 0, 1.3, -1.0, g);
    } else if (name === "toppings") {
      // Mushroom, pepperoni and tomato on the back band, kept at x <= 0.8 so the lamp tower stays clear.
      mesh(geo.cyl, mat(0xf4eee0), 0.12, 0.2, 0.12, -0.6, 2.21, -1.9, g); // mushroom stem
      mesh(geo.sph, mat(0xb98a5a, { roughness: 0.7 }), 0.24, 0.12, 0.24, -0.6, 2.4, -1.9, g); // mushroom cap
      const pepMat = mat(0xb3262a, { roughness: 0.6 });
      mesh(geo.cyl, pepMat, 0.2, 0.05, 0.2, -0.05, 2.22, -1.9, g); // pepperoni slice
      mesh(geo.cyl, pepMat, 0.2, 0.05, 0.2, -0.05, 2.28, -1.9, g); // second slice on top
      mesh(geo.sph, mat(0xe0402a, { roughness: 0.5 }), 0.24, 0.24, 0.24, 0.5, 2.35, -1.9, g); // tomato
      mesh(geo.cyl, mat(0x3f8f3f), 0.12, 0.03, 0.12, 0.5, 2.6, -1.9, g, false); // tomato calyx
    } else if (name === "bake") {
      // Wood-fired oven on the back band, same footprint spot as the package carton.
      // Base and dome are mosaic-tiled like the reference oven.
      const tileMat = mat(0xffffff, { vertexColors: true, roughness: 0.6 });
      const tileRand = mulberry32(11);
      const tileBase = mosaicGeometry(THREE, reg, new THREE.BoxGeometry(1, 1, 1), TILE_COLORS, tileRand);
      const tileDome = mosaicGeometry(THREE, reg, new THREE.SphereGeometry(1, 14, 10), TILE_COLORS, tileRand);
      mesh(tileBase, tileMat, 1.5, 0.45, 1.1, 0, 2.325, -1.9, g); // oven base
      mesh(tileDome, tileMat, 0.75, 0.45, 0.55, 0, 2.55, -1.9, g); // tiled dome
      mesh(geo.cyl, darkMat, 0.09, 0.5, 0.09, 0.48, 3.1, -2.1, g); // flue
      mesh(geo.box, darkMat, 0.62, 0.3, 0.06, 0, 2.3, -1.34, g, false); // oven mouth
      mesh(
        geo.box,
        mat(0xff8a3d, { emissive: 0xff6a1f, emissiveIntensity: 0.5 }),
        0.44, 0.1, 0.02, 0, 2.24, -1.305, g, false,
      ); // embers
      glow = mesh(geo.box, glowMat, 2.0, 0.9, 0.08, 0, 1.1, -1.12, g, false);
      glow.visible = false;
    } else if (name === "slice") {
      // Pizza cutter: a vertical blade wheel on a mast with a handle, so the station reads as a cutter.
      const cutter = new THREE.Group();
      // Same spot as the package station's carton (centre of the back band).
      cutter.position.set(0, 2.45, -1.9);
      mesh(geo.box, darkMat, 0.1, 0.55, 0.1, 0, -0.275, -0.34, cutter, false); // mast on the canopy
      mesh(geo.box, darkMat, 0.08, 0.08, 0.4, 0, 0, -0.17, cutter, false); // bracket to the axle
      const axle = new THREE.Group();
      axle.rotation.x = Math.PI / 2; // blade plane stands vertical and faces the camera
      const blade = mesh(geo.cyl, mat(0xaeb9cc, { metalness: 0.35, roughness: 0.45 }), 0.5, 0.04, 0.5, 0, 0, 0, axle, false);
      blade.castShadow = false;
      mesh(geo.cyl, mat(0x8a93a6), 0.55, 0.02, 0.55, 0, -0.03, 0, axle, false); // darker rim outline
      mesh(geo.cyl, darkMat, 0.1, 0.16, 0.1, 0, 0, 0, axle, false); // axle hub
      const dot = mesh(geo.sph, darkMat, 0.05, 0.05, 0.05, 0.32, 0.04, 0, blade, false);
      dot.castShadow = false;
      spinners.push({ o: blade, speed: 7 });
      cutter.add(axle);
      const tilt = -0.6;
      mesh(geo.cyl, darkMat, 0.06, 0.85, 0.06, 0.2, 0.3, 0, cutter, false).rotation.z = tilt;
      mesh(geo.cyl, darkMat, 0.12, 0.5, 0.12, 0.36, 0.52, 0, cutter, false).rotation.z = tilt;
      g.add(cutter);
    } else if (name === "package") {
      // Flat square pizza boxes, two stacked, kraft-brown cardboard with the shop name on the top lid.
      const cardMat = mat(0xb48a5a);
      mesh(geo.box, cardMat, 1.3, 0.14, 1.3, 0, 2.2, -1.9, g);
      mesh(geo.box, cardMat, 1.3, 0.14, 1.3, 0, 2.36, -1.9, g);
      const lidCanvas = document.createElement("canvas");
      lidCanvas.width = 256;
      lidCanvas.height = 256;
      const lg = lidCanvas.getContext("2d");
      lg.fillStyle = "#3b2412";
      lg.font = "700 40px Georgia, serif";
      lg.textAlign = "center";
      lg.textBaseline = "middle";
      lg.fillText("Ting's Pizza", 128, 128); // canvas text only, see T-05-48
      const lidTex = reg.add(new THREE.CanvasTexture(lidCanvas));
      lidTex.colorSpace = THREE.SRGBColorSpace;
      const lidMat = reg.add(new THREE.MeshBasicMaterial({ map: lidTex, transparent: true }));
      const lid = new THREE.Mesh(reg.add(new THREE.PlaneGeometry(1.2, 1.2)), lidMat);
      lid.rotation.x = -Math.PI / 2; // lie flat on the lid, text reading left to right from the front
      lid.position.set(0, 2.436, -1.9);
      g.add(lid);
    }

    const label = makeLabelSprite(THREE, reg, name);
    label.sprite.position.set(0, 4.1, -1.9);
    g.add(label.sprite);
    const badge = makeBadgeSprite(THREE, reg);
    badge.sprite.position.set(-W / 2 - 0.5, 3.3, -1.0);
    badge.sprite.visible = false;
    g.add(badge.sprite);
    const offlineMark = makeLabelSprite(THREE, reg, "?", { width: 1.3, maxChars: 2, stroke: "#e08a00" });
    offlineMark.sprite.position.set(0, 3.7, -1.9);
    offlineMark.sprite.visible = false;
    g.add(offlineMark.sprite);
    let flashTimer = null;

    const handle = {
      name,
      index: i,
      group: g,
      spinners,
      glow,
      status: "offline",
      bandActive: false,
      queue: 0,
      setStatus(status) {
        handle.status = status;
        if (flashTimer) return; // the repaired flash owns the light for 600 ms
        const busy = status === "busy";
        const down = status === "down" || status === "failed" || status === "error";
        // idle and busy both light the green lamp; down lights red; anything else leaves all lamps dark.
        showLamp(busy || status === "idle" ? "busy" : down ? "down" : null);
        if (glow) glow.visible = busy;
      },
      // blink (red down lamp) toggles emissive only; paused/reduced-motion callers skip it
      setBlink(on) {
        if (handle.status === "down" || handle.status === "failed" || handle.status === "error") {
          lamps.down.emissive.setHex(on ? LAMP_ON.down : 0x000000);
          lamps.down.emissiveIntensity = on ? 0.6 : 0;
        }
      },
      setBandActive(active) {
        if (handle.bandActive === active) return;
        handle.bandActive = active;
        bandMat.color.setHex(active ? COLORS.bandActive : COLORS.bandIdle);
      },
      setQueueBadge(n) {
        if (handle.queue === n && badge.sprite.visible === n > 0) return;
        handle.queue = n;
        badge.sprite.visible = n > 0;
        if (n > 0) { badge.paint(n); badge.tex.needsUpdate = true; }
      },
      // Offline: all lamps dark plus a small amber "?" above the station.
      setOffline(on) {
        offlineMark.sprite.visible = Boolean(on);
        if (on) showLamp(null);
      },
      // Down: progress bar turns amber and stays frozen (no animation).
      setPaused(on) {
        barMat.color.setHex(on ? COLORS.warn : COLORS.bandActive);
      },
      // Text-only DOWN cue used with reduced motion (static red light plus floating label).
      setDownLabel(on) {
        const t = on ? `${name} DOWN` : name;
        if (handle.labelText === t) return;
        handle.labelText = t;
        label.setText(t);
      },
      flashRepaired() {
        showLamp("busy");
        lamps.busy.emissiveIntensity = 0.8;
        if (flashTimer) clearTimeout(flashTimer);
        flashTimer = setTimeout(() => {
          flashTimer = null;
          handle.setStatus(handle.status);
        }, 600);
      },
      setProgress(fraction) {
        const f = Math.max(0, Math.min(1, fraction || 0));
        bar.visible = f > 0;
        bar.scale.x = Math.max(0.001, 1.8 * f);
        bar.position.x = -0.9 + 0.9 * f;
      },
    };
    handle.setStatus("offline");
    handles.push(handle);
  });
  return { group, handles };
}

// Stocker label options. The label canvas is 256 px wide, so a longer text such as
// "Stocker  WAIT 5 / BANK 0" needs a smaller font (and a wider sprite to keep it readable).
export const STOCKER_LABEL_OPTS = { width: 7.2, maxChars: 30, fontPx: 15 };

// Stocker: a white platform with a mint top behind the stations; its rows hold WAIT and BANK lots.
export function buildStocker(THREE, reg) {
  const { geo, mat, mesh } = makeKit(THREE, reg);
  const group = new THREE.Group();
  const cx = STOCKER_X0 + ((STOCKER_COLS - 1) * 0.78) / 2;
  // depth covers all four rows plus the lap offsets (last lot reaches z0 - 5.1) so shelves stay on the slab.
  const depth = STOCKER_DEPTH;
  const cz = STOCKER_Z0 - 2.2;
  const width = STOCKER_COLS * 0.78 + 1.2;
  mesh(geo.box, mat(COLORS.body), width, 0.2, depth, cx, 0.1, cz, group);
  mesh(geo.box, mat(COLORS.mint), width, 0.04, depth, cx, 0.21, cz, group, false);

  // Warehouse shell, open on the +z side so lots stay visible from the camera.
  const zFront = cz + depth / 2;
  const zBack = cz - depth / 2;
  const wallH = 2.0;
  const wallMat = mat(COLORS.canopy);
  const darkMat = mat(COLORS.dark);
  mesh(geo.box, wallMat, width, wallH, 0.12, cx, 0.2 + wallH / 2, zBack + 0.06, group);
  for (const sx of [-1, 1]) {
    mesh(geo.box, wallMat, 0.12, wallH, depth, cx + sx * (width / 2 - 0.06), 0.2 + wallH / 2, cz, group);
    mesh(geo.box, darkMat, 0.16, wallH, 0.16, cx + sx * (width / 2 - 0.08), 0.2 + wallH / 2, zFront - 0.08, group);
  }
  mesh(geo.box, darkMat, width, 0.18, 0.18, cx, 0.2 + wallH + 0.09, zFront - 0.09, group);

  // Shelves: one board per kind row (matches stockerSlot rows), rails between rows, uprights at both ends.
  const boardMat = mat(COLORS.bandIdle);
  const boardW = width - 0.5;
  for (let row = 0; row < STOCKER_KINDS.length; row++) {
    const zc = STOCKER_Z0 - row * 1.4 - 0.45;
    mesh(geo.box, boardMat, boardW, 0.06, 1.4, cx, BELT_Y - 0.01, zc, group, false);
  }
  for (let r = 0; r <= STOCKER_KINDS.length; r++) {
    const zr = STOCKER_Z0 - r * 1.4 + 0.25;
    mesh(geo.box, darkMat, boardW, 0.05, 0.05, cx, 1.1, zr, group, false);
    for (const sx of [-1, 1]) {
      mesh(geo.box, darkMat, 0.07, 1.1, 0.07, cx + sx * (width / 2 - 0.35), 0.2 + 0.55, zr, group, false);
    }
  }
  const label = makeLabelSprite(THREE, reg, "Stocker - empty", { ...STOCKER_LABEL_OPTS });
  label.sprite.position.set(cx, 2.2, STOCKER_Z0 + 0.8);
  group.add(label.sprite);
  group.userData = { title: "" };
  return {
    group,
    setLabel(text, title) {
      if (group.userData.text !== text) {
        group.userData.text = text;
        label.setText(text);
      }
      group.userData.title = title || "";
    },
  };
}

// Conveyor lanes joining the stations to the Stocker. Boxes, cylinders and cones only (no text).
export function buildLanes(THREE, reg) {
  const { geo, mat, mesh } = makeKit(THREE, reg);
  const group = new THREE.Group();
  group.userData = { kind: "lanes" };
  const outMat = mat(COLORS.belt);
  const retMat = mat(COLORS.bandIdle);
  const darkMat = mat(COLORS.dark);
  const DECK_Y = 0.17;
  const T = 0.1;
  const N = STATION_NAMES.length;
  // deck running along z at fixed x, or along x at fixed z
  const deckZ = (m, x, z0, z1) => mesh(geo.box, m, LANE_W, T, Math.abs(z1 - z0), x, DECK_Y, (z0 + z1) / 2, group, false);
  const deckX = (m, x0, x1, z, y = DECK_Y) => mesh(geo.box, m, Math.abs(x1 - x0), T, LANE_W, (x0 + x1) / 2, y, z, group, false);
  const railX = (x0, x1, z) => {
    for (const dz of [-1, 1]) mesh(geo.box, darkMat, Math.abs(x1 - x0), 0.08, 0.05, (x0 + x1) / 2, DECK_Y + 0.08, z + dz * (LANE_W / 2), group, false);
  };

  // to-Stocker lane (flows west) and entry leg
  const outX0 = ENTRY_X - LANE_W / 2;
  const outX1 = spurX(N - 1) + LANE_W / 2;
  deckX(outMat, outX0, outX1, LANE_OUT_Z);
  railX(outX0, outX1, LANE_OUT_Z);
  deckZ(outMat, ENTRY_X, LANE_OUT_Z, STOCKER_FRONT_Z);
  // exit leg
  deckZ(retMat, EXIT_X, STOCKER_FRONT_Z, LANE_RET_Z);

  // return lane (flows east) split around a hop at every spur
  const retX1 = dropX(N - 1) + LANE_W / 2;
  let cursor = EXIT_X - LANE_W / 2;
  const ang = Math.atan(HOP_H / HOP_RAMP);
  const rampLen = Math.hypot(HOP_RAMP, HOP_H);
  for (let j = 0; j < N; j++) {
    const sx = spurX(j);
    const a = sx - HOP_HALF;
    const b = sx + HOP_HALF;
    if (a > cursor) { deckX(retMat, cursor, a, LANE_RET_Z); railX(cursor, a, LANE_RET_Z); }
    deckX(retMat, a + HOP_RAMP, b - HOP_RAMP, LANE_RET_Z, DECK_Y + HOP_H);
    const left = mesh(geo.box, retMat, rampLen, T, LANE_W, a + HOP_RAMP / 2, DECK_Y + HOP_H / 2, LANE_RET_Z, group, false);
    left.rotation.z = ang;
    const right = mesh(geo.box, retMat, rampLen, T, LANE_W, b - HOP_RAMP / 2, DECK_Y + HOP_H / 2, LANE_RET_Z, group, false);
    right.rotation.z = -ang;
    cursor = b;
  }
  if (retX1 > cursor) { deckX(retMat, cursor, retX1, LANE_RET_Z); railX(cursor, retX1, LANE_RET_Z); }

  for (let i = 0; i < N; i++) {
    deckZ(outMat, spurX(i), -0.4, LANE_OUT_Z); // spur
    deckZ(retMat, dropX(i), LANE_RET_Z, -0.4); // drop
  }

  // flow arrows at the midpoint of each long leg
  const arrow = (x, z, rotZ) => {
    const c = mesh(geo.cone, darkMat, 0.22, 0.5, 0.22, x, DECK_Y + 0.12, z, group, false);
    c.rotation.z = rotZ;
  };
  arrow((outX0 + outX1) / 2, LANE_OUT_Z, Math.PI / 2); // westward
  arrow((EXIT_X + retX1) / 2, LANE_RET_Z, -Math.PI / 2); // eastward
  return group;
}

// "SCRAPPED 999" is 12 chars: 15 * 20 * 0.55 = 165 px, still inside the 240 px canvas.
export const SCRAP_LABEL_OPTS = { width: 7.2, maxChars: 34, fontPx: 13, stroke: "#e5484d" };

// Scrap zone: a neutral floor apron marks the zone; the conveyor on it (deck, inclined
// head) tips failed lots into a wheeled square can at the east end, past package.
export function buildScrapZone(THREE, reg) {
  const { geo, mat, mesh } = makeKit(THREE, reg);
  const group = new THREE.Group();
  const red = mat(COLORS.lightDown); // structure only: can rim
  const floor = mat(COLORS.bandIdle); // the floor itself is deliberately not red
  const dark = mat(COLORS.dark);
  // Same striped top as buildBelt, one shared texture for the whole scrap conveyor.
  const c = document.createElement("canvas");
  c.width = 64;
  c.height = 16;
  const g = c.getContext("2d");
  g.fillStyle = "#ffffff";
  g.fillRect(0, 0, 64, 16);
  g.fillStyle = "#c4cbd9";
  g.fillRect(0, 0, 8, 16);
  g.fillRect(32, 0, 8, 16);
  const tex = reg.add(new THREE.CanvasTexture(c));
  tex.wrapS = tex.wrapT = THREE.RepeatWrapping;
  tex.repeat.set(24, 1);
  const belt = reg.add(new THREE.MeshStandardMaterial({ color: COLORS.belt, roughness: 0.9, map: tex }));

  // Floor apron: neutral zone marking from the deck west end to clear of the can.
  const apronLen = SCRAP_APRON_X1 - SCRAP_X0;
  const apronCx = (SCRAP_X0 + SCRAP_APRON_X1) / 2;
  mesh(geo.box, floor, apronLen, 0.04, 2 * (SCRAP_APRON_Z1 - SCRAP_Z),
    apronCx, 0.02, SCRAP_Z, group, false);

  // Per-station spurs: a short deck in front of every station feeding the scrap belt. The
  // ground strip runs from just outside the main belt to the apron edge (they abut,
  // never overlap, so the two coplanar faces cannot z-fight).
  const apronNearZ = SCRAP_Z - 1.8;
  for (let i = 0; i < STATION_NAMES.length; i++) {
    const x = scrapEntryX(i);
    const spurLen = SCRAP_SPUR_Z1 - SCRAP_SPUR_Z0;
    const spurZc = (SCRAP_SPUR_Z0 + SCRAP_SPUR_Z1) / 2;
    // strip half-width (0.65) stays under half the minimum junction gap (0.7) so strips abut, never overlap
    mesh(geo.box, floor, SCRAP_SPUR_W + 0.3, 0.04, apronNearZ - SCRAP_SPUR_Z0 + 0.05,
      x, 0.02, (SCRAP_SPUR_Z0 - 0.05 + apronNearZ) / 2, group, false);
    mesh(geo.box, dark, SCRAP_SPUR_W + 0.2, 0.22, spurLen, x, 0.11, spurZc, group);
    mesh(geo.box, belt, SCRAP_SPUR_W, 0.06, spurLen, x, 0.26, spurZc, group, false);
  }

  // Flat section: frame, deck, legs.
  const flatLen = SCRAP_RAMP_X0 - SCRAP_X0;
  const flatCx = (SCRAP_X0 + SCRAP_RAMP_X0) / 2;
  mesh(geo.box, dark, flatLen, 0.22, SCRAP_W + 0.3, flatCx, 0.11, SCRAP_Z, group);
  mesh(geo.box, belt, flatLen, 0.06, SCRAP_W, flatCx, 0.26, SCRAP_Z, group, false);
  for (const x of [SCRAP_X0 + 1, flatCx, SCRAP_RAMP_X0 - 1]) {
    for (const dz of [-1, 1]) {
      mesh(geo.box, dark, 0.1, 0.26, 0.1, x, 0.13, SCRAP_Z + dz * 0.55, group, false);
    }
  }

  // Incline up to the head, written the same way as the lane hops in buildLanes.
  const rampLen = Math.hypot(SCRAP_X1 - SCRAP_RAMP_X0, SCRAP_HEAD_Y - SCRAP_Y);
  const rampAngle = Math.atan2(SCRAP_HEAD_Y - SCRAP_Y, SCRAP_X1 - SCRAP_RAMP_X0);
  const rampCx = (SCRAP_RAMP_X0 + SCRAP_X1) / 2;
  const rampCy = (SCRAP_Y + SCRAP_HEAD_Y) / 2 - 0.03;
  const ramp = mesh(geo.box, belt, rampLen, 0.06, SCRAP_W, rampCx, rampCy, SCRAP_Z, group, false);
  ramp.rotation.z = rampAngle;

  // Head: short discharge section that sits over the can mouth.
  const headLen = SCRAP_X2 - SCRAP_X1;
  const headCx = (SCRAP_X1 + SCRAP_X2) / 2;
  mesh(geo.box, belt, headLen, 0.06, SCRAP_W, headCx, SCRAP_HEAD_Y - 0.03, SCRAP_Z, group, false);

  // Wheeled square can: four walls and a floor leave the top open so the failed lots inside
  // stay visible; the red rim is four edge bars, not a lid.
  const binW = 2 * SCRAP_BIN_R;
  const wallT = 0.12;
  mesh(geo.box, dark, binW, SCRAP_BIN_H, wallT, SCRAP_BIN_X, SCRAP_BIN_H / 2, SCRAP_Z - SCRAP_BIN_R + wallT / 2, group);
  mesh(geo.box, dark, binW, SCRAP_BIN_H, wallT, SCRAP_BIN_X, SCRAP_BIN_H / 2, SCRAP_Z + SCRAP_BIN_R - wallT / 2, group);
  mesh(geo.box, dark, wallT, SCRAP_BIN_H, binW, SCRAP_BIN_X - SCRAP_BIN_R + wallT / 2, SCRAP_BIN_H / 2, SCRAP_Z, group);
  mesh(geo.box, dark, wallT, SCRAP_BIN_H, binW, SCRAP_BIN_X + SCRAP_BIN_R - wallT / 2, SCRAP_BIN_H / 2, SCRAP_Z, group);
  mesh(geo.box, dark, binW, 0.08, binW, SCRAP_BIN_X, 0.04, SCRAP_Z, group, false);
  mesh(geo.box, mat(COLORS.bandIdle), 2.2, 0.04, 2.2, SCRAP_BIN_X, 0.08, SCRAP_Z, group, false);
  for (const dz of [-1, 1]) {
    mesh(geo.box, red, binW + 0.12, 0.1, 0.12, SCRAP_BIN_X, SCRAP_BIN_H, SCRAP_Z + dz * (SCRAP_BIN_R + 0.02), group, false);
  }
  for (const dx of [-1, 1]) {
    mesh(geo.box, red, 0.12, 0.1, binW + 0.12, SCRAP_BIN_X + dx * (SCRAP_BIN_R + 0.02), SCRAP_BIN_H, SCRAP_Z, group, false);
  }
  for (const dx of [-1, 1]) {
    for (const dz of [-1, 1]) {
      mesh(geo.box, dark, 0.12, 0.9, 0.12, SCRAP_BIN_X + dx * (SCRAP_BIN_R - 0.02), 0.5, SCRAP_Z + dz * (SCRAP_BIN_R - 0.02), group, false);
    }
  }
  for (const dz of [-1, 1]) {
    for (const dx of [-0.4, 0.4]) {
      mesh(geo.box, dark, 0.12, 0.9, 0.12, SCRAP_BIN_X + dx, 0.5, SCRAP_Z + dz * (SCRAP_BIN_R + 0.02), group, false);
    }
  }
  for (const dx of [-0.9, 0.9]) {
    for (const dz of [-0.9, 0.9]) {
      const wheel = mesh(geo.cyl, dark, 0.14, 0.1, 0.14, SCRAP_BIN_X + dx, 0.14, SCRAP_Z + dz, group);
      wheel.rotation.x = Math.PI / 2;
    }
  }

  const label = makeLabelSprite(THREE, reg, "SCRAPPED 0 lots / 0 pizzas", { ...SCRAP_LABEL_OPTS });
  label.sprite.position.set(SCRAP_BIN_X, SCRAP_BIN_H + 2.4, SCRAP_Z);
  group.add(label.sprite);
  group.userData = { kind: "scrap", text: "" };
  return {
    group,
    setLabel(text) {
      if (group.userData.text !== text) {
        group.userData.text = text;
        label.setText(text);
      }
    },
  };
}

export function buildGate(THREE, reg) {
  const { geo, mat, mesh } = makeKit(THREE, reg);
  const gate = new THREE.Group();
  gate.position.set(GATE_X, 0, 0);
  const dark = mat(COLORS.dark);
  mesh(geo.box, dark, 0.2, 1.8, 0.2, 0, 0.9, -1.2, gate);
  mesh(geo.box, dark, 0.2, 1.8, 0.2, 0, 0.9, 1.2, gate);
  mesh(geo.box, mat(COLORS.bandIdle), 0.3, 0.3, 2.8, 0, 1.85, 0, gate);
  return gate;
}

export function buildTruck(THREE, reg) {
  const { geo, mat, mesh } = makeKit(THREE, reg);
  const tr = new THREE.Group();
  tr.position.set(TRUCK_X + 1.2, 0, 0);
  mesh(geo.box, mat(0xffffff), 3.6, 2.0, 2.2, 0.6, 1.5, 0, tr);
  mesh(geo.box, mat(0x6f8ed8), 1.3, 1.5, 2.1, -1.8, 1.25, 0, tr);
  mesh(geo.box, mat(0xbfd6ff), 0.06, 0.7, 1.6, -2.46, 1.5, 0, tr);
  const wheel = mat(0x222831);
  [[-1.8, 1], [-1.8, -1], [1.2, 1], [1.2, -1]].forEach(([x, z]) => {
    const w = mesh(geo.cyl, wheel, 0.42, 0.3, 0.42, x, 0.42, z * 1.12, tr);
    w.rotation.x = Math.PI / 2;
  });
  mesh(geo.box, mat(COLORS.bandIdle), 3.62, 0.22, 2.22, 0.6, 2.1, 0, tr);
  return tr;
}

export function buildDecor(THREE, reg) {
  const { geo, mat, mesh } = makeKit(THREE, reg);
  const decor = new THREE.Group();
  const leaf = mat(0x7fd1ae);
  const trunk = mat(0x8a6a4a);
  const wall = mat(0xffffff);
  const roof = mat(0xb9c8e6);
  const rnd = mulberry32(3);
  const tree = (x, z, s) => {
    const g = new THREE.Group();
    mesh(geo.cyl, trunk, 0.18 * s, 0.9 * s, 0.18 * s, 0, 0.45 * s, 0, g);
    mesh(geo.cone, leaf, 0.9 * s, 1.8 * s, 0.9 * s, 0, 1.6 * s, 0, g);
    mesh(geo.cone, leaf, 0.7 * s, 1.4 * s, 0.7 * s, 0, 2.5 * s, 0, g);
    g.position.set(x, 0, z);
    decor.add(g);
  };
  for (let i = 0; i < 26; i++) {
    const x = -40 + rnd() * 80;
    const z = (rnd() > 0.5 ? 1 : -1) * (6 + rnd() * 12);
    const sc = 0.8 + rnd() * 0.7;
    if (z < -3.5 && z > -8.0) continue; // keep the lanes clear (random sequence unchanged)
    if (z > 2.4 && z < 9.0 && x > SCRAP_X0 - 2) continue; // keep trees out of the scrap conveyor
    tree(x, z, sc);
  }
  const bld = (x, z, w, h, d) => {
    mesh(geo.box, wall, w, h, d, x, h / 2, z, decor);
    mesh(geo.box, roof, w * 1.04, 0.3, d * 1.04, x, h + 0.15, z, decor);
  };
  // Pushed behind the stocker (z0 -13, footprint to about -18.5) so they do not hide it.
  bld(-24, -22, 9, 4, 5);
  bld(-12, -22, 6, 3, 5);
  bld(14, -13, 10, 5, 5);
  bld(30, -11, 6, 3, 4);
  bld(-30, 12, 7, 3, 5);
  bld(8, 14, 7, 3.4, 4);
  const crate = mat(0xe9eef8);
  [[-30, 6.6], [-27.6, 7.4], [32, -6.8], [34.4, -6.2]].forEach(([x, z]) => mesh(geo.box, crate, 1.6, 1.1, 1.6, x, 0.58, z, decor));

  // Three low-poly puppet figures (boy, chef with dough, girl) in the open back area, behind the stocker
  // and between the buildings. Built from the same primitives as the rest of the decor.
  const PUPPET_Z = -19;
  const puppet = (x, s) => {
    const f = new THREE.Group();
    f.position.set(x, 0, PUPPET_Z);
    decor.add(f);
    const skin = mat(s.skin);
    const hair = mat(s.hair);
    const shirt = mat(s.shirt);
    const pants = mat(s.pants);
    const white = mat(0xf7f7f7);
    // legs and torso
    mesh(geo.box, pants, 0.36, 0.7, 0.42, -0.22, 0.35, 0, f);
    mesh(geo.box, pants, 0.36, 0.7, 0.42, 0.22, 0.35, 0, f);
    mesh(geo.box, shirt, 1.0, 1.2, 0.6, 0, 1.3, 0, f);
    if (s.stripes) for (const y of [1.0, 1.25, 1.5]) mesh(geo.box, mat(s.stripes), 1.02, 0.07, 0.62, 0, y, 0, f, false);
    if (s.buttons) for (const y of [1.0, 1.3, 1.6]) mesh(geo.sph, mat(s.buttons), 0.05, 0.05, 0.05, 0, y, 0.33, f, false);
    // arms raised up and outward, with a hand at the top
    for (const sx of [-1, 1]) {
      const a = 0.6; // tilt from vertical
      const shoulder = { x: sx * 0.55, y: 1.8 };
      const arm = mesh(geo.cyl, skin, 0.14, 0.9, 0.14,
        shoulder.x + sx * Math.sin(a) * 0.45, shoulder.y + Math.cos(a) * 0.45, 0, f);
      arm.rotation.z = -sx * a;
      mesh(geo.sph, skin, 0.14, 0.14, 0.14,
        shoulder.x + sx * Math.sin(a) * 0.9, shoulder.y + Math.cos(a) * 0.9, 0, f, false);
    }
    // head, hair and face
    mesh(geo.sph, skin, 0.5, 0.5, 0.48, 0, 2.7, 0, f);
    mesh(geo.sph, hair, 0.52, 0.28, 0.5, 0, 2.98, -0.04, f, false);
    for (const ex of [-0.17, 0.17]) mesh(geo.sph, mat(0x1d1a1a), 0.05, 0.05, 0.05, ex, 2.76, 0.45, f, false);
    mesh(geo.sph, mat(s.nose), 0.09, 0.09, 0.09, 0, 2.66, 0.5, f, false);
    if (s.beard) mesh(geo.sph, white, 0.36, 0.2, 0.3, 0, 2.45, 0.36, f, false);
    if (s.hat) {
      mesh(geo.cyl, white, 0.36, 0.45, 0.36, 0, 3.3, 0, f);
      mesh(geo.sph, white, 0.38, 0.22, 0.38, 0, 3.58, 0, f);
    }
    if (s.pigtails) {
      for (const px of [-0.5, 0.5]) {
        mesh(geo.sph, hair, 0.18, 0.18, 0.18, px, 2.8, -0.05, f);
        mesh(geo.box, mat(0x3c6fd6), 0.2, 0.08, 0.1, px, 3.0, 0.02, f, false);
      }
    }
    return f;
  };
  puppet(-5.5, { skin: 0xc98a4e, hair: 0x1d1a1a, shirt: 0x3c6fd6, stripes: 0xf08a3c, pants: 0x2b4a8a, nose: 0xe8786a });
  puppet(0, { skin: 0xf0a040, hair: 0xf5f0e6, shirt: 0xf7f7f7, buttons: 0x555a66, pants: 0x333a4a, beard: true, hat: true, nose: 0xe86a3c });
  puppet(5.5, { skin: 0xf08aa8, hair: 0xf2d46a, shirt: 0xf6d24a, pants: 0x2b4a8a, pigtails: true, nose: 0xd9607f });
  return decor;
}
