// Lots in the 3D scene: meshes, queue slots, fact-driven animation, authoritative reconcile,
// picking and the selection bracket. The pure helpers at the top (rankOf, diffLots) have no
// THREE dependency so node tests can cover the reconcile/exemption logic.
import {
  STATION_NAMES, STOCKER_KINDS, queueSlot, stationSlot, gateSlot, truckSlot, stockerSlot, isStockerReturn,
  scrapSlot, routeToScrap,
  makeLabelSprite, COLORS, routeToStocker, routeFromStocker, pathLength, pointAt, travelDuration, easeInOut,
} from "./scene-objects.js";
import { animationDt } from "./speed.js";
import {
  partsFor, lotPickBox, createPool, buildStageGeometries, buildOutlineGeometry, stageHasDots, clampQty,
} from "./lots-instancing.js";
import { isHighPriority, PRIORITY_DEFAULT } from "./priority.js";

// High-priority lots (priority <= 10) render 1.18x larger (D-15).
// Start time of the lot's current step: summary field first, legacy full-shape steps map second.
export function stepStartedTs(lot) {
  if (!lot) return null;
  if (lot.step_started_ts) return lot.step_started_ts;
  const st = lot.steps && lot.steps[lot.station];
  return st && st.started_ts ? st.started_ts : null;
}

export function lotScale(priority) {
  return (isHighPriority(priority) ? 1.18 : 1) * 1.4;
}

const MAX_LIVE = 120;
const MAX_BADGES = 80;
const EASE_RATE = 5;
const SETTLE_EPS = 1e-4;
const EXIT_RATE = 3.2;
const SHRINK_SECONDS = 0.45;
const LAST = STATION_NAMES.length - 1;
const GHOST_OPACITY = 0.5;
const PRODUCT_TOP = { margherita: 0x3f8f3f, pepperoni: 0xa42424, veggie: 0xe08a00 };

// Position of a lot along the line, used to ignore snapshots that lag behind the event stream.
export function rankOf(lot) {
  if (!lot) return -1;
  if (lot.state === "done") return Infinity;
  if (lot.state === "pre_release") return -1;
  const idx = STATION_NAMES.indexOf(lot.station);
  if (idx < 0) return -1;
  return idx * 2 + (lot.state === "running" ? 1 : 0);
}

// Where a lot lives in the scene. Stocker covers WAIT lots and BANK lots; failed lots are scrap.
export function zoneOf(lot) {
  if (!lot) return "queue";
  if (lot.stocker || lot.state === "banked") return "stocker";
  if (lot.state === "failed") return "scrap";
  if (lot.state === "running") return "run";
  if (lot.state === "pre_release") return "gate";
  if (lot.state === "done") return "exit";
  return "queue";
}

// Any rework WAIT: cooling in the hold queue, back in the queue after it, or dispatched.
function retryWaiting(lot) {
  return lot.state === "queued" && (lot.visit || 1) > 1 && Boolean(lot.retry_since_ts);
}

// Visual treatment of a lot: ghosted solid outline for WAIT, dashed for BANK, amber R-badge for rework.
export function lotVisual(lot) {
  if (lot && lot.state === "banked") return { opacity: 0.5, outline: "dashed" };
  if (lot && lot.stocker) {
    const k = lot.stocker.kind;
    const tint = k === "station_down" || k === "station_offline" ? "amber" : null;
    return { opacity: 0.5, outline: "solid", tint };
  }
  if (lot && retryWaiting(lot)) return { tint: "amber", badge: `R${lot.visit}` };
  return { opacity: 1, outline: null, tint: null, badge: null };
}

export function stockerLabel(counts) {
  const c = counts || {};
  const w = (c.stocker_overflow || 0) + (c.stocker_down || 0);
  const b = c.bank || 0;
  if (w === 0 && b === 0) return "Stocker - empty";
  return `Stocker  WAIT ${w} / BANK ${b}`;
}

export function stockerTitle(counts) {
  const c = counts || {};
  const o = c.stocker_overflow || 0;
  const d = c.stocker_down || 0;
  return `WAIT ${o + d} counts in WIP (${o} queue full, ${d} station down/offline). BANK ${c.bank || 0} does not.`;
}

// DLQ counter on the trash can: always shows a number, straight from line.counts.failed.
export function scrapLabel(counts) {
  const n = (v) => (typeof v === "number" && Number.isFinite(v) && v > 0 ? Math.floor(v) : 0);
  return `SCRAPPED ${n(counts && counts.failed)} lots / ${n(counts && counts.failed_pizzas)} pizzas`;
}

// Diff an authoritative lot list against the lots we know. Lots that are riding to the truck
// (exiting) are exempt from removal until their animation ends.
export function diffLots(known, lots) {
  const seen = new Set();
  const spawn = [];
  const update = [];
  const exit = [];
  for (const lot of lots) {
    seen.add(lot.order_id);
    const e = known.get(lot.order_id);
    if (!e) {
      if (lot.state !== "done") spawn.push(lot);
      continue;
    }
    if (lot.state === "done") {
      if (!e.exiting) exit.push(lot);
      continue;
    }
    if (e.exiting) continue;
    const s = e.snap;
    if (
      !s || s.state !== lot.state || s.station !== lot.station || s.steps_done !== lot.steps_done ||
      (s.zone || zoneOf(s)) !== zoneOf(lot)
    ) update.push(lot);
  }
  const remove = [];
  for (const [id, e] of known) if (!seen.has(id) && !e.exiting) remove.push(id);
  return { spawn, update, exit, remove };
}

export function createLots(THREE, scene, stations, store, reg) {
  const group = new THREE.Group();
  scene.add(group);
  const entries = new Map();
  const queues = STATION_NAMES.map(() => []);
  const stockers = Object.fromEntries(STOCKER_KINDS.map((k) => [k, []]));
  const scraps = [];
  const expected = STATION_NAMES.map(() => 3);
  let reconciled = false;

  // ----- instanced pools: no THREE.Group / Mesh per lot -----
  // One InstancedMesh pool per (stage geometry x ghost) plus box and outline pools, so the draw call
  // count is bounded by the number of pools (tens) no matter how many lots are live.
  const stageGeos = buildStageGeometries(THREE, reg);
  const matSolid = reg.add(new THREE.MeshStandardMaterial({ vertexColors: true, roughness: 0.9 }));
  const matGhost = reg.add(matSolid.clone());
  matGhost.transparent = true;
  matGhost.opacity = GHOST_OPACITY;
  const boxSolid = reg.add(new THREE.MeshStandardMaterial({ color: 0xc9a46a, transparent: true, opacity: 0.88 }));
  const boxGhost = reg.add(boxSolid.clone());
  boxGhost.opacity = GHOST_OPACITY;
  const outlineMats = {
    dark: reg.add(new THREE.MeshBasicMaterial({ color: COLORS.dark })),
    amber: reg.add(new THREE.MeshBasicMaterial({ color: COLORS.warn })),
  };
  const outlineGeos = new Map();
  const pools = new Map();
  const poolList = [];
  const touched = [];
  let structureDirty = false;
  let uploads = 0;

  function poolFor(key, geometry, material, castShadow) {
    let p = pools.get(key);
    if (!p) {
      p = createPool(THREE, geometry(), material, { castShadow });
      p.dirty = false;
      pools.set(key, p);
      poolList.push(p);
      group.add(p.mesh);
    }
    return p;
  }
  const mark = (p) => { if (!p.dirty) { p.dirty = true; touched.push(p); } };
  function flush() {
    for (const p of touched) {
      p.mesh.instanceMatrix.needsUpdate = true;
      p.dirty = false;
      uploads++;
    }
    touched.length = 0;
  }
  function stagePool(stage, topColor, ghost) {
    const key = `s:${stage}:${stageHasDots(stage) ? topColor : 0}:${ghost ? 1 : 0}`;
    return poolFor(key, () => stageGeos.get(stage, topColor), ghost ? matGhost : matSolid, true);
  }
  function boxPool(ghost) {
    return poolFor(`b:${ghost ? 1 : 0}`, () => stageGeos.get("box"), ghost ? boxGhost : boxSolid, true);
  }
  function outlinePool(qty, style, tint) {
    const key = `o:${qty}:${style}:${tint === "amber" ? "amber" : "dark"}`;
    return poolFor(key, () => {
      const gk = `${qty}:${style}`;
      let geo = outlineGeos.get(gk);
      if (!geo) { geo = reg.add(buildOutlineGeometry(THREE, qty, style)); outlineGeos.set(gk, geo); }
      return geo;
    }, outlineMats[tint === "amber" ? "amber" : "dark"], false);
  }

  // R-badge sprites for rework-waiting lots: few of them, so they stay individual sprites (capped).
  const badges = new Map();
  const freeBadges = [];
  function removeBadge(id) {
    const rec = badges.get(id);
    if (!rec) return;
    rec.lb.sprite.visible = false;
    freeBadges.push(rec.lb);
    badges.delete(id);
  }
  function syncBadge(e) {
    const want = e.vis && e.vis.badge;
    if (!want) { removeBadge(e.id); return; }
    let rec = badges.get(e.id);
    if (!rec && badges.size < MAX_BADGES) {
      let lb = freeBadges.pop();
      if (!lb) {
        lb = makeLabelSprite(THREE, reg, want, { width: 1.1, maxChars: 4, stroke: "#e08a00" });
        group.add(lb.sprite);
      }
      rec = { lb, text: null };
      badges.set(e.id, rec);
    }
    if (!rec) return;
    if (rec.text !== want) { rec.lb.setText(want); rec.text = want; }
    rec.lb.sprite.visible = true;
    rec.lb.sprite.scale.set(1.1 * e.curScale, 0.275 * e.curScale, 1);
  }

  // Rebuild every pool from the entries. Coalesced: runs at most once per update().
  function repack() {
    structureDirty = false;
    const prev = poolList.map((p) => p.count);
    for (const p of poolList) p.setCount(0);
    for (const e of entries.values()) {
      e.slots.length = 0;
      syncBadge(e);
      if (!e.pos) continue;
      const v = e.vis || {};
      const ghost = (v.opacity ?? 1) < 1;
      const S = e.curScale;
      const topColor = PRODUCT_TOP[e.product] ?? 0x3f8f3f;
      for (const part of partsFor(e)) {
        const p = part.box ? boxPool(ghost) : stagePool(part.stage, topColor, ghost);
        const i = p.add();
        p.set(i, e.pos.x, e.pos.y + part.y * S, e.pos.z, S, part.box ? part.sy * S : S, S);
        e.slots.push(p, i, part.y, part.box ? part.sy : 0);
      }
      const style = v.outline || (v.tint ? "solid" : null);
      if (style) {
        const p = outlinePool(clampQty(e.qty), style, v.tint);
        const i = p.add();
        p.set(i, e.pos.x, e.pos.y, e.pos.z, S, S, S);
        e.slots.push(p, i, 0, 0);
      }
    }
    // pools may have been created above; their prev count is 0
    poolList.forEach((p, k) => { if ((prev[k] || 0) > 0 || p.count > 0) mark(p); });
  }

  // Rewrite only this entry's recorded instance slots (it moved but its structure did not change).
  function moveEntry(e) {
    const s = e.slots;
    const S = e.curScale;
    for (let k = 0; k < s.length; k += 4) {
      const p = s[k];
      p.set(s[k + 1], e.pos.x, e.pos.y + s[k + 2] * S, e.pos.z, S, s[k + 3] ? s[k + 3] * S : S, S);
      mark(p);
    }
  }

  function skin(entry) {
    entry.skinned = entry.layers;
    structureDirty = true;
  }

  function listFor(loc) {
    if (!loc) return null;
    if (loc.kind === "queue") return queues[loc.i];
    if (loc.kind === "stocker") return stockers[loc.i];
    if (loc.kind === "scrap") return scraps;
    return null;
  }

  function layoutList(loc) {
    const list = listFor(loc);
    if (!list) return;
    list.forEach((e, k) => {
      if (loc.kind === "queue") e.target = queueSlot(loc.i, k);
      else if (loc.kind === "stocker") e.target = stockerSlot(loc.i, k);
      else if (loc.kind === "scrap") e.target = scrapSlot(k);
    });
  }

  function leaveQueue(entry) {
    const loc = entry.loc;
    const list = listFor(loc);
    if (list) {
      const at = list.indexOf(entry);
      if (at >= 0) list.splice(at, 1);
      layoutList(loc);
    }
  }

  function layoutQueue(i) {
    queues[i].forEach((e, k) => { e.target = queueSlot(i, k); });
  }

  function place(entry, kind, i = 0) {
    if (kind === "stocker" && !stockers[i]) i = "overflow";
    if (entry.loc && entry.loc.kind === kind && entry.loc.i === i) return;
    const prev = entry.loc;
    leaveQueue(entry);
    entry.loc = { kind, i };
    // a return trip still in flight rides on to its station; the op_started that follows must not cut it short
    const keepReturn = kind === "run" && entry.returning && entry.travel;
    // stocked and released lots ride the conveyor lanes; every other move keeps the direct ease
    if (!keepReturn) {
      entry.travel = null;
      entry.returning = false;
    }
    const canRoute = entry.pos && !(entry.instant && !entry.settled);
    if (keepReturn) {
      // already on the return lane toward this station; nothing to re-route
    } else if (canRoute && prev && kind === "stocker" && (prev.kind === "queue" || prev.kind === "run")) {
      const points = routeToStocker(prev.i, entry.pos);
      const len = pathLength(points);
      entry.travel = { points, len, dur: travelDuration(len), t: 0 };
    } else if (canRoute && isStockerReturn(prev, kind)) {
      const points = routeFromStocker(i, entry.pos);
      const len = pathLength(points);
      entry.travel = { points, len, dur: travelDuration(len), t: 0 };
      entry.returning = true;
    } else if (canRoute && prev && kind === "scrap") {
      // a lot coming out of the Stocker enters at the west end instead of crossing the line
      const from = prev.kind === "stocker" ? -1 : prev.i;
      const points = routeToScrap(from, entry.pos, scraps.length);
      const len = pathLength(points);
      entry.travel = { points, len, dur: travelDuration(len), t: 0 };
    }
    if (kind === "queue" || kind === "stocker" || kind === "scrap") {
      listFor(entry.loc).push(entry);
      layoutList(entry.loc);
    } else if (kind === "run") {
      entry.target = stationSlot(i);
      entry.elapsed = entry.elapsed || 0;
    } else if (kind === "gate") {
      entry.target = gateSlot();
    } else if (kind === "exit") {
      entry.target = truckSlot();
      entry.exiting = true;
      entry.shrink = 0;
    }
  }

  function create(info, instant) {
    if (entries.size >= MAX_LIVE) return null;
    const entry = {
      id: info.order_id, product: info.product, qty: info.qty || 1, priority: Number.isFinite(Number(info.priority)) ? Number(info.priority) : PRIORITY_DEFAULT,
      layers: 0, loc: null, target: gateSlot(), pos: null, slots: [],
      snap: null, rank: -1, exiting: false, shrink: 0, elapsed: 0, vis: null,
    };
    entry.baseScale = lotScale(entry.priority);
    entry.curScale = entry.baseScale;
    structureDirty = true;
    const start = instant ? null : gateSlot();
    entry.pos = start ? { ...start } : null;
    entries.set(entry.id, entry);
    entry.instant = Boolean(instant);
    return entry;
  }

  function startElapsed(entry, lot) {
    const t = Date.parse(stepStartedTs(lot));
    entry.elapsed = Number.isFinite(t) ? Math.max(0, Math.min(60, (Date.now() - t) / 1000)) : 0;
  }

  function applyLot(entry, lot) {
    entry.layers = Math.max(entry.layers, lot.steps_done || 0);
    const zone = zoneOf(lot);
    entry.snap = { state: lot.state, station: lot.station, steps_done: lot.steps_done, zone };
    entry.vis = lotVisual(lot);
    entry.skinned = -1;
    const rank = rankOf(lot);
    // stocker/scrap zones and rework visits legitimately move a lot backwards along the line
    const bypass = zone === "stocker" || zone === "scrap" || retryWaiting(lot);
    if (!bypass && rank < entry.rank) return; // the event stream is already ahead of this snapshot
    entry.rank = rank;
    const idx = STATION_NAMES.indexOf(lot.station);
    if (zone === "stocker") place(entry, "stocker", lot.state === "banked" ? "bank" : lot.stocker.kind);
    else if (zone === "scrap") place(entry, "scrap");
    else if (lot.state === "pre_release" || idx < 0) place(entry, "gate");
    else if (lot.state === "running") {
      const was = entry.loc && entry.loc.kind === "run" && entry.loc.i === idx;
      place(entry, "run", idx);
      if (!was) startElapsed(entry, lot);
    } else place(entry, "queue", idx);
  }

  function reconcile(state) {
    const lots = (state && state.line && state.line.lots) || [];
    // lots present when the first live snapshot arrives appear in place instead of sliding in
    const live = Boolean(state && state.connection === "live");
    const instant = live && !reconciled;
    if (live) reconciled = true;
    // Priority can change without any state/station change (inspector edit): rescale in place.
    for (const lot of lots) {
      const entry = entries.get(lot.order_id);
      const pr = Number(lot.priority);
      if (!entry || entry.exiting || !Number.isFinite(pr) || pr === entry.priority) continue;
      entry.priority = pr;
      entry.baseScale = lotScale(pr);
      entry.curScale = entry.baseScale;
      structureDirty = true;
    }
    const d = diffLots(entries, lots);
    for (const lot of d.spawn) {
      const entry = create(lot, instant);
      if (entry) applyLot(entry, lot);
    }
    for (const lot of d.update) {
      const entry = entries.get(lot.order_id);
      if (entry) applyLot(entry, lot);
    }
    for (const lot of d.exit) {
      const entry = entries.get(lot.order_id);
      if (!entry) continue;
      entry.layers = Math.max(entry.layers, lot.steps_done || 0);
      place(entry, "exit");
    }
    for (const id of d.remove) drop(id);
  }

  function drop(id) {
    const entry = entries.get(id);
    if (!entry) return;
    leaveQueue(entry);
    removeBadge(id);
    entries.delete(id);
    structureDirty = true;
  }

  function ensure(fact) {
    return entries.get(fact.order_id) || create(fact, false);
  }

  function applyFact(fact) {
    if (!fact) return;
    const idx = STATION_NAMES.indexOf(fact.station);
    switch (fact.kind) {
      case "order_lifecycle": {
        if (!fact.order_id) return;
        if (fact.state === "created") {
          const e = ensure(fact);
          if (e) place(e, "gate");
        } else if (fact.state === "released") {
          const e = ensure(fact);
          if (e && e.rank < 0) { e.rank = 0; place(e, "queue", 0); }
        } else if (fact.state === "completed") {
          const e = entries.get(fact.order_id);
          if (e) place(e, "exit");
        } else if (fact.state === "failed") {
          const e = entries.get(fact.order_id);
          if (e && !e.exiting) place(e, "scrap");
        }
        break;
      }
      case "op_dispatched": {
        const e = idx >= 0 ? ensure(fact) : null;
        if (e && !e.exiting && (fact.visit || 1) > 1) { e.rank = idx * 2; place(e, "queue", idx); } // rework revisit
        else if (e && !e.exiting && e.rank < idx * 2) { e.rank = idx * 2; place(e, "queue", idx); }
        break;
      }
      case "op_started": {
        const e = idx >= 0 ? ensure(fact) : null;
        if (e && !e.exiting && e.rank <= idx * 2 + 1) {
          e.rank = idx * 2 + 1;
          e.elapsed = 0;
          place(e, "run", idx);
        }
        break;
      }
      case "op_completed": {
        const e = idx >= 0 ? entries.get(fact.order_id) : null;
        if (!e || e.exiting) break;
        if (e.loc && e.loc.kind === "run" && e.loc.i === idx && e.elapsed > 0.2) {
          expected[idx] = expected[idx] * 0.6 + e.elapsed * 0.4;
        }
        e.layers = Math.max(e.layers, idx + 1);
        e.rank = Math.max(e.rank, idx * 2 + 2);
        e.elapsed = 0;
        if (idx >= LAST) place(e, "exit");
        else place(e, "queue", idx + 1);
        break;
      }
      case "station_status": {
        if (idx >= 0 && stations[idx] && fact.status) stations[idx].setStatus(fact.status);
        break;
      }
      case "lot_stocked": {
        const e = fact.order_id ? ensure(fact) : null;
        if (e && !e.exiting) place(e, "stocker", fact.stock_kind);
        break;
      }
      case "lot_banked": {
        const e = fact.order_id ? ensure(fact) : null;
        if (e && !e.exiting) place(e, "stocker", "bank");
        break;
      }
      case "lot_unstocked":
      case "lot_released": {
        const e = idx >= 0 ? entries.get(fact.order_id) : null;
        if (e && !e.exiting) {
          e.rank = Math.min(e.rank, idx * 2);
          place(e, "queue", idx);
        }
        break;
      }
      default:
        break;
    }
  }

  // ----- selection bracket + floating label -----
  const box3 = new THREE.Box3();
  const bracket = new THREE.Box3Helper(box3, COLORS.bandActive);
  bracket.visible = false;
  scene.add(bracket);
  reg.add(bracket.geometry);
  reg.add(bracket.material);
  let selLabel = null;
  let sel = null;
  let selSeen = false;

  function setSelection(next) {
    const same = sel && next && sel.kind === next.kind && sel.id === next.id;
    if (same) return;
    sel = next ? { kind: next.kind, id: next.id } : null;
    selSeen = false;
    if (selLabel) { scene.remove(selLabel.sprite); selLabel = null; }
    // Stations already show a permanent name label; only lots need a floating one here.
    if (sel && sel.kind === "lot") {
      selLabel = makeLabelSprite(THREE, reg, sel.id, { accent: true, width: 3.6 });
      scene.add(selLabel.sprite);
    }
    refreshBracket();
  }

  function refreshBracket() {
    let ok = false;
    let top = 0;
    let cx = 0;
    let cz = 0;
    if (sel && sel.kind === "station") {
      const i = STATION_NAMES.indexOf(sel.id);
      if (i >= 0) {
        const gx = stations[i].group.position.x;
        box3.min.set(gx - 1.9, 0, -2.8);
        box3.max.set(gx + 1.9, 3.3, 1.0);
        ok = true; top = 3.3; cx = gx; cz = -0.9;
      }
    } else if (sel && sel.kind === "lot") {
      const e = entries.get(sel.id);
      if (e && e.pos) {
        const b = lotPickBox(e.pos, e.curScale, e.qty);
        box3.min.set(b.minX, b.minY, b.minZ);
        box3.max.set(b.maxX, b.maxY, b.maxZ);
        ok = true; top = b.maxY; cx = e.pos.x; cz = e.pos.z;
        selSeen = true;
      } else if (selSeen) {
        // the selected lot left the scene: clear the selection through the store
        store.select(null);
        return;
      }
    }
    bracket.visible = ok;
    if (selLabel) {
      selLabel.sprite.visible = ok;
      if (ok) selLabel.sprite.position.set(cx, top + 1.1, cz);
    }
  }

  // ----- per-frame update -----
  // `timeScale` speeds up the purely visual motion (travel, easing, exit shrink). The progress
  // clock `e.elapsed` stays on wall time: `expected` is learned from wall time, so it is already
  // self-consistent and multiplying it again would overshoot the 0.95 cap.
  function update(dt, paused, timeScale = 1) {
    if (!paused) {
      const vdt = animationDt(dt, timeScale);
      const k = 1 - Math.exp(-EASE_RATE * vdt);
      const kx = 1 - Math.exp(-EXIT_RATE * vdt);
      const running = new Set();
      for (const [id, e] of entries) {
        let moved = false;
        if (!e.pos) { e.pos = { x: e.target.x, y: e.target.y, z: e.target.z }; moved = true; structureDirty = true; }
        else if (e.instant && !e.settled) { e.pos = { x: e.target.x, y: e.target.y, z: e.target.z }; e.settled = true; moved = true; }
        if (e.travel) {
          const tr = e.travel;
          tr.t = Math.min(1, tr.t + vdt / tr.dur);
          const p = pointAt(tr.points, easeInOut(tr.t) * tr.len);
          e.pos.x = p.x; e.pos.y = p.y; e.pos.z = p.z;
          if (tr.t >= 1) { e.travel = null; e.returning = false; }
          if (e.skinned !== e.layers) skin(e);
          moveEntry(e);
          continue;
        }
        const r = e.exiting ? kx : k;
        const dx = e.target.x - e.pos.x;
        const dy = e.target.y - e.pos.y;
        const dz = e.target.z - e.pos.z;
        if (dx !== 0 || dy !== 0 || dz !== 0) {
          if (Math.abs(dx) < SETTLE_EPS && Math.abs(dy) < SETTLE_EPS && Math.abs(dz) < SETTLE_EPS) {
            e.pos.x = e.target.x; e.pos.y = e.target.y; e.pos.z = e.target.z; // settled: stop writing matrices
          } else {
            e.pos.x += dx * r; e.pos.y += dy * r; e.pos.z += dz * r;
          }
          moved = true;
        }
        if (e.skinned !== e.layers) skin(e);
        if (e.exiting) {
          const dist = Math.abs(e.target.x - e.pos.x);
          if (dist < 0.4 || e.shrink > 0) {
            e.shrink += vdt;
            e.curScale = Math.max(0.001, e.baseScale * (1 - e.shrink / SHRINK_SECONDS));
            structureDirty = true;
            if (e.shrink >= SHRINK_SECONDS) drop(id);
          }
        } else if (e.loc && e.loc.kind === "run") {
          // progress freezes while the station is down
          if (stations[e.loc.i].status !== "down") e.elapsed += dt;
          running.add(e.loc.i);
          stations[e.loc.i].setProgress(Math.min(0.95, e.elapsed / Math.max(0.5, expected[e.loc.i])));
        }
        if (moved) moveEntry(e);
      }
      stations.forEach((s, i) => { if (!running.has(i)) s.setProgress(0); });
    } else {
      for (const e of entries.values()) {
        if (!e.pos) {
          e.pos = { x: e.target.x, y: e.target.y, z: e.target.z };
          structureDirty = true;
        }
        if (e.skinned !== e.layers) skin(e);
      }
    }
    if (structureDirty) repack();
    flush();
    for (const [id, rec] of badges) {
      const e = entries.get(id);
      if (e && e.pos) rec.lb.sprite.position.set(e.pos.x, e.pos.y + 1.0 * e.curScale, e.pos.z);
    }
    refreshBracket();
  }

  // ----- picking -----
  const ray = new THREE.Raycaster();
  const ndc = new THREE.Vector2();
  const pickBox = new THREE.Box3();
  const pickPt = new THREE.Vector3();
  function pick(clientX, clientY, canvas, camera) {
    const r = canvas.getBoundingClientRect();
    ndc.set(((clientX - r.left) / r.width) * 2 - 1, -((clientY - r.top) / r.height) * 2 + 1);
    ray.setFromCamera(ndc, camera);
    let stationHit = null;
    let stationD = Infinity;
    const hits = ray.intersectObjects(stations.map((st) => st.group), true);
    for (const h of hits) {
      let o = h.object;
      while (o && !o.userData.kind) o = o.parent;
      if (o && o.userData.id) { stationHit = { kind: o.userData.kind, id: o.userData.id }; stationD = h.distance; break; }
    }
    // Lots are instanced, so raycasting the pools would test every instance triangle on each pointermove.
    // Instead test an analytic box per live entry; the entry itself is the order_id mapping (pools are
    // repacked from the same entries), so no instanceId lookup is needed.
    let lotHit = null;
    let lotD = Infinity;
    for (const e of entries.values()) {
      if (!e.pos || e.exiting) continue;
      const b = lotPickBox(e.pos, e.curScale, e.qty);
      pickBox.min.set(b.minX, b.minY, b.minZ);
      pickBox.max.set(b.maxX, b.maxY, b.maxZ);
      if (ray.ray.intersectBox(pickBox, pickPt)) {
        const d = pickPt.distanceTo(ray.ray.origin);
        if (d < lotD) { lotD = d; lotHit = e; }
      }
    }
    if (lotHit && lotD < stationD) return { kind: "lot", id: lotHit.id };
    return stationHit;
  }

  function stats() {
    let instances = 0;
    let outlines = 0;
    for (const [key, p] of pools) {
      instances += p.count;
      if (key.startsWith("o:")) outlines += p.count;
    }
    return { pools: poolList.length, instances, outlines, dirtyUploads: uploads };
  }

  function positionOf(id) {
    const e = entries.get(id);
    return e && e.pos ? { x: e.pos.x, z: e.pos.z } : null;
  }

  function dispose() {
    for (const id of [...entries.keys()]) drop(id);
    for (const p of poolList) p.dispose();
    poolList.length = 0;
    pools.clear();
    for (const lb of freeBadges) group.remove(lb.sprite);
    scene.remove(group);
    scene.remove(bracket);
    if (selLabel) scene.remove(selLabel.sprite);
  }

  return { reconcile, applyFact, update, pick, setSelection, positionOf, dispose, entries, stats };
}
