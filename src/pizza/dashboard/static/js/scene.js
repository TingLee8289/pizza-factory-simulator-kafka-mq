// Three.js "Light Digital Twin" scene. Fails soft: without WebGL it shows a small notice and
// returns so the HUD keeps working (main.js also isolates import errors).
import * as THREE from "three";
import { createCameraRig, cameraPosition, orthoExtents, viewOffsetPx, FOCUS_ZOOM } from "./camera.js";
import {
  STATION_NAMES, COLORS, stationX, createRegistry, buildGround, buildBelt, buildStations,
  buildGate, buildTruck, buildDecor, buildStocker, buildScrapZone, buildLanes,
  makeLabelSprite, TRUCK_X,
} from "./scene-objects.js";
import { shippedLabel, TRUCK_LABEL_OPTS } from "./truck-label.js";
import { createLots, scrapLabel, stockerLabel, stockerTitle } from "./lots3d.js";
import { animationDt } from "./speed.js";

// Sketch intensities were authored for legacy lights; modern three needs a factor of pi.
const LIGHT_SCALE = Math.PI;
const ROTATE_STEP = Math.PI / 4;
const WHEEL_RATE = 0.0012;
const BUTTON_ZOOM = 1.25;
const PANEL_SHIFT_PX = 120;
const OFFSET_EASE_MS = 300;

function showNotice(host, text) {
  const note = document.createElement("div");
  note.className = "scene-notice";
  note.textContent = text;
  note.style.cssText = "position:absolute;left:50%;top:50%;transform:translate(-50%,-50%);color:#6a7794;font:14px system-ui,sans-serif;";
  host.appendChild(note);
}

export function init(ctx) {
  const { store } = ctx;
  const host = ctx.el("scene");
  if (!host) return;

  let renderer;
  try {
    renderer = new THREE.WebGLRenderer({ antialias: true });
  } catch (err) {
    showNotice(host, "3D view unavailable (WebGL is not supported). The dashboard panels still work.");
    console.warn("WebGL unavailable:", err);
    return;
  }

  const reg = createRegistry();
  const motionQuery = window.matchMedia ? window.matchMedia("(prefers-reduced-motion: reduce)") : null;
  let reduced = Boolean(motionQuery && motionQuery.matches);
  const onMotionChange = (e) => { reduced = e.matches; rig.reducedMotion = reduced; };

  const canvas = renderer.domElement;
  canvas.style.cssText = "position:absolute;inset:0;width:100%;height:100%;display:block;touch-action:none;cursor:grab;";
  host.appendChild(canvas);
  renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
  renderer.shadowMap.enabled = true;
  renderer.shadowMap.type = THREE.PCFShadowMap; // PCFSoftShadowMap is deprecated in the vendored three (r186)
  renderer.setClearColor(COLORS.clear);

  const scene = new THREE.Scene();
  scene.background = new THREE.Color(COLORS.clear);
  scene.fog = new THREE.Fog(COLORS.clear, 90, 200);
  const hemi = new THREE.HemisphereLight(0xffffff, 0xb6c6e4, 0.95 * LIGHT_SCALE);
  const sun = new THREE.DirectionalLight(0xffffff, 0.85 * LIGHT_SCALE);
  sun.position.set(-16, 28, 20);
  sun.castShadow = true;
  sun.shadow.mapSize.set(2048, 2048);
  Object.assign(sun.shadow.camera, { left: -38, right: 38, top: 22, bottom: -22, near: 1, far: 90 });
  sun.shadow.bias = -0.0006;
  scene.add(hemi, sun);

  scene.add(buildGround(THREE, reg));
  const belt = buildBelt(THREE, reg);
  scene.add(belt.group);
  scene.add(buildLanes(THREE, reg));
  const built = buildStations(THREE, reg, STATION_NAMES);
  scene.add(built.group);
  scene.add(buildGate(THREE, reg));
  scene.add(buildTruck(THREE, reg));
  const truckLabel = makeLabelSprite(THREE, reg, shippedLabel({}), { ...TRUCK_LABEL_OPTS });
  truckLabel.sprite.position.set(TRUCK_X + 1.2, 4.2, 0);
  scene.add(truckLabel.sprite);
  let lastShipped = "";
  scene.add(buildDecor(THREE, reg));
  const stations = built.handles;
  const stocker = buildStocker(THREE, reg);
  scene.add(stocker.group);
  const scrap = buildScrapZone(THREE, reg);
  scene.add(scrap.group);
  const prevStatus = {};

  const lots = createLots(THREE, scene, stations, store, reg);

  // ----- camera -----
  const rig = createCameraRig({
    target: { x: 3, z: -2.5 }, azimuth: 0.3, elevation: 0.62, distance: 80, viewHeight: 36, reducedMotion: reduced,
  });
  const camera = new THREE.OrthographicCamera(-1, 1, 1, -1, -200, 400);
  let size = { w: 1, h: 1 };
  let followId = null;
  let curOffset = 0;
  let offsetFrom = 0;
  let offsetStart = 0;

  function resize() {
    const w = Math.max(1, host.clientWidth);
    const h = Math.max(1, host.clientHeight);
    size = { w, h };
    renderer.setSize(w, h, false);
  }

  function applyCamera() {
    const p = cameraPosition(rig);
    camera.position.set(p.x, p.y, p.z);
    camera.lookAt(rig.target.x, 0, rig.target.z);
    const e = orthoExtents(rig, size.w / size.h);
    camera.left = e.left; camera.right = e.right; camera.top = e.top; camera.bottom = e.bottom;
    camera.updateProjectionMatrix();
    if (Math.abs(curOffset) > 0.5) camera.setViewOffset(size.w, size.h, -curOffset, 0, size.w, size.h);
    else camera.clearViewOffset();
  }

  const ro = typeof ResizeObserver === "function" ? new ResizeObserver(resize) : null;
  if (ro) ro.observe(host);
  window.addEventListener("resize", resize);
  resize();

  // ----- station state from the store (authoritative) -----
  let lastLots = null;
  let lastStations = null;
  let lastSelKey = "";
  function syncFromStore(state) {
    const line = state.line || {};
    const sel = state.selection;
    stations.forEach((s) => {
      const rec = (line.stations || {})[s.name];
      const status = rec ? rec.status : "offline";
      const was = prevStatus[s.name];
      s.setStatus(status);
      s.setOffline(status === "offline");
      s.setPaused(status === "down");
      s.setDownLabel(reduced && status === "down");
      if ((was === "down" || was === "offline") && (status === "idle" || status === "busy") && !reduced) s.flashRepaired();
      prevStatus[s.name] = status;
      s.setQueueBadge(rec ? rec.queue || 0 : 0);
      s.setBandActive(Boolean((rec && rec.status === "busy") || (sel && sel.kind === "station" && sel.id === s.name)));
    });
    stocker.setLabel(stockerLabel(line.counts), stockerTitle(line.counts));
    scrap.setLabel(scrapLabel(line.counts));
    const shipped = shippedLabel(line.counts);
    if (shipped !== lastShipped) { lastShipped = shipped; truckLabel.setText(shipped); }
    if (line.lots !== lastLots || line.stations !== lastStations) {
      lastLots = line.lots;
      lastStations = line.stations;
      lots.reconcile(state);
    }
    const key = sel ? `${sel.kind}:${sel.id}` : "";
    if (key !== lastSelKey) {
      lastSelKey = key;
      lots.setSelection(sel);
    }
  }
  const unsub = store.subscribe(syncFromStore);
  const unsubFact = store.onFact((fact) => lots.applyFact(fact));
  syncFromStore(store.getState());

  // ----- render loop -----
  let raf = 0;
  let last = performance.now();
  let alive = true;
  function frame(now) {
    if (!alive) return;
    raf = requestAnimationFrame(frame);
    if (document.hidden) { last = now; return; }
    const dt = Math.min(0.1, Math.max(0, (now - last) / 1000));
    last = now;
    const state = store.getState();
    const paused = state.run.state === "paused" || Boolean(state.line && state.line.paused);

    // panel shift ease (jump under reduced motion)
    const goal = rig.offsetPx;
    if (reduced || OFFSET_EASE_MS <= 0) curOffset = goal;
    else if (curOffset !== goal) {
      if (offsetStart === 0) { offsetStart = now; offsetFrom = curOffset; }
      const t = Math.min(1, (now - offsetStart) / OFFSET_EASE_MS);
      curOffset = offsetFrom + (goal - offsetFrom) * (1 - Math.pow(1 - t, 3));
      if (t >= 1) { curOffset = goal; offsetStart = 0; }
    } else offsetStart = 0;

    if (followId) {
      const p = lots.positionOf(followId);
      if (!p) followId = null;
      else if (rig.goal) { rig.goal.x = Math.max(-30, Math.min(30, p.x)); rig.goal.z = Math.max(-14, Math.min(14, p.z)); rig.update(dt); }
      else rig.track(p);
    } else rig.update(dt);

    const animate = !paused && !reduced;
    const timeScale = state.timeScale || 1;
    const vdt = animationDt(dt, timeScale); // visual time: faster sim, faster belt and spinners
    if (animate) {
      belt.texture.offset.x -= vdt * 0.1;
      const blinkOn = Math.floor(now / 500) % 2 === 0;
      for (const s of stations) {
        if (s.status === "busy") for (const sp of s.spinners) sp.o.rotation.y += sp.speed * vdt;
        s.setBlink(blinkOn);
      }
    } else {
      for (const s of stations) s.setBlink(true);
    }
    lots.update(dt, paused, timeScale);
    applyCamera();
    renderer.render(scene, camera);
  }
  raf = requestAnimationFrame(frame);

  // ----- input -----
  let drag = null;
  const onContext = (e) => e.preventDefault();
  const onDown = (e) => {
    canvas.setPointerCapture(e.pointerId);
    drag = { x: e.clientX, y: e.clientY, moved: 0, btn: e.button };
    canvas.style.cursor = "grabbing";
  };
  const onMove = (e) => {
    if (!drag) {
      canvas.style.cursor = lots.pick(e.clientX, e.clientY, canvas, camera) ? "pointer" : "grab";
      return;
    }
    const dx = e.clientX - drag.x;
    const dy = e.clientY - drag.y;
    drag.x = e.clientX; drag.y = e.clientY;
    drag.moved += Math.abs(dx) + Math.abs(dy);
    if (drag.btn === 2) {
      rig.rotate(-dx * 0.006);
    } else {
      followId = null;
      const k = rig.viewHeight / size.h / rig.zoom;
      const a = rig.azimuth;
      const s = k / Math.sin(rig.elevation);
      rig.pan(-Math.cos(a) * dx * k - Math.sin(a) * dy * s, Math.sin(a) * dx * k - Math.cos(a) * dy * s);
    }
  };
  const onUp = (e) => {
    const d = drag;
    drag = null;
    canvas.style.cursor = "grab";
    if (d && d.moved < 5 && d.btn === 0) {
      const hit = lots.pick(e.clientX, e.clientY, canvas, camera);
      store.select(hit);
    }
  };
  const onWheel = (e) => {
    e.preventDefault();
    rig.zoomBy(Math.exp(-e.deltaY * WHEEL_RATE));
  };
  const onKey = (e) => {
    if (e.key !== "Escape") return;
    const pop = ctx.el("order-popover");
    if (pop && !pop.classList.contains("hidden")) return; // the popover closes first
    if (store.getState().selection) store.select(null);
  };
  const onPanel = (e) => {
    let open = e && e.detail && typeof e.detail.expanded === "boolean" ? e.detail.expanded : null;
    if (open === null) {
      const btn = ctx.el("panel-toggle");
      open = Boolean(btn && btn.getAttribute("aria-expanded") === "true");
    }
    api.setViewOffsetPx(open ? PANEL_SHIFT_PX : 0);
  };
  const onVisible = () => { last = performance.now(); };
  canvas.addEventListener("contextmenu", onContext);
  canvas.addEventListener("pointerdown", onDown);
  canvas.addEventListener("pointermove", onMove);
  canvas.addEventListener("pointerup", onUp);
  canvas.addEventListener("wheel", onWheel, { passive: false });
  document.addEventListener("keydown", onKey);
  document.addEventListener("panel-toggle", onPanel);
  document.addEventListener("visibilitychange", onVisible);
  if (motionQuery && motionQuery.addEventListener) motionQuery.addEventListener("change", onMotionChange);

  // ----- public API -----
  const api = {
    focusStation(name) {
      const i = STATION_NAMES.indexOf(name);
      if (i < 0) return;
      followId = null;
      rig.focusOn({ x: stationX(i), z: 0 }, FOCUS_ZOOM);
    },
    followLot(id) {
      const p = lots.positionOf(id);
      if (!p) return;
      followId = id;
      rig.focusOn(p, 2);
    },
    home() { followId = null; rig.home(); },
    zoom(factor) { rig.zoomBy(factor); },
    rotate(delta) { rig.rotate(delta); },
    setViewOffsetPx(px) { viewOffsetPx(rig, px); },
    select(sel) { store.select(sel || null); },
    dispose() {
      alive = false;
      cancelAnimationFrame(raf);
      unsub();
      unsubFact();
      if (ro) ro.disconnect();
      window.removeEventListener("resize", resize);
      canvas.removeEventListener("contextmenu", onContext);
      canvas.removeEventListener("pointerdown", onDown);
      canvas.removeEventListener("pointermove", onMove);
      canvas.removeEventListener("pointerup", onUp);
      canvas.removeEventListener("wheel", onWheel);
      document.removeEventListener("keydown", onKey);
      document.removeEventListener("panel-toggle", onPanel);
      document.removeEventListener("visibilitychange", onVisible);
      if (motionQuery && motionQuery.removeEventListener) motionQuery.removeEventListener("change", onMotionChange);
      for (const b of toolBindings) b.el.removeEventListener("click", b.fn);
      lots.dispose();
      reg.disposeAll();
      renderer.dispose();
      canvas.remove();
    },
  };
  ctx.scene3d = api;

  // tool strip
  const toolBindings = [];
  const bind = (id, fn) => {
    const el = ctx.el(id);
    if (!el) return;
    el.addEventListener("click", fn);
    toolBindings.push({ el, fn });
  };
  bind("tool-zoom-in", () => api.zoom(BUTTON_ZOOM));
  bind("tool-zoom-out", () => api.zoom(1 / BUTTON_ZOOM));
  bind("tool-rotate-left", () => api.rotate(ROTATE_STEP));
  bind("tool-rotate-right", () => api.rotate(-ROTATE_STEP));
  bind("tool-home", () => api.home());
}
