// Pure camera rig math (no THREE, no DOM): node can import and test this file.
// Locked numbers come from 05-UI-SPEC "Scene (locked)".
export const PAN_LIMIT = { x: 30, z: 14 };
export const ZOOM_LIMIT = { min: 0.6, max: 3.2 };
export const FOCUS_ZOOM = 1.6;
const TWO_PI = Math.PI * 2;

export const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));

// Wrap an angle into (-pi, pi].
export function wrapAngle(a) {
  let r = a % TWO_PI;
  if (r > Math.PI) r -= TWO_PI;
  else if (r <= -Math.PI) r += TWO_PI;
  return r;
}

export function createCameraRig(opts = {}) {
  const init = {
    target: { x: 3, z: -2.5, ...(opts.target || {}) },
    azimuth: opts.azimuth ?? 0.3,
    elevation: opts.elevation ?? 0.62,
    distance: opts.distance ?? 80,
    viewHeight: opts.viewHeight ?? 36,
  };
  const rig = {
    target: { x: init.target.x, z: init.target.z },
    azimuth: wrapAngle(init.azimuth),
    elevation: init.elevation,
    distance: init.distance,
    viewHeight: init.viewHeight,
    zoom: 1,
    offsetPx: 0,
    reducedMotion: Boolean(opts.reducedMotion),
    goal: null, // {x, z, zoom} while a focus ease is running
    pan(dx, dz) {
      rig.goal = null;
      rig.target.x = clamp(rig.target.x + dx, -PAN_LIMIT.x, PAN_LIMIT.x);
      rig.target.z = clamp(rig.target.z + dz, -PAN_LIMIT.z, PAN_LIMIT.z);
      return rig;
    },
    zoomBy(factor) {
      rig.goal = null;
      rig.zoom = clamp(rig.zoom * factor, ZOOM_LIMIT.min, ZOOM_LIMIT.max);
      return rig;
    },
    rotate(delta) {
      rig.azimuth = wrapAngle(rig.azimuth + delta);
      return rig;
    },
    home() {
      rig.goal = null;
      rig.target.x = init.target.x;
      rig.target.z = init.target.z;
      rig.azimuth = wrapAngle(init.azimuth);
      rig.zoom = 1;
      return rig;
    },
    focusOn(point, zoom = FOCUS_ZOOM) {
      const goal = {
        x: clamp(point.x, -PAN_LIMIT.x, PAN_LIMIT.x),
        z: clamp(point.z ?? 0, -PAN_LIMIT.z, PAN_LIMIT.z),
        zoom: clamp(zoom, ZOOM_LIMIT.min, ZOOM_LIMIT.max),
      };
      if (rig.reducedMotion) {
        rig.target.x = goal.x;
        rig.target.z = goal.z;
        rig.zoom = goal.zoom;
        rig.goal = null;
      } else {
        rig.goal = goal;
      }
      return rig;
    },
    // Camera tracks a moving point every frame (Follow); jumps, no easing.
    track(point) {
      rig.target.x = clamp(point.x, -PAN_LIMIT.x, PAN_LIMIT.x);
      rig.target.z = clamp(point.z ?? 0, -PAN_LIMIT.z, PAN_LIMIT.z);
      return rig;
    },
    update(dt, speed = 6) {
      if (!rig.goal) return rig;
      const next = focus(rig, rig.goal, rig.goal.zoom, dt, speed);
      rig.target.x = next.target.x;
      rig.target.z = next.target.z;
      rig.zoom = next.zoom;
      if (next.done) rig.goal = null;
      return rig;
    },
  };
  return rig;
}

// Camera position for the orthographic rig: the distance runs along the view direction.
export function cameraPosition(rig) {
  const { azimuth: a, elevation: e, distance: d, target } = rig;
  return {
    x: target.x + d * Math.cos(e) * Math.sin(a),
    y: d * Math.sin(e),
    z: target.z + d * Math.cos(e) * Math.cos(a),
  };
}

export function orthoExtents(rig, aspect) {
  const half = rig.viewHeight / rig.zoom / 2;
  return { left: -half * aspect, right: half * aspect, top: half, bottom: -half };
}

// Deterministic exponential ease of target/zoom toward a point. Returns the next state
// without mutating the rig; reducedMotion jumps straight to the goal.
export function focus(rig, point, zoom = FOCUS_ZOOM, dt = 1 / 60, speed = 6) {
  const gx = clamp(point.x, -PAN_LIMIT.x, PAN_LIMIT.x);
  const gz = clamp(point.z ?? 0, -PAN_LIMIT.z, PAN_LIMIT.z);
  const gzoom = clamp(zoom, ZOOM_LIMIT.min, ZOOM_LIMIT.max);
  if (rig.reducedMotion) return { target: { x: gx, z: gz }, zoom: gzoom, done: true };
  const k = 1 - Math.exp(-speed * Math.max(0, dt));
  const x = rig.target.x + (gx - rig.target.x) * k;
  const z = rig.target.z + (gz - rig.target.z) * k;
  const zm = rig.zoom + (gzoom - rig.zoom) * k;
  const done = Math.abs(gx - x) < 0.01 && Math.abs(gz - z) < 0.01 && Math.abs(gzoom - zm) < 0.005;
  return done
    ? { target: { x: gx, z: gz }, zoom: gzoom, done: true }
    : { target: { x, z }, zoom: zm, done: false };
}

// Horizontal pixel shift of the frame (panel expanded -> 120). 0 clears it.
export function viewOffsetPx(rig, px) {
  rig.offsetPx = Number.isFinite(px) ? px : 0;
  return rig.offsetPx;
}
