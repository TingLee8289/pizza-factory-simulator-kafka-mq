// Simulation speed buttons (1x..8x). The scale lives in the backend (Kafka sim.timescale);
// the highlight always follows store.state.timeScale, which comes from /api/state and ticks.
import { postJson, errorReason } from "./api.js";
import { showToast } from "./toast.js";

export const SPEEDS = [1, 2, 4, 8];
export const SPEED_TITLE = "模擬速度(KPI 以模擬秒計算)";
// Largest simulated time a single animation frame may advance, so 8x never teleports parts.
export const MAX_ANIM_DT = 0.4;
export const SPEED_ERROR_TEXT = "Could not change the simulation speed. Check that Kafka is running and try again.";

function normalizeActive(active) {
  const n = Number(active);
  return SPEEDS.includes(n) ? n : 1;
}

// Pure view model: one entry per button. Buttons are never disabled (not while a run is
// executing, not during a comparison): the scale is independent of run state.
export function speedViewModel({ active } = {}) {
  const current = normalizeActive(active);
  return SPEEDS.map((scale) => ({
    scale,
    label: `${scale}×`,
    active: scale === current,
    disabled: false,
    title: SPEED_TITLE,
  }));
}

// Visual time for one frame: the scene's already-clamped wall dt times the scale, capped.
export function animationDt(dt, scale) {
  const s = Number.isFinite(Number(scale)) && Number(scale) > 0 ? Number(scale) : 1;
  const d = Number.isFinite(dt) && dt > 0 ? dt : 0;
  return Math.min(d * s, MAX_ANIM_DT);
}

function render(buttons, active) {
  const vm = new Map(speedViewModel({ active }).map((v) => [v.scale, v]));
  for (const btn of buttons) {
    const v = vm.get(Number(btn.dataset.scale));
    if (!v) continue;
    btn.classList.toggle("active", v.active);
    btn.setAttribute("aria-pressed", v.active ? "true" : "false");
    btn.disabled = false;
    btn.title = v.title;
  }
}

export function init(ctx) {
  const { store } = ctx;
  const host = ctx.el("speed-switch");
  if (!host) return;
  const buttons = [...host.querySelectorAll("button[data-scale]")];
  const redraw = () => render(buttons, store.getState().timeScale);
  store.subscribe(redraw);
  redraw();
  for (const btn of buttons) {
    btn.addEventListener("click", async () => {
      const scale = Number(btn.dataset.scale);
      try {
        const out = await postJson("/api/time_scale", { scale });
        store.setTimeScale(out && out.scale !== undefined ? out.scale : scale);
      } catch (err) {
        showToast({ text: `${SPEED_ERROR_TEXT} (${errorReason(err)})`, kind: "error" });
        redraw(); // highlight snaps back to the backend-reported scale
      }
    });
  }
}
