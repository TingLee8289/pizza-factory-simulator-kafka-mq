// Right column fit: while a lot inspector is open, the Stations/Lots panel gives
// up height (down to MIN_PANEL_PX) so the inspector shows all of its content,
// Bank/Scrap menus included. Closed inspector -> the panel is back at 40vh.
// Works by overriding --tabs-panel-h, which sizes #tabs-wrap and caps #inspector.
const TOP_PX = 76; // #inspector top
const GAP_PX = 24; // panel bottom margin + gap above it (theme.css #inspector max-height)
export const MIN_PANEL_PX = 160;
const DEFAULT_PANEL_VH = 40;

export function panelHeight(viewportH, inspectorNeedPx) {
  const def = (viewportH * DEFAULT_PANEL_VH) / 100;
  if (!inspectorNeedPx) return def;
  const room = viewportH - TOP_PX - GAP_PX - inspectorNeedPx;
  return Math.max(MIN_PANEL_PX, Math.min(def, room));
}

// Natural inspector height: the details (left) and the action dock (right) columns
// scroll when capped, so add back the most either of them hides.
function inspectorNeed(insp, body) {
  if (!insp || insp.classList.contains("hidden")) return 0;
  const hiddenIn = (e) => (e ? e.scrollHeight - e.clientHeight : 0);
  const hidden = Math.max(hiddenIn(body), hiddenIn(insp.querySelector(".insp-dock-host")));
  return Math.ceil(insp.getBoundingClientRect().height + Math.max(0, hidden));
}

export function init(ctx) {
  const doc = ctx.document;
  const win = doc.defaultView;
  const insp = ctx.el("inspector");
  if (!insp || !win) return;
  const root = doc.documentElement;
  let queued = false;
  let last = null;

  const apply = () => {
    queued = false;
    const h = Math.round(panelHeight(win.innerHeight, inspectorNeed(insp, ctx.el("inspector-body"))));
    if (h === last) return;
    last = h;
    root.style.setProperty("--tabs-panel-h", `${h}px`);
  };
  const schedule = () => {
    if (queued) return;
    queued = true;
    win.requestAnimationFrame(apply);
  };

  new win.MutationObserver(schedule).observe(insp, {
    attributes: true, attributeFilter: ["class"], childList: true, subtree: true, characterData: true,
  });
  if (win.ResizeObserver) new win.ResizeObserver(schedule).observe(insp);
  win.addEventListener("resize", schedule);
  schedule();
}
