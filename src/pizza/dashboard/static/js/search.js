// Search box: "/" focuses, Enter selects the first match, Esc clears. Plain string matching only
// (no RegExp from user input).
import { ROUTE } from "./inspector.js";

export function matchSearch(query, state) {
  const q = String(query == null ? "" : query).trim().toLowerCase();
  if (!q) return null;
  if (ROUTE.includes(q)) return { kind: "station", id: q };
  const lots = (state && state.line && state.line.lots) || [];
  for (const l of lots) {
    if (String(l.order_id).toLowerCase().includes(q)) return { kind: "lot", id: l.order_id };
  }
  for (const name of ROUTE) {
    if (name.startsWith(q)) return { kind: "station", id: name };
  }
  for (const name of ROUTE) {
    if (name.includes(q)) return { kind: "station", id: name };
  }
  return null;
}

export function init(ctx) {
  const { store, el, document: doc } = ctx;
  const input = el("search");
  if (!input) return;

  doc.addEventListener("keydown", (e) => {
    if (e.key !== "/" || e.ctrlKey || e.metaKey || e.altKey) return;
    const t = e.target;
    const tag = t && t.tagName ? t.tagName.toLowerCase() : "";
    if (tag === "input" || tag === "textarea" || tag === "select" || (t && t.isContentEditable)) return;
    e.preventDefault();
    input.focus();
  });

  input.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
      input.value = "";
      input.blur();
      return;
    }
    if (e.key !== "Enter") return;
    const hit = matchSearch(input.value, store.getState());
    if (!hit) return;
    store.select(hit);
    if (ctx.scene3d) {
      ctx.scene3d.select(hit);
      if (hit.kind === "station") ctx.scene3d.focusStation(hit.id);
    }
  });
}
