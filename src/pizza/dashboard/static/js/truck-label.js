// Pure helper for the "SHIPPED N lots / M pizzas" label floating above the 3D truck.
// No THREE / DOM dependency so it stays unit-testable under node --test.

export const TRUCK_LABEL_OPTS = { width: 7.2, maxChars: 34, fontPx: 13, stroke: "#2e9e6b" };

export function shippedLabel(counts) {
  const n = (v) => (typeof v === "number" && Number.isFinite(v) && v > 0 ? Math.floor(v) : 0);
  return `SHIPPED ${n(counts && counts.completed)} lots / ${n(counts && counts.completed_pizzas)} pizzas`;
}
