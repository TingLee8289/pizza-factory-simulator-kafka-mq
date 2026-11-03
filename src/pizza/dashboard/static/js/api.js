// Same-origin JSON client. Error text is reduced to a short plain string (never HTML).
const MAX_DETAIL = 200;

export class ApiError extends Error {
  constructor(status, detail) {
    super(detail || `HTTP ${status}`);
    this.name = "ApiError";
    this.status = status;
    this.detail = detail || "";
  }
}

function sanitizeDetail(detail) {
  let text = "";
  if (typeof detail === "string") text = detail;
  else if (Array.isArray(detail)) {
    text = detail.map((d) => (d && typeof d.msg === "string" ? d.msg : "")).filter(Boolean).join("; ");
  }
  return text.replace(/<[^>]*>/g, "").slice(0, MAX_DETAIL);
}

export async function postJson(path, body) {
  let resp;
  try {
    resp = await fetch(path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body === undefined ? {} : body),
      credentials: "same-origin",
    });
  } catch (err) {
    throw new ApiError(0, "network");
  }
  let data = null;
  try {
    data = await resp.json();
  } catch (err) {
    data = null;
  }
  if (!resp.ok) throw new ApiError(resp.status, sanitizeDetail(data && data.detail));
  return data;
}

export async function getJson(path, { signal } = {}) {
  let resp;
  try {
    resp = await fetch(path, { method: "GET", credentials: "same-origin", signal });
  } catch (err) {
    if (err && err.name === "AbortError") throw new ApiError(0, "aborted");
    throw new ApiError(0, "network");
  }
  let data = null;
  try {
    data = await resp.json();
  } catch (err) {
    data = null;
  }
  if (!resp.ok) throw new ApiError(resp.status, sanitizeDetail(data && data.detail));
  return data;
}

export async function breakStation(name, durationS) {
  const body = Number.isFinite(durationS) && durationS > 0 ? { duration_s: durationS } : {};
  return postJson(`/api/station/${encodeURIComponent(name)}/break`, body);
}

export async function repairStation(name) {
  return postJson(`/api/station/${encodeURIComponent(name)}/repair`, {});
}

export async function bankLot(orderId, reason) {
  return postJson(`/api/lot/${encodeURIComponent(orderId)}/bank`, { reason });
}

export async function scrapLot(orderId, reason) {
  return postJson(`/api/lot/${encodeURIComponent(orderId)}/scrap`, { reason });
}

export async function releaseLot(orderId) {
  return postJson(`/api/lot/${encodeURIComponent(orderId)}/release`, {});
}

export async function cancelLotHold(orderId) {
  return postJson(`/api/lot/${encodeURIComponent(orderId)}/hold/cancel`, {});
}

export async function setLotPriority(orderId, priority) {
  return postJson(`/api/lot/${encodeURIComponent(orderId)}/priority`, { priority });
}

// Live per-station ~IN/~OUT for one lot: { available, stations: { station: { in, out } } }.
export async function getLotEta(orderId, { signal } = {}) {
  return getJson(`/api/lot/${encodeURIComponent(orderId)}/eta`, { signal });
}

// Heavy per-lot history (visits, steps, station_eta, failures, priority_changes) for one lot.
export async function getLotDetail(orderId, { signal } = {}) {
  return getJson(`/api/lot/${encodeURIComponent(orderId)}`, { signal });
}

export async function getFailureConfig() {
  return getJson("/api/failure-config");
}

export async function getRecipes() {
  return getJson("/api/recipes");
}

export async function setRecipe(station, product, unitTimeS) {
  return postJson(`/api/station/${encodeURIComponent(station)}/recipe`, {
    product,
    unit_time_s: unitTimeS,
  });
}

// --- Phase 7: dispatch-rule comparison (COMP-01..04) ---

export async function startComparison(body) {
  return postJson("/api/comparison/start", body);
}

export async function nextRound() {
  return postJson("/api/comparison/next");
}

export async function endRound() {
  return postJson("/api/comparison/end");
}

export async function cancelComparison() {
  return postJson("/api/comparison/cancel");
}

export async function getComparison() {
  return getJson("/api/comparison");
}

export async function getRuns() {
  return getJson("/api/runs");
}

export async function recomputeRun(runId) {
  return postJson(`/api/runs/${encodeURIComponent(runId)}/recompute`);
}

export function csvUrl(comparisonId) {
  return comparisonId
    ? `/api/comparison.csv?comparison_id=${encodeURIComponent(comparisonId)}`
    : "/api/comparison.csv";
}

export function errorReason(err) {
  const status = err && typeof err.status === "number" ? err.status : 0;
  const detail = err && err.detail ? err.detail : "";
  if (status === 0) return "Cannot reach the dashboard service";
  if (status === 404) return "Unknown station or lot";
  if (status === 409) return detail || "No active run";
  if (status === 422) return detail || "A field value is out of range";
  if (status === 415) return "Request must be JSON";
  if (status === 503) return "Broker unavailable";
  return detail || `Request failed (${status})`;
}
