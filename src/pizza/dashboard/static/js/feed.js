// Event feed: last 5 station events, driven by store.onFact (not state.feed polling).
export const FEED_CAP = 5;
export const EMPTY_FEED = "Waiting for station events…";

const KIND_LABEL = {
  op_started: "started",
  op_completed: "completed",
  station_status: "status",
  order_lifecycle: "lot",
  op_failed: "failed",
  op_dispatched: "dispatched",
  lot_stocked: "stocked",
  lot_unstocked: "unstocked",
  lot_banked: "banked",
  lot_released: "released",
  rule_changed: "rule",
};

function clock(ts) {
  const m = typeof ts === "string" ? /T(\d{2}:\d{2}:\d{2})/.exec(ts) : null;
  return m ? m[1] : "--:--:--";
}

export function createFeedMemory() {
  return new Map();
}

export function feedLine(fact, memory) {
  if (!fact || !KIND_LABEL[fact.kind]) return "";
  const t = clock(fact.ts);
  const st = fact.station || "—";
  const oid = fact.order_id || "—";
  switch (fact.kind) {
    case "op_failed":
      return `${t}  op_failed ${oid} at ${st}${fact.cause === "breakdown" ? " (breakdown)" : ""}`;
    case "op_dispatched":
      if ((fact.visit || 1) > 1) return `${t}  rework -> ${st} (visit ${fact.visit})`;
      return `${t}  ${st}  dispatched  ${oid}`;
    case "order_lifecycle":
      if ((fact.state || fact.status) === "failed") return `${t}  -> DLQ ${oid}`;
      break;
    case "station_status": {
      const prev = memory ? memory.get(fact.station) : undefined;
      if (memory && fact.station) memory.set(fact.station, fact.status);
      if (fact.status === "down") return `${t}  ${st} DOWN`;
      if (fact.status === "offline") return `${t}  ${st} OFFLINE`;
      if ((fact.status === "idle" || fact.status === "busy") && (prev === "down" || prev === "offline")) {
        return `${t}  ${st} UP`;
      }
      break;
    }
    case "lot_stocked": {
      const k = fact.stock_kind;
      const why = k === "station_down" ? `${st} down` : k === "station_offline" ? `${st} offline` : "queue full";
      return `${t}  STOCKER ${oid} (${why})`;
    }
    case "lot_unstocked":
      return `${t}  ${st}  unstocked  ${oid}`;
    case "lot_banked":
      return `${t}  BANK ${oid} (${fact.reason || "capacity"}, ${String(fact.source || "").startsWith("auto") ? "auto" : "manual"})`;
    case "lot_released":
      return `${t}  RELEASE ${oid} -> ${st}`;
    case "rule_changed":
      return `${t}  RULE ${String(fact.rule || "").toUpperCase()}`;
    case "op_started":
      if (fact.redelivered === true) return `${t}  redelivered ${oid} at ${st}`;
      break;
    default:
      break;
  }
  let label = KIND_LABEL[fact.kind];
  if (fact.kind === "station_status" && fact.status) label = `status ${fact.status}`;
  return `${t}  ${st}  ${label}  ${oid}`;
}

export function pushFeed(lines, line) {
  if (!line) return lines;
  return lines.concat([line]).slice(-FEED_CAP);
}

export function init(ctx) {
  const { store, el, document: doc } = ctx;
  const host = el("feed");
  if (!host) return;
  let lines = [];
  const memory = createFeedMemory();

  function render() {
    while (host.firstChild) host.removeChild(host.firstChild);
    if (!lines.length) {
      host.textContent = EMPTY_FEED;
      return;
    }
    for (const l of lines) {
      const row = doc.createElement("div");
      row.className = "feed-line";
      row.textContent = l;
      host.appendChild(row);
    }
  }

  store.onFact((fact) => {
    const next = pushFeed(lines, feedLine(fact, memory));
    if (next === lines) return;
    lines = next;
    render();
  });
  render();
}
