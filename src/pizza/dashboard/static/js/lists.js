// Stations / Lots tab lists. Utilization comes from state.kpi.stations only (never derived here).
import { ROUTE, DISPLAY, machineLabel } from "./inspector.js";
import { isHighPriority, PRIORITY_DEFAULT } from "./priority.js";

const RENDER_MS = 200;
const QUEUE_AMBER_AT = 4;
const DASH = "—";
const REASON = { capacity: "Capacity", customer: "Customer", schedule: "Schedule" };
export const EMPTY_LOTS = "No lots in progress";

export function statusChip(status) {
  if (status === "busy") return { text: "Busy", tone: "ok" };
  if (status === "idle") return { text: "Idle", tone: "idle" };
  if (status === "down") return { text: "Down", tone: "danger" };
  return { text: "Offline", tone: "amber" };
}

export function emptyCopy(filter) {
  if (filter === "failed") {
    return { heading: "No scrap lots", body: "Lots that exhaust their rework limit are scrapped; they appear here and in the dead-letter queue." };
  }
  if (filter === "bank") {
    return { heading: "No banked lots", body: "Lots are banked when a station stays down too long, or when you bank them from the lot inspector." };
  }
  return { heading: EMPTY_LOTS, body: "" };
}

function isRetryWait(l, name) {
  return (
    // Any rework WAIT: cooling in the hold queue, back in the queue, or dispatched.
    l.state === "queued" && l.station === name && (l.visit || 1) > 1 && Boolean(l.retry_since_ts)
  );
}

export function stationRows(state) {
  const line = (state && state.line) || {};
  const stations = line.stations || {};
  const kpi = (state && state.kpi && state.kpi.stations) || {};
  const lots = line.lots || [];
  const sel = state && state.selection;
  return ROUTE.map((name) => {
    const st = stations[name] || {};
    const queue = typeof st.queue === "number" ? st.queue : 0;
    // WIP = waiting at the station plus the batch running there now (same rule as the global WIP).
    const wip = queue + (st.current_lot ? 1 : 0);
    const u = kpi[name] && typeof kpi[name].utilization === "number" ? kpi[name].utilization : null;
    const av = kpi[name] && typeof kpi[name].availability === "number" ? kpi[name].availability : null;
    const tp = kpi[name] && typeof kpi[name].pizzas_per_s === "number" ? kpi[name].pizzas_per_s : null;
    const status = st.status || "offline";
    return {
      name,
      status,
      queue,
      wip,
      utilization: u,
      availability: av,
      availText: av == null ? DASH : `${Math.round(av * 100)}%`,
      throughputText: tp == null ? DASH : `${(tp * 60).toFixed(1)} pizzas/min`,
      retryWait: lots.some((l) => isRetryWait(l, name)),
      utilDown: status === "down",
      queueTone: wip >= QUEUE_AMBER_AT ? "amber" : "normal",
      selected: Boolean(sel && sel.kind === "station" && sel.id === name),
    };
  });
}

// Status labels match the inspector's Status row (WAIT / RUN / BANK / SCRAP / DONE).
// The reason (queue full, station down, ...) lives in the inspector's Location row.
function lotChip(l) {
  if (l.state === "failed") return { text: "SCRAP", tone: "danger", dashed: false };
  if (l.state === "banked") return { text: "BANK", tone: "idle", dashed: true };
  if (l.stocker) {
    const k = l.stocker.kind;
    const amber = k === "station_down" || k === "station_offline";
    return { text: "WAIT", tone: amber ? "amber" : "idle", dashed: false };
  }
  return null;
}

// Plain lifecycle chip for lots with no special status (RUN, WAIT, DONE), so the Status column is never empty.
function plainStatus(l) {
  if (l.state === "running") return { text: "RUN", tone: "ok", dashed: false };
  if (l.state === "queued" || l.state === "pre_release") return { text: "WAIT", tone: "idle", dashed: false };
  if (l.state === "done") return { text: "DONE", tone: "idle", dashed: false };
  return null;
}

// Default lot order: oldest first, then by id. Also the tie-break under every column sort.
function byCreated(a, b) {
  const ca = String(a.created_ts || "");
  const cb = String(b.created_ts || "");
  if (ca !== cb) return ca < cb ? -1 : 1;
  return String(a.order_id) < String(b.order_id) ? -1 : String(a.order_id) > String(b.order_id) ? 1 : 0;
}

const byText = (a, b) => String(a || "").localeCompare(String(b || ""), "en", { numeric: true, sensitivity: "base" });
const byNumber = (a, b) => a - b;

function priorityOf(l) {
  return Number.isFinite(Number(l.priority)) ? Number(l.priority) : PRIORITY_DEFAULT;
}

// Route position for the Step column: pre-release sorts before Spin, no station sorts last.
function stepRank(l) {
  if (l.state === "pre_release") return -1;
  const i = ROUTE.indexOf(l.station);
  return i < 0 ? ROUTE.length : i;
}

// Status column order: RUN, WAIT in queue, WAIT in stocker / pre-release, BANK, SCRAP.
function statusRank(l) {
  if (l.state === "running") return 0;
  if (l.state === "queued" && !l.stocker) return 1;
  if (l.state === "queued" || l.state === "pre_release") return 2;
  if (l.state === "banked") return 3;
  if (l.state === "failed") return 4;
  return 5;
}

// Lots list column sorts. `desc` is the direction of the first click (Progress: most done first).
export const LOT_SORTS = {
  id: { value: (l) => l.order_id, cmp: byText, desc: false },
  product: { value: (l) => l.product, cmp: byText, desc: false },
  priority: { value: priorityOf, cmp: byNumber, desc: false },
  progress: { value: (l) => l.steps_done || 0, cmp: byNumber, desc: true },
  step: { value: stepRank, cmp: byNumber, desc: false },
  status: { value: statusRank, cmp: byNumber, desc: false },
};

// Header click cycle: first direction -> flipped -> back to the default order (null).
export function nextLotSort(current, key) {
  if (!LOT_SORTS[key]) return null;
  if (!current || current.key !== key) return { key, flipped: false };
  return current.flipped ? null : { key, flipped: true };
}

export function lotSortDesc(sort) {
  return Boolean(sort && LOT_SORTS[sort.key] && LOT_SORTS[sort.key].desc !== sort.flipped);
}

function lotComparator(sort) {
  const spec = sort && LOT_SORTS[sort.key];
  if (!spec) return byCreated;
  const sign = lotSortDesc(sort) ? -1 : 1;
  return (a, b) => sign * spec.cmp(spec.value(a), spec.value(b)) || byCreated(a, b);
}

// While the pointer is over the list the row order is held: known rows keep their previous
// position, new rows follow in sorted order, and rows that left the list drop out.
export function holdOrder(rows, prevIds) {
  if (!prevIds || !prevIds.length) return rows;
  const pos = new Map(prevIds.map((id, i) => [id, i]));
  const kept = rows.filter((r) => pos.has(r.id)).sort((a, b) => pos.get(a.id) - pos.get(b.id));
  return kept.concat(rows.filter((r) => !pos.has(r.id)));
}

export function lotRows(state, filter = "all", sort = null) {
  let lots = ((state && state.line && state.line.lots) || []).filter((l) => l.state !== "done");
  if (filter === "active") lots = lots.filter((l) => l.state === "queued" || l.state === "running");
  else if (filter === "bank") lots = lots.filter((l) => l.state === "banked");
  else if (filter === "failed") lots = lots.filter((l) => l.state === "failed");
  const sel = state && state.selection;
  const total = ROUTE.length;
  return lots
    .slice()
    .sort(lotComparator(sort))
    .map((l) => {
      const row = {
        id: l.order_id,
        product: l.product,
        qty: l.qty,
        progress: Math.min(1, Math.max(0, (l.steps_done || 0) / total)),
        station: l.state === "pre_release" ? "pre-release" : l.station || null,
        priority: priorityOf(l),
        selected: Boolean(sel && sel.kind === "lot" && sel.id === l.order_id),
      };
      const chip = lotChip(l) || plainStatus(l);
      if (chip) row.chip = chip;
      if (l.state === "failed") {
        const f = l.failed || {};
        row.failStation = f.station || l.station || null;
        row.visit = f.visit || l.visit || null;
      }
      if (l.state === "banked" && l.bank && String(l.bank.source || "").startsWith("auto")) row.sub = "auto";
      if ((l.visit || 1) > 1 && isRetryWait(l, l.station)) row.retryBadge = `R${l.visit}`;
      return row;
    });
}

function mk(doc, tag, cls, text) {
  const n = doc.createElement(tag);
  if (cls) n.className = cls;
  if (text != null) n.textContent = text;
  return n;
}

function clear(host) {
  while (host.firstChild) host.removeChild(host.firstChild);
}

function bar(doc, fraction, accent, down) {
  const track = mk(doc, "span", "bar");
  const fill = mk(doc, "span", down ? "bar-fill down" : accent ? "bar-fill accent" : "bar-fill");
  const pct = Math.round(Math.min(1, Math.max(0, fraction || 0)) * 100);
  fill.style.width = `${pct}%`;
  track.appendChild(fill);
  return track;
}

const FILTERS = [["active", "Active"], ["bank", "Bank"], ["failed", "Scrap"], ["all", "All"]];

// Column header for the Stations list; classes match the cell classes in renderStations.
const STATION_COLUMNS = [
  ["station-col-stage", "Stage"],
  ["station-col-machine", "Machine ID"],
  ["station-col-status", "Status"],
  ["station-col-wip", "WIP"],
  ["station-col-avail", "Avail"],
  ["station-col-util", "Util"],
  ["station-col-tput", "Throughput"],
];

// Column header for the Lots list; classes match the cell classes in renderLots.
// The third field is the LOT_SORTS key the header click sorts by.
const LOT_COLUMNS = [
  ["lot-col-id", "Lot ID", "id"],
  ["lot-col-product", "Product ID", "product"],
  ["lot-col-prio", "Priority", "priority"],
  ["lot-col-progress", "Progress", "progress"],
  ["lot-col-step", "Step", "step"],
  ["lot-col-status", "Status", "status"],
];

export function init(ctx) {
  const { store, el, document: doc } = ctx;
  const stationsHost = el("tab-stations");
  const lotsHost = el("tab-lots");
  if (!stationsHost && !lotsHost) return;
  let filter = "active";
  let sort = null; // { key, flipped } from a header click; null = default order
  let hovering = false; // pointer over the lot list: hold the row order
  let shownIds = []; // row order of the last render, kept while hovering
  let filterBar = null;
  let listHost = null;

  function choose(sel) {
    store.select(sel);
    if (ctx.scene3d) ctx.scene3d.select(sel);
  }

  if (lotsHost) {
    clear(lotsHost);
    filterBar = mk(doc, "div", "seg-group");
    filterBar.setAttribute("role", "group");
    filterBar.setAttribute("aria-label", "Lots filter");
    for (const [key, label] of FILTERS) {
      const b = mk(doc, "button", null, label);
      b.type = "button";
      b.dataset.filter = key;
      b.addEventListener("click", () => {
        filter = key;
        sort = null;
        shownIds = [];
        render();
      });
      filterBar.appendChild(b);
    }
    listHost = mk(doc, "div", "lot-list");
    listHost.addEventListener("pointerenter", () => { hovering = true; });
    listHost.addEventListener("pointerleave", () => {
      hovering = false;
      render();
    });
    lotsHost.appendChild(filterBar);
    lotsHost.appendChild(listHost);
  }

  const dlqChip = el("panel-dlq-chip");
  if (dlqChip) {
    dlqChip.addEventListener("click", () => {
      const tabs = el("tabs");
      const btn = tabs && tabs.querySelector('[data-tab="lots"]');
      if (btn) btn.click();
      filter = "failed";
      sort = null;
      shownIds = [];
      render();
    });
  }

  // A hidden tab or collapsed panel is not rebuilt per tick; the tabs-head click re-renders on show.
  let forceFirst = true;
  function visible(host) {
    if (forceFirst) return true;
    const wrap = el("tabs-wrap");
    return !host.classList.contains("hidden") && !(wrap && wrap.classList.contains("collapsed"));
  }

  function renderStations(state) {
    if (!stationsHost || !visible(stationsHost)) return;
    clear(stationsHost);
    const head = mk(doc, "div", "list-head");
    for (const [cls, text] of STATION_COLUMNS) head.appendChild(mk(doc, "span", cls, text));
    stationsHost.appendChild(head);
    for (const r of stationRows(state)) {
      const row = mk(doc, "div", r.selected ? "list-row selected" : "list-row");
      row.setAttribute("role", "button");
      row.tabIndex = 0;
      row.appendChild(mk(doc, "span", "row-name", DISPLAY[r.name]));
      row.appendChild(mk(doc, "span", "row-sub station-machine", machineLabel(r.name)));
      const sc = statusChip(r.status);
      const statusCell = mk(doc, "span", "station-status-cell");
      statusCell.appendChild(mk(doc, "span", `chip ${sc.tone}`, sc.text));
      if (r.retryWait) statusCell.appendChild(mk(doc, "span", "chip amber", "Retry"));
      row.appendChild(statusCell);
      row.appendChild(mk(doc, "span", r.queueTone === "amber" ? "chip amber station-wip" : "chip idle station-wip", String(r.wip)));
      row.appendChild(mk(doc, "span", "row-sub station-avail", r.availText));
      row.appendChild(mk(doc, "span", "row-sub station-util-pct", r.utilization == null ? DASH : `${Math.round(r.utilization * 100)}%`));
      row.appendChild(mk(doc, "span", "row-sub station-tput", r.throughputText));
      const pick = () => choose({ kind: "station", id: r.name });
      row.addEventListener("click", pick);
      row.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          pick();
        }
      });
      stationsHost.appendChild(row);
    }
  }

  function renderLots(state) {
    if (!lotsHost || !listHost || !visible(lotsHost)) return;
    for (const b of filterBar.children) {
      const on = b.dataset.filter === filter;
      b.classList.toggle("active", on);
      b.setAttribute("aria-pressed", on ? "true" : "false");
    }
    clear(listHost);
    let rows = lotRows(state, filter, sort);
    if (hovering) rows = holdOrder(rows, shownIds);
    shownIds = rows.map((r) => r.id);
    if (!rows.length) {
      const c = emptyCopy(filter);
      listHost.appendChild(mk(doc, "p", "empty-copy", c.heading));
      if (c.body) listHost.appendChild(mk(doc, "p", "empty-copy", c.body));
      return;
    }
    const head = mk(doc, "div", "lot-head");
    for (const [cls, text, key] of LOT_COLUMNS) {
      const active = Boolean(sort && sort.key === key);
      const desc = active && lotSortDesc(sort);
      const btn = mk(doc, "button", `${cls} lot-sort${active ? " active" : ""}`, text);
      btn.type = "button";
      btn.title = `Sort by ${text}`;
      btn.setAttribute("aria-sort", active ? (desc ? "descending" : "ascending") : "none");
      if (active) btn.appendChild(mk(doc, "span", "sort-arrow", desc ? "▼" : "▲"));
      btn.addEventListener("click", () => {
        sort = nextLotSort(sort, key);
        shownIds = []; // a header click re-sorts now, even while hovering
        render();
      });
      head.appendChild(btn);
    }
    listHost.appendChild(head);
    for (const r of rows) {
      const row = mk(doc, "div", r.selected ? "list-row selected" : "list-row");
      row.setAttribute("role", "button");
      row.tabIndex = 0;
      row.appendChild(mk(doc, "span", "row-name", r.id));
      row.appendChild(mk(doc, "span", "row-sub lot-product", r.product));
      row.appendChild(mk(doc, "span", isHighPriority(r.priority) ? "chip danger lot-prio" : "chip lot-prio", String(r.priority)));
      row.appendChild(bar(doc, r.progress, true));
      row.appendChild(mk(doc, "span", "row-sub lot-step", r.station ? (DISPLAY[r.station] || r.station) : "—"));
      const status = mk(doc, "span", "lot-status");
      if (r.chip) status.appendChild(mk(doc, "span", `chip ${r.chip.tone}${r.chip.dashed ? " dashed" : ""}`, r.chip.text));
      if (r.failStation) status.appendChild(mk(doc, "span", "row-sub", `${DISPLAY[r.failStation] || r.failStation} visit ${r.visit || 1}`));
      if (r.sub) status.appendChild(mk(doc, "span", "row-sub", r.sub));
      if (r.retryBadge) status.appendChild(mk(doc, "span", "chip amber", r.retryBadge));
      if (status.childNodes.length) row.appendChild(status);
      const pick = () => choose({ kind: "lot", id: r.id });
      row.addEventListener("click", pick);
      row.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          pick();
        }
      });
      listHost.appendChild(row);
    }
  }

  function render() {
    const state = store.getState();
    renderStations(state);
    renderLots(state);
  }

  let last = 0;
  let timer = null;
  store.subscribe(() => {
    const wait = RENDER_MS - (Date.now() - last);
    if (wait <= 0) {
      last = Date.now();
      render();
    } else if (!timer) {
      timer = setTimeout(() => {
        timer = null;
        last = Date.now();
        render();
      }, wait);
    }
  });
  render();
  forceFirst = false;
  const tabsHead = el("tabs-head");
  if (tabsHead) tabsHead.addEventListener("click", () => render());
}
