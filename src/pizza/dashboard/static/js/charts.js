// Trend charts (Chart.js, vendored global). Data comes from state.history only.
const UPDATE_MS = 1000;
const ACCENT = "#2f6bff";
const MUTED = "#6a7794";

const SPECS = [
  { id: "chart-throughput", series: [["pizzas_per_min", "pizzas/min", ACCENT]] },
  { id: "chart-wip", series: [["wip_pizzas", "WIP now (pizzas)", ACCENT], ["wip_avg_pizzas", "WIP avg (pizzas)", MUTED]] },
  // cycle history is stored in seconds; the chart plots minutes (scale 1/60)
  { id: "chart-cycle", series: [["cycle_mean_s", "mean min/pizza", ACCENT, 1 / 60], ["cycle_p95_s", "p95 min/pizza", MUTED, 1 / 60]] },
];

export function formatTrendTime(t) {
  const d = new Date(t * 1000);
  const p2 = (n) => String(n).padStart(2, "0");
  // Two lines (date, then time): Chart.js draws array labels as stacked lines, so the year is never cut off.
  return [`${d.getFullYear()}/${p2(d.getMonth() + 1)}/${p2(d.getDate())}`, `${p2(d.getHours())}:${p2(d.getMinutes())}`];
}

export function seriesFrom(history, key, cap = 600) {
  const out = history.map((p) => (typeof p[key] === "number" ? p[key] : null));
  return out.length > cap ? out.slice(out.length - cap) : out;
}

export function init(ctx) {
  const cap = ctx.store.HISTORY_CAP;
  const charts = [];
  let lastKey = "";

  function build() {
    if (typeof window === "undefined" || !window.Chart) return false;
    for (const spec of SPECS) {
      const canvas = ctx.el(spec.id);
      if (!canvas) continue;
      const chart = new window.Chart(canvas, {
        type: "line",
        data: {
          labels: [],
          datasets: spec.series.map(([key, label, color, scale = 1]) => ({
            key, label, scale, data: [], borderColor: color, borderWidth: 2, pointRadius: 0, tension: 0.2,
          })),
        },
        options: {
          animation: false,
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { display: false } },
          scales: { x: { ticks: { maxTicksLimit: 5, minRotation: 0, maxRotation: 0 } }, y: { beginAtZero: true } },
        },
      });
      charts.push(chart);
    }
    return charts.length > 0;
  }

  function tick() {
    if (!charts.length && !build()) return;
    const history = ctx.store.getState().history;
    const last = history.length ? history[history.length - 1].t : "";
    const key = `${history.length}:${last}`;
    if (key === lastKey) return;
    lastKey = key;
    const labels = history.map((p, i) => (typeof p.t === "number" ? formatTrendTime(p.t) : [String(i)]));
    for (const chart of charts) {
      chart.data.labels = labels.length > cap ? labels.slice(-cap) : labels;
      for (const ds of chart.data.datasets) {
        ds.data = seriesFrom(history, ds.key, cap).map((v) => (v === null ? null : v * ds.scale));
      }
      chart.update("none");
    }
  }

  setInterval(tick, UPDATE_MS);
}
