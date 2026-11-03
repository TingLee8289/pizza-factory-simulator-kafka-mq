// Tiny hand-drawn sparklines (no chart library for ~20 mini charts).
const isNum = (v) => typeof v === "number" && Number.isFinite(v);

export function sparklinePath(values, width, height) {
  if (!Array.isArray(values) || values.length === 0) return [];
  const nums = values.filter(isNum);
  if (nums.length === 0) return [];
  const min = Math.min(...nums);
  const max = Math.max(...nums);
  const span = max - min;
  const step = values.length > 1 ? width / (values.length - 1) : 0;
  const pts = [];
  values.forEach((v, i) => {
    if (!isNum(v)) return;
    const y = span === 0 ? height / 2 : height - ((v - min) / span) * height;
    pts.push({ x: i * step, y, gap: i > 0 && !isNum(values[i - 1]) });
  });
  return pts;
}

export function drawSparkline(canvas, values, { stroke = "#6a7794" } = {}) {
  const ctx = canvas.getContext && canvas.getContext("2d");
  if (!ctx) return;
  const w = canvas.width;
  const h = canvas.height;
  ctx.clearRect(0, 0, w, h);
  const pts = sparklinePath(values, w, h - 2);
  if (pts.length === 0) return;
  ctx.strokeStyle = stroke;
  ctx.lineWidth = 1.5;
  ctx.beginPath();
  pts.forEach((p, i) => {
    if (i === 0 || p.gap) ctx.moveTo(p.x, p.y + 1);
    else ctx.lineTo(p.x, p.y + 1);
  });
  ctx.stroke();
}
