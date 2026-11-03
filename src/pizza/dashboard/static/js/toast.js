// Toasts: DOM built with createElement/textContent only.
const MAX_TOASTS = 3;
const INFO_MS = 4000;

export function showToast({ text, kind = "info" }, doc = document) {
  const host = doc.getElementById("toasts");
  if (!host) return null;
  while (host.children.length >= MAX_TOASTS) host.removeChild(host.firstChild);
  const item = doc.createElement("div");
  item.className = `toast ${kind}`;
  item.setAttribute("role", kind === "error" ? "alert" : "status");
  const msg = doc.createElement("span");
  msg.textContent = text;
  item.appendChild(msg);
  const remove = () => { if (item.parentNode) item.parentNode.removeChild(item); };
  if (kind === "error") {
    const close = doc.createElement("button");
    close.type = "button";
    close.className = "toast-close";
    close.setAttribute("aria-label", "Close");
    close.textContent = "✕";
    close.addEventListener("click", remove);
    item.appendChild(close);
  } else {
    setTimeout(remove, INFO_MS);
  }
  host.appendChild(item);
  return item;
}
