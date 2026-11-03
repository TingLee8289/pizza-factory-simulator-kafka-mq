// Shared numeric-priority helpers (Phase 07.2, D-14..D-16). Smaller number = more urgent.
// Mirrors server PRIORITY_HIGH_THRESHOLD; keep in sync.
export const PRIORITY_MIN = 1;
export const PRIORITY_MAX = 99;
export const PRIORITY_DEFAULT = 50;
export const HIGH_PRIORITY_MAX = 10;

export function isHighPriority(p) {
  const n = Number(p);
  return Number.isFinite(n) && n <= HIGH_PRIORITY_MAX;
}

// Returns the integer priority, or null when not a whole number within 1..99.
export function parsePriority(raw) {
  if (raw === "" || raw === null || raw === undefined || typeof raw === "boolean") return null;
  const n = Number(raw);
  if (!Number.isInteger(n) || n < PRIORITY_MIN || n > PRIORITY_MAX) return null;
  return n;
}

export const PRIORITY_ERROR = `Priority must be a whole number from ${PRIORITY_MIN} to ${PRIORITY_MAX}`;
