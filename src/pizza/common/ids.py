"""Deterministic id helpers (D-10, D-14). No broker imports."""

import uuid

_NAMESPACE = uuid.UUID("6f1c2a3e-0000-4000-8000-000000000003")


def job_event_id(run_id: str, order_id: str, step: str, visit: int = 1) -> uuid.UUID:
    """Stable event id for a job command; redispatch after a restart reuses it."""
    return uuid.uuid5(_NAMESPACE, "|".join((run_id, order_id, step, str(visit))))


def make_order_id(seed: int, n: int) -> str:
    """Lot id: "L-" plus the 4-digit zero-padded sequence number.

    seed is accepted for signature stability but is no longer part of the id,
    so ids repeat across runs (run_id disambiguates).
    """
    return f"L-{n:04d}"
