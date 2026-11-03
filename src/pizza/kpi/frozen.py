"""Pure frozen-interval helpers for the active-time transform (D-04).

While the line is frozen (Stop), stations stay busy and orders stay in flight,
so wall-clock durations overstate observation time. KPI math therefore uses
*active* seconds: wall-clock seconds minus the overlap with frozen intervals.
No I/O, no clock: ``as_of`` is always passed in.
"""

import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime

Interval = tuple[datetime, datetime]
# (start, end, scale): wall interval during which one simulation speed was in effect.
ScaleSegment = tuple[datetime, datetime, float]

_FAR_FUTURE = datetime.max.replace(tzinfo=UTC)


def frozen_intervals(
    marks: Mapping[uuid.UUID, tuple[datetime, bool]], as_of: datetime
) -> list[Interval]:
    """Derive sorted, non-overlapping frozen intervals from pause/resume marks.

    ``marks`` maps event_id to ``(ts, is_pause)``. A pause while already paused
    and a resume while not paused are ignored; an open interval ends at
    ``as_of``. Deterministic regardless of insertion order.
    """
    ordered = sorted(marks.items(), key=lambda kv: (kv[1][0], not kv[1][1], str(kv[0])))
    out: list[Interval] = []
    start: datetime | None = None
    for _eid, (ts, is_pause) in ordered:
        if is_pause:
            if start is None:
                start = ts
        elif start is not None:
            out.append((start, ts))
            start = None
    if start is not None and as_of > start:
        out.append((start, as_of))
    return out


def frozen_overlap(a: datetime, b: datetime, intervals: Sequence[Interval]) -> float:
    """Seconds of ``[a, b]`` covered by the (non-overlapping) intervals."""
    total = 0.0
    for lo, hi in intervals:
        s = max(a, lo)
        e = min(b, hi)
        if e > s:
            total += (e - s).total_seconds()
    return total


def active_seconds(a: datetime, b: datetime, intervals: Sequence[Interval]) -> float:
    """``b - a`` in seconds minus frozen overlap; plain difference if ``b < a``."""
    raw = (b - a).total_seconds()
    if b < a:
        return raw
    return raw - frozen_overlap(a, b, intervals)


def scale_segments(
    marks: Mapping[uuid.UUID, tuple[datetime, float]], as_of: datetime
) -> list[ScaleSegment]:
    """Derive wall segments with a constant scale from ``time_scale.changed`` marks.

    ``marks`` maps event_id to ``(ts, scale)``. Time before the first mark counts as
    1x; the last segment is open-ended. Marks later than ``as_of`` are ignored.
    Deterministic regardless of insertion order (ties broken by event_id).
    """
    ordered = sorted(
        ((ts, str(eid), scale) for eid, (ts, scale) in marks.items() if ts <= as_of),
        key=lambda m: (m[0], m[1]),
    )
    out: list[ScaleSegment] = []
    for i, (ts, _eid, scale) in enumerate(ordered):
        end = ordered[i + 1][0] if i + 1 < len(ordered) else _FAR_FUTURE
        out.append((ts, end, scale))
    return out


def sim_seconds(
    a: datetime,
    b: datetime,
    intervals: Sequence[Interval],
    segments: Sequence[ScaleSegment],
) -> float:
    """Simulation seconds in ``[a, b]``: active wall seconds weighted by the scale.

    Each wall slice has its frozen overlap removed and is multiplied by the scale in
    effect for that slice (1x outside every segment). With no segments this equals
    ``active_seconds``; ``b < a`` keeps the raw wall difference.
    """
    if not segments:
        return active_seconds(a, b, intervals)
    raw = (b - a).total_seconds()
    if b < a:
        return raw
    total = 0.0
    cursor = a
    for lo, hi, scale in segments:
        if hi <= a or lo >= b:
            continue
        if lo > cursor:  # uncovered gap before this segment runs at 1x
            total += _slice(cursor, lo, intervals, 1.0)
        s = max(lo, cursor)
        e = min(hi, b)
        if e > s:
            total += _slice(s, e, intervals, scale)
            cursor = e
    if cursor < b:
        total += _slice(cursor, b, intervals, 1.0)
    return total


def _slice(a: datetime, b: datetime, intervals: Sequence[Interval], scale: float) -> float:
    return ((b - a).total_seconds() - frozen_overlap(a, b, intervals)) * scale


@dataclass(frozen=True)
class ActiveClock:
    """Frozen intervals plus scale segments: converts wall spans to active sim seconds."""

    frozen: Sequence[Interval] = ()
    segments: Sequence[ScaleSegment] = field(default_factory=tuple)

    def seconds(self, a: datetime, b: datetime) -> float:
        return sim_seconds(a, b, self.frozen, self.segments)

    def unpaused_seconds(self, a: datetime, b: datetime) -> float:
        """Sim seconds in ``[a, b]`` with scale applied but pauses NOT removed."""
        return sim_seconds(a, b, (), self.segments)
