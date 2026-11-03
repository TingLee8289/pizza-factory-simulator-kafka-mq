"""Rework policy: pure decision. No broker or asyncio imports."""

from collections.abc import Mapping
from dataclasses import dataclass

DEFAULT_REWORK_LIMITS: dict[str, int] = {
    "spin": 2,
    "package": 2,
    "sauce": 0,
    "cheese": 0,
    "toppings": 0,
    "bake": 0,
    "slice": 0,
}


@dataclass(frozen=True)
class Rework:
    next_visit: int


@dataclass(frozen=True)
class DeadLetter:
    reason: str = "rework_exhausted"


def decide(step: str, visit: int, limits: Mapping[str, int]) -> Rework | DeadLetter:
    """Rework (visit + 1) while visit <= limit for the step, else dead-letter."""
    if visit <= limits.get(step, 0):
        return Rework(visit + 1)
    return DeadLetter()
