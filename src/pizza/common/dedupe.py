"""Pure de-duplication of operations (D-10/D-11).

No broker imports. The station itself never dedupes (D-09); downstream
consumers (recorder, scheduler, aggregator) use this to ignore redeliveries.
"""

import uuid
from dataclasses import dataclass, field
from typing import Protocol


class OpIdentity(Protocol):
    """Anything carrying the identity of one operation."""

    command_event_id: uuid.UUID
    order_id: str
    step: str
    visit: int


@dataclass
class Deduper:
    """Duplicate if EITHER the command event id OR (order_id, step, visit) was seen.

    A rework (new event id, visit + 1) is first-time (D-10).
    """

    _event_ids: set[uuid.UUID] = field(default_factory=set)
    _ops: set[tuple[str, str, int]] = field(default_factory=set)

    def first_time(
        self, command_event_id: uuid.UUID, order_id: str, step: str, visit: int
    ) -> bool:
        key = (order_id, step, visit)
        if command_event_id in self._event_ids or key in self._ops:
            return False
        self._event_ids.add(command_event_id)
        self._ops.add(key)
        return True

    def first_time_op(self, op: OpIdentity) -> bool:
        return self.first_time(op.command_event_id, op.order_id, op.step, op.visit)
