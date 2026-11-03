"""Seeded independent RNG streams shared by the generator and the failure model."""

import hashlib
import random


def stream_rng(seed: int, stream: str) -> random.Random:
    """Independent stream; sha256 avoids dependence on CPython str-seeding (A4)."""
    digest = hashlib.sha256(f"{seed}:{stream}".encode()).digest()
    return random.Random(int.from_bytes(digest[:8], "big"))
