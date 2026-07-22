"""Per-worker state and the shared id allocator for generated orders."""
from __future__ import annotations

import random
from collections import deque
from dataclasses import dataclass, field


class IdAllocator:
    """Hands out unique, increasing order ids. Safe under asyncio because allocation never awaits."""

    def __init__(self, start: int) -> None:
        self._next = start

    def next(self) -> int:
        value = self._next
        self._next += 1
        return value


@dataclass
class WorkerState:
    worker_id: int
    rng: random.Random
    goods_ids: list[int]        # real product ids to reference, sampled at startup
    allocator: IdAllocator      # shared across workers
    anomaly_rate: float         # probability a line-item is injected as a pricing anomaly
    recent_orders: deque = field(default_factory=lambda: deque(maxlen=1000))
