"""A shared token-bucket rate limiter for pacing aggregate throughput."""
from __future__ import annotations

import asyncio
import time


class TokenBucket:
    """Refills `rate` tokens per second up to `capacity`. Each acquire spends one token,
    waiting when the bucket is empty. One shared instance paces all workers together."""

    def __init__(self, rate: float, capacity: float) -> None:
        self.rate = rate
        self.capacity = capacity
        self._tokens = capacity
        self._updated = time.monotonic()

    async def acquire(self) -> None:
        while True:
            now = time.monotonic()
            self._tokens = min(self.capacity, self._tokens + (now - self._updated) * self.rate)
            self._updated = now
            if self._tokens >= 1:
                self._tokens -= 1
                return
            await asyncio.sleep((1 - self._tokens) / self.rate)
