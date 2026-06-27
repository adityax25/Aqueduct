"""A single worker: performs change operations in the configured mix until signalled to stop."""
from __future__ import annotations

import asyncio
import logging

import asyncpg

from operations import delete_order, insert_order, update_order
from ratelimit import TokenBucket
from state import WorkerState
from stats import Counter

log = logging.getLogger("workload-generator.worker")

_OPS = ("insert", "update", "delete")


async def run_worker(
    state: WorkerState,
    pool: asyncpg.Pool,
    stop: asyncio.Event,
    counter: Counter,
    weights: tuple[float, float, float],
    limiter: TokenBucket | None,
) -> None:
    while not stop.is_set():
        if limiter is not None:
            await limiter.acquire()
        choice = state.rng.choices(_OPS, weights=weights)[0]
        try:
            async with pool.acquire() as conn:
                if choice == "update" and state.recent_orders:
                    events = await update_order(conn, state)
                elif choice == "delete" and state.recent_orders:
                    events = await delete_order(conn, state)
                else:
                    events = await insert_order(conn, state)
            counter.operations += 1
            counter.events += events
        except Exception:
            log.exception("operation failed")
            counter.errors += 1
            await asyncio.sleep(0.1)
