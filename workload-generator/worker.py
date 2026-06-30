"""A single worker: performs change operations in the configured mix until signalled to stop."""
from __future__ import annotations

import asyncio
import logging
import time

import asyncpg

from metrics import ERRORS, EVENTS, OPERATIONS, OPERATION_SECONDS
from operations import delete_order, insert_order, update_order
from ratelimit import TokenBucket
from state import WorkerState
from stats import Counter

log = logging.getLogger("workload-generator.worker")


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

        choice = state.rng.choices(("insert", "update", "delete"), weights=weights)[0]
        if choice == "update" and state.recent_orders:
            op, action = "update", update_order
        elif choice == "delete" and state.recent_orders:
            op, action = "delete", delete_order
        else:
            op, action = "insert", insert_order

        started = time.monotonic()
        try:
            async with pool.acquire() as conn:
                events = await action(conn, state)
            counter.operations += 1
            counter.events += events
            OPERATIONS.labels(op=op).inc()
            EVENTS.inc(events)
            OPERATION_SECONDS.labels(op=op).observe(time.monotonic() - started)
        except Exception:
            log.exception("operation failed")
            counter.errors += 1
            ERRORS.inc()
            await asyncio.sleep(0.1)
