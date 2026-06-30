"""Workload generator entrypoint."""
from __future__ import annotations

import asyncio
import logging
import random
import signal

from config import Config
from db import create_pool
from metrics import start_metrics_server
from ratelimit import TokenBucket
from state import IdAllocator, WorkerState
from stats import Counter
from worker import run_worker

log = logging.getLogger("workload-generator")


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


async def report(counter: Counter, stop: asyncio.Event) -> None:
    last_ops = last_events = last_errors = 0
    while not stop.is_set():
        await asyncio.sleep(1)
        ops, events, errors = counter.operations, counter.events, counter.errors
        log.info(
            "ops/s=%s events/s=%s errors/s=%s total_ops=%s total_events=%s total_errors=%s",
            ops - last_ops, events - last_events, errors - last_errors, ops, events, errors,
        )
        last_ops, last_events, last_errors = ops, events, errors


async def run(config: Config) -> None:
    start_metrics_server(config.metrics_port)
    log.info("metrics available at http://localhost:%s/metrics", config.metrics_port)

    pool = await create_pool(config)
    log.info("connected to database, pool size %s", config.workers)

    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT goods_id FROM goods_info LIMIT 5000")
        start_id = await conn.fetchval(
            "SELECT COALESCE(MAX(order_id) + 1, $1) FROM order_info WHERE order_id >= $1",
            config.id_offset,
        )
    goods_ids = [r["goods_id"] for r in rows]
    allocator = IdAllocator(start_id)
    log.info("loaded %s product ids, allocating new order ids from %s", len(goods_ids), start_id)

    limiter = None if config.burst else TokenBucket(config.target_rate, max(config.target_rate, 1.0))
    log.info("rate limit: %s", "burst mode (no limit)" if config.burst else f"{config.target_rate} ops/s")

    stop = asyncio.Event()
    counter = Counter()
    weights = (config.insert_weight, config.update_weight, config.delete_weight)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    states = [
        WorkerState(
            worker_id=i,
            rng=random.Random(config.seed + i),
            goods_ids=goods_ids,
            allocator=allocator,
        )
        for i in range(config.workers)
    ]

    tasks = [
        asyncio.create_task(run_worker(s, pool, stop, counter, weights, limiter))
        for s in states
    ]
    tasks.append(asyncio.create_task(report(counter, stop)))

    if config.duration_seconds > 0:
        try:
            await asyncio.wait_for(stop.wait(), timeout=config.duration_seconds)
        except asyncio.TimeoutError:
            stop.set()
    else:
        await stop.wait()

    log.info("draining workers")
    await asyncio.gather(*tasks, return_exceptions=True)
    await pool.close()
    log.info(
        "stopped, total_ops=%s total_events=%s total_errors=%s",
        counter.operations, counter.events, counter.errors,
    )


def main() -> None:
    configure_logging()
    asyncio.run(run(Config.from_env()))


if __name__ == "__main__":
    main()
