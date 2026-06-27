"""PostgreSQL connection pool."""
from __future__ import annotations

import asyncpg

from config import Config


async def create_pool(config: Config) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        dsn=config.database_url,
        min_size=config.workers,
        max_size=config.workers,
    )
