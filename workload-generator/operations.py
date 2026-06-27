"""The change operations the workers perform: insert, update, delete."""
from __future__ import annotations

import datetime
from decimal import Decimal

import asyncpg

from state import WorkerState


def _money(value: float) -> Decimal:
    return Decimal(f"{value:.2f}")


async def insert_order(conn: asyncpg.Connection, state: WorkerState) -> int:
    """Insert one order and its line-items in a single transaction. Returns the change-event count.

    rec_id is left to the database sequence on order_goods, so it never collides across runs.
    """
    rng = state.rng
    order_id = state.allocator.next()

    lines = []
    total = Decimal("0.00")
    for _ in range(rng.randint(1, 5)):
        goods_price = _money(rng.uniform(1, 200))
        deal_price = _money(float(goods_price) * rng.uniform(0.6, 1.0))
        quantity = rng.randint(1, 3)
        total += deal_price * quantity
        lines.append((order_id, rng.choice(state.goods_ids), goods_price, deal_price, quantity))

    today = datetime.date.today()
    async with conn.transaction():
        await conn.execute(
            "INSERT INTO order_info (order_id, user_id, country, zipcode, goods_amount, order_date, year) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7)",
            order_id, rng.randint(1, 1_000_000), "US",
            str(rng.randint(10000, 99999)), total, today, today.year,
        )
        await conn.executemany(
            "INSERT INTO order_goods (order_id, goods_id, goods_price, deal_price, goods_number) "
            "VALUES ($1, $2, $3, $4, $5)",
            lines,
        )

    state.recent_orders.append(order_id)
    return 1 + len(lines)


async def update_order(conn: asyncpg.Connection, state: WorkerState) -> int:
    """Change the total on a recent order. Returns the change-event count."""
    rng = state.rng
    order_id = rng.choice(state.recent_orders)
    async with conn.transaction():
        status = await conn.execute(
            "UPDATE order_info SET goods_amount = $1 WHERE order_id = $2",
            _money(rng.uniform(1, 500)), order_id,
        )
    return int(status.split()[-1])


async def delete_order(conn: asyncpg.Connection, state: WorkerState) -> int:
    """Remove a recent order and its line-items. Returns the change-event count."""
    order_id = state.recent_orders.pop()
    async with conn.transaction():
        goods = await conn.execute("DELETE FROM order_goods WHERE order_id = $1", order_id)
        info = await conn.execute("DELETE FROM order_info WHERE order_id = $1", order_id)
    return int(goods.split()[-1]) + int(info.split()[-1])
