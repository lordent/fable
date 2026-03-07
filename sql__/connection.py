from typing import Self

import asyncpg

from .query import Q


class Database:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> Self:
        if not self._pool:
            self._pool = await asyncpg.create_pool(self.dsn)
        return self

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()

    async def fetch(self, query_obj: Q) -> list[asyncpg.Record]:
        """Выполняет SELECT и возвращает список записей."""
        if not self._pool:
            raise RuntimeError("Database not connected")

        sql, *args = list(query_obj)
        async with self._pool.acquire() as conn:
            return await conn.fetch(sql, *args)

    async def execute(self, query_obj: Q) -> str:
        """Выполняет INSERT/UPDATE/DELETE."""
        if not self._pool:
            raise RuntimeError("Database not connected")

        sql, *args = list(query_obj)
        async with self._pool.acquire() as conn:
            return await conn.execute(sql, *args)

    async def fetch_row(self, query_obj: Q) -> asyncpg.Record | None:
        """Возвращает одну строку (удобно для .returning())."""
        if not self._pool:
            raise RuntimeError("Database not connected")

        sql, *args = list(query_obj)
        async with self._pool.acquire() as conn:
            return await conn.fetchrow(sql, *args)
