from typing import Self

import asyncpg

from .query import Q


class Engine:
    def __init__(self, url: str):
        # Чистим URL для asyncpg (он не понимает префиксы sqlalchemy)
        self.url = url.replace("postgresql+asyncpg://", "postgresql://")
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> Self:
        if not self._pool:
            self._pool = await asyncpg.create_pool(self.url)
        return self

    async def disconnect(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def fetch(self, query_obj: Q) -> list[asyncpg.Record]:
        """Для SELECT: возвращает список Record (доступ по ключам или индексам)"""
        if not self._pool:
            raise RuntimeError("Engine not connected")

        sql, *args = list(query_obj)
        async with self._pool.acquire() as conn:
            return await conn.fetch(sql, *args)

    async def fetch_row(self, query_obj: Q) -> asyncpg.Record | None:
        """Для SELECT LIMIT 1 или RETURNING"""
        if not self._pool:
            raise RuntimeError("Engine not connected")

        sql, *args = list(query_obj)
        async with self._pool.acquire() as conn:
            return await conn.fetchrow(sql, *args)

    async def execute(self, query_obj: Q | str) -> str:
        """Для INSERT/UPDATE/DELETE без возвращаемых значений"""
        if not self._pool:
            raise RuntimeError("Engine not connected")

        if isinstance(query_obj, Q):
            sql, *args = list(query_obj)
            async with self._pool.acquire() as conn:
                return await conn.execute(sql, *args)
        else:
            async with self._pool.acquire() as conn:
                return await conn.execute(query_obj)
