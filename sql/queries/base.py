from __future__ import annotations

import json
from typing import Any

import asyncpg

from sql.analyze import AnalyzerConfig, analyze_plan
from sql.analyze import logger as analyze_logger
from sql.app import Application
from sql.core.node import Node, QueryContext
from sql.core.types import QueryType
from sql.db import Engine, get_session
from sql.models import QueryModel, RecursiveModel
from sql.queries.values import ValuesNodeMixin


class Query(QueryType, Node):
    app: Application = None
    analyze_config = AnalyzerConfig()

    def _arg(self, value: Any):
        value = super()._arg(value)

        if not self.app:
            if isinstance(value, Query):
                self.app = value.app
            elif isinstance(value, Node):
                self.app = next(
                    (app for m in value.relations if (app := m._app)),
                    None,
                )

        return value

    async def _get_connection(self, app_name: str) -> tuple[asyncpg.Connection, bool]:
        if conn := get_session(app_name):
            return conn, False
        return await Engine.get_active().get_connection(app_name), True

    def __await__(self):
        return self.execute().__await__()

    async def execute(self) -> list[asyncpg.Record]:
        app = self.app
        if not app:
            raise RuntimeError(f"Application не найден для {self.relations}")

        conn, is_internal = await self._get_connection(app.name)

        try:
            if Engine.get_active().is_debug(app.name):
                sql, *params = self.prepare()
                await self._explain_and_analyze(conn, sql, params)
                return await conn.fetch(sql, *params)
            else:
                return await conn.fetch(*self.prepare())
        finally:
            if is_internal:
                await conn.close()

    async def _explain_and_analyze(
        self, conn: asyncpg.Connection, sql: str, params: list
    ):
        is_in_transaction = conn.is_in_transaction()
        transaction_id = f"advisor_{id(self)}"

        try:
            if is_in_transaction:
                await conn.execute(f"SAVEPOINT {transaction_id}")
            else:
                await conn.execute("BEGIN")

            self._analyze_plan(
                sql,
                json.loads(
                    await conn.fetchval(
                        f"EXPLAIN (FORMAT JSON, ANALYZE, BUFFERS) {sql}", *params
                    )
                )[0],
                self.analyze_config,
            )

        except Exception as e:
            analyze_logger.warning(f"Advisor failed: {e}")
        finally:
            if is_in_transaction:
                await conn.execute(f"ROLLBACK TO SAVEPOINT {transaction_id}")
            else:
                await conn.execute("ROLLBACK")

    _analyze_plan = analyze_plan


class ValuesQuery(ValuesNodeMixin, Query):
    def as_model(self, base_model: type[QueryModel] = QueryModel):
        return base_model(self)

    def __or__(self: ValuesQuery, other: ValuesQuery) -> Union:
        return Union(self, other, all=False)

    def __and__(self: ValuesQuery, other: ValuesQuery) -> Union:
        return Union(self, other, all=True)


class Union(ValuesQuery):
    def __init__(self, *queries: ValuesQuery, all: bool = False):
        super().__init__()

        self._queries: list[ValuesQuery] = self._list_arg(queries)
        self._all = all

        if self._queries:
            self._values = self._queries[0]._values

    def __sql__(self, context: QueryContext) -> str:
        parts = [f"({q.__sql__(context)})" for q in self._queries]
        operator = " UNION ALL " if self._all else " UNION "
        return operator.join(parts)


class RecursiveContext:
    def __init__(self, base_query: ValuesQuery):
        self.base_query = base_query
        self.tree_model = RecursiveModel(base_query)

    def __enter__(self):
        return self.tree_model

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False
