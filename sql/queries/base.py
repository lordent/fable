from __future__ import annotations

import json
import logging
from typing import Any, Self, TypeVar

import asyncpg

from sql.app import Application

from ..core import E, Query
from ..db import Engine, get_session
from ..fields import Field
from ..model import Model, QueryModel

logger = logging.getLogger("sql_builder.analyzer")

T = TypeVar("T", bound="Model")


class QueryBuilder(Query):
    def as_model(self, base_model: type[QueryModel] = QueryModel):
        return base_model.factory(self)

    async def _get_connection(self, app_name: str) -> tuple[asyncpg.Connection, bool]:
        conn = get_session(app_name)
        if conn:
            return conn, False
        return await Engine.get_active().get_connection(app_name), True

    def __await__(self):
        return self.execute().__await__()

    async def execute(self) -> list[asyncpg.Record]:
        app: Application = next(
            (
                getattr(m, "_app", None)
                for m in self.relations
                if getattr(m, "_app", None)
            ),
            None,
        )

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
        self, conn: asyncpg.Connection, sql: str, params: list[Any]
    ):
        is_in_transaction = conn.is_in_transaction()

        try:
            if not is_in_transaction:
                await conn.execute("BEGIN")
            else:
                await conn.execute(f"SAVEPOINT advisor_{id(self)}")

            explain_query = f"EXPLAIN (FORMAT JSON, ANALYZE, BUFFERS) {sql}"
            raw_plan = await conn.fetchval(explain_query, *params)

            plan = json.loads(raw_plan)[0]
            self._analyze_plan(sql, plan)

        except Exception as e:
            logger.warning(f"Advisor failed: {e}")
        finally:
            if not is_in_transaction:
                await conn.execute("ROLLBACK")
            else:
                await conn.execute(f"ROLLBACK TO SAVEPOINT advisor_{id(self)}")

    def _analyze_plan(self, sql: str, plan_data: dict):
        issues = []
        root_node = plan_data.get("Plan", {})

        def walk(node: dict):
            node_type = node.get("Node Type")
            actual_rows = node.get("Actual Rows", 0)

            # АЛЕРТ 1: Sequential Scan на больших объемах (отсутствие индекса)
            if node_type == "Seq Scan" and actual_rows > 100:
                rel = node.get("Relation Name", "unknown")
                issues.append(
                    f"🔴 SEQ SCAN на '{rel}':"
                    "таблица сканируется целиком. Добавьте индекс!"
                )

            # АЛЕРТ 2: Использование диска для сортировки (мало work_mem)
            if "Sort Method" in node and "external" in node["Sort Method"].lower():
                issues.append(
                    "🟡 SORT ON DISK: сортировка не влезла в память. "
                    "Увеличьте 'work_mem'."
                )

            # АЛЕРТ 3: Большая разница между планом и реальностью (протухшая статистика)
            est_rows = node.get("Plan Rows", 1)
            if actual_rows > 0 and (
                actual_rows / est_rows > 10 or est_rows / actual_rows > 10
            ):
                issues.append(
                    f"🟠 STATS ERROR: оценка строк ({est_rows}) сильно разнится"
                    f"с реальностью ({actual_rows}). Сделайте ANALYZE."
                )

            for sub_plan in node.get("Plans", []):
                walk(sub_plan)

        walk(root_node)

        if issues:
            print("\n" + "═" * 60)
            print("🚀 SQL PERFORMANCE ADVISOR")
            print(f"Query: {sql[:100]}...")
            print(f"Total Time: {plan_data.get('Execution Time', 0)}ms")
            for msg in issues:
                print(f"  {msg}")
            print("═" * 60 + "\n")


class SelectValuesQuery(QueryBuilder):
    def __init__(self, *args: Field, **kwargs: dict[str, Any]):
        super().__init__()
        if args or kwargs:
            self.values(*args, **kwargs)

    def values(self, *args: Field, **kwargs: Any) -> Self:
        for f in args:
            if not isinstance(f, Field):
                raise ValueError(
                    f"Аргумент {f} должен быть Field. Для выражений используйте kwargs."
                )
            self._values[f.name] = f
            self.relations |= f.relations

        for alias, v in kwargs.items():
            self._values[alias] = v
            if isinstance(v, E):
                self.relations |= v.relations
        return self


class Item(SelectValuesQuery):
    def _json_build_recursive(self, fields: dict, params: list[Any]) -> str:
        tokens = []
        for name, value in fields.items():
            tokens.append(f"'{name}'")
            tokens.append(self._value(value, params))
        return f"JSONB_BUILD_OBJECT({', '.join(tokens)})"

    def __sql__(self, params: list[Any]):
        return self._json_build_recursive(self._values, params)


class List(Item):
    def __init__(self, *args: Field, **kwargs: dict[str, Any]):
        super().__init__(*args, **kwargs)
        self.is_aggregate = True

    def __sql__(self, params: list[Any]) -> str:
        inner_json = super().__sql__(params)
        return f"COALESCE(JSONB_AGG({inner_json}), '[]'::jsonb)"
