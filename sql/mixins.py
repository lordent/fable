from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

import asyncpg

from .core import Expr
from .db import Engine, _session_ctx
from .types import with_type

if TYPE_CHECKING:
    pass

logger = logging.getLogger("sql_builder.analyzer")


class ExecutableMixin(with_type(Expr)):
    async def _get_connection(self, app_name: str):
        conn = _session_ctx.get()
        if conn:
            return conn, False
        return await Engine.get_active().get_connection(app_name), True

    def __await__(self):
        return self.execute().__await__()

    async def execute(self) -> list[dict[str, Any]]:
        if not self.relations:
            raise RuntimeError(
                "Запрос не привязан ни к одной модели (relations is empty)"
            )

        any_model = next(iter(self.relations))
        app_name = any_model._app.name
        engine = Engine.get_active()

        conn, is_internal = await self._get_connection(app_name)

        try:
            sql, params = self.prepare()

            if engine.is_debug(app_name):
                await self._explain_and_analyze(conn, sql, params)

            return await conn.fetch(sql, *params)
        finally:
            if is_internal:
                await conn.close()

    async def _explain_and_analyze(
        self, conn: asyncpg.Connection, sql: str, params: list[Any]
    ):
        try:
            explain_query = f"EXPLAIN (FORMAT JSON, ANALYZE, BUFFERS) {sql}"
            raw_plan = await conn.fetchval(explain_query, *params)

            plan = json.loads(raw_plan)[0]
            self._analyze_plan(sql, plan)
        except Exception as e:
            logger.warning(f"Failed to auto-explain query: {e}")

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
