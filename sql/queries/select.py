from __future__ import annotations

from enum import StrEnum
from typing import Any, Self, TypedDict, cast

from ..core import E, OrderBy, Q
from ..fields import Field
from ..model import Model
from .base import SelectValuesQuery


class JoinStrategy(StrEnum):
    LEFT = "JOIN"
    INNER = "INNER JOIN"
    RIGHT = "RIGHT JOIN"
    FULL = "FULL OUTER JOIN"


class LockMode(StrEnum):
    UPDATE = "FOR UPDATE"
    SHARE = "FOR SHARE"


class LockDict(TypedDict):
    mode: LockMode
    of: set[type[Model]]
    nowait: bool
    skip_locked: bool


class GroupMode(StrEnum):
    ROLLUP = "ROLLUP"
    CUBE = "CUBE"
    SETS = "GROUPING SETS"


class SummaryDict(TypedDict):
    mode: GroupMode
    fields: list[E | Field]


class JoinDict(TypedDict):
    on: Q
    strategy: JoinStrategy


class Select(SelectValuesQuery):
    def __init__(self, *args: Field, **kwargs: E | Any):
        self._joins: dict[type[Model], JoinDict] = {}
        self._where: Q | None = None
        self._having: Q | None = None
        self._order_by: list[OrderBy] = []
        self._group_by_manual = []
        self._limit: int | None = None
        self._offset: int | None = None
        self._lock: LockDict | None = None
        self._summary: SummaryDict | None = None

        super().__init__(*args, **kwargs)

    def _set_lock(
        self,
        mode: LockMode,
        of: tuple[type[Model], ...],
        nowait: bool,
        skip_locked: bool,
    ) -> Self:
        self._lock = {
            "mode": mode,
            "of": set(of),
            "nowait": nowait,
            "skip_locked": skip_locked,
        }
        self.relations |= self._lock["of"]
        return self

    def for_update(
        self, *of: type[Model], nowait: bool = False, skip_locked: bool = False
    ) -> Self:
        return self._set_lock(LockMode.UPDATE, of, nowait, skip_locked)

    def for_share(
        self, *of: type[Model], nowait: bool = False, skip_locked: bool = False
    ) -> Self:
        return self._set_lock(LockMode.SHARE, of, nowait, skip_locked)

    def summary(self, *fields: Field | E, mode: GroupMode = GroupMode.ROLLUP) -> Self:
        self._summary = {"mode": mode, "fields": list(fields)}
        for f in fields:
            if hasattr(f, "relations"):
                self.relations |= f.relations
        return self

    def filter(self, expr: Q) -> Self:
        if expr.is_aggregate:
            self._having = (self._having & expr) if self._having else expr
        else:
            self._where = (self._where & expr) if self._where else expr
        self.relations |= expr.relations
        return self

    def join(
        self,
        target: type[Model],
        on: Q | None = None,
        strategy: JoinStrategy = JoinStrategy.LEFT,
    ) -> Self:
        if not on:
            for field in target._foreign_fields.values():
                if (to := field.to) in self.relations:
                    on = field == cast(Model, to).id
                    break
            if not on:
                for rel in self.relations:
                    for field in rel._foreign_fields.values():
                        if field.to == target:
                            on = field == target.id
                            break
                    if on:
                        break
        if not on:
            raise ValueError(f"Связь для {target._alias} не найдена.")
        self._joins[target] = {"on": on, "strategy": strategy}
        self.relations |= {target} | on.relations
        return self

    def order_by(self, *exprs: E | OrderBy) -> Self:
        for e in exprs:
            self._order_by.append(e if isinstance(e, OrderBy) else e.asc())
            self.relations |= e.relations
        return self

    def group_by(self, *fields: Field | E) -> Self:
        self._group_by_manual.extend(fields)
        for f in fields:
            if hasattr(f, "relations"):
                self.relations |= f.relations
        return self

    def limit(self, val: int) -> Self:
        self._limit = val
        return self

    def offset(self, val: int) -> Self:
        self._offset = val
        return self

    def __sql__(self, params: list[Any]) -> str:
        has_aggregate = any(
            getattr(v, "is_aggregate", False) for v in self._values.values()
        )

        cols, group_by = [], []

        for alias, expr in self._values.items():
            val_sql = self._value(expr, params)

            if has_aggregate and isinstance(expr, E):
                if not expr.is_aggregate and not getattr(expr, "window", None):
                    group_by.append(val_sql)

            cols.append(f'{val_sql} AS "{alias}"')

        if has_aggregate and self._order_by:
            for ob in self._order_by:
                ob_sql = self._value(ob.wrapped, params)
                if not getattr(ob.wrapped, "is_aggregate", False):
                    group_by.append(ob_sql)

        sql = [f"SELECT {', '.join(cols)}"]

        joined_models = set(self._joins.keys())
        main_models = [m for m in self.relations if m not in joined_models]

        if main_models:
            sql.append(f"FROM {', '.join(m.__sql__(params) for m in main_models)}")

        for target, join_data in self._joins.items():
            strategy = join_data["strategy"].value
            on_sql = join_data["on"].__sql__(params)
            sql.append(f"{strategy} {target.__sql__(params)} ON {on_sql}")

        if self._where:
            sql.append(f"WHERE {self._where.__sql__(params)}")

        manual_group_sql = [self._value(f, params) for f in self._group_by_manual]

        final_group_list = group_by + manual_group_sql

        if final_group_list:
            group_fields_unique = list(dict.fromkeys(final_group_list))

            if self._summary:
                summary = self._summary
                summary_sql_list = [self._value(f, params) for f in summary["fields"]]

                regular_group = [
                    f for f in group_fields_unique if f not in summary_sql_list
                ]

                summary_clause = (
                    f"{summary['mode'].value}({', '.join(summary_sql_list)})"
                )

                if regular_group:
                    sql.append(f"GROUP BY {', '.join(regular_group)}, {summary_clause}")
                else:
                    sql.append(f"GROUP BY {summary_clause}")
            else:
                sql.append(f"GROUP BY {', '.join(group_fields_unique)}")

        if self._having:
            sql.append(f"HAVING {self._having.__sql__(params)}")

        if self._order_by:
            sql.append(
                f"ORDER BY {', '.join(e.__sql__(params) for e in self._order_by)}"
            )

        if self._limit is not None:
            sql.append(f"LIMIT {self._limit}")
        if self._offset is not None:
            sql.append(f"OFFSET {self._offset}")

        if self._lock:
            lock = self._lock
            parts = [lock["mode"]]
            if lock["of"]:
                parts.append(f"OF {', '.join(f'"{m._alias}"' for m in lock['of'])}")
            if lock["nowait"]:
                parts.append("NOWAIT")
            elif lock["skip_locked"]:
                parts.append("SKIP LOCKED")
            sql.append(" ".join(parts))

        return " ".join(sql)
