from __future__ import annotations

from enum import StrEnum
from typing import Any, Self, TypedDict

from sql.core.expressions import Expression, Q, Ref
from sql.fields.base import Field
from sql.queries.base import RecursiveContext, ValuesQuery
from sql.utils import quote_ident

from ..core.base import OrderBy, QueryContext
from ..models import Model
from .values import Item, List


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
    fields: list[Expression | Field]


class JoinDict(TypedDict):
    on: Q
    strategy: JoinStrategy


class Select(ValuesQuery):
    Item = Item
    List = List
    Join = JoinStrategy
    Summary = GroupMode
    Lock = LockMode

    def __init__(self, *args: Field, **kwargs: Any):
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

    def recursive(self):
        return RecursiveContext(self)

    def for_update(
        self, *of: type[Model], nowait: bool = False, skip_locked: bool = False
    ) -> Self:
        return self._set_lock(LockMode.UPDATE, of, nowait, skip_locked)

    def for_share(
        self, *of: type[Model], nowait: bool = False, skip_locked: bool = False
    ) -> Self:
        return self._set_lock(LockMode.SHARE, of, nowait, skip_locked)

    def summary(
        self, *args: Field | Expression, mode: GroupMode = GroupMode.ROLLUP
    ) -> Self:
        self._summary = {"mode": mode, "fields": self._list_arg(args)}
        return self

    def filter(self, *args: Q) -> Self:
        for a in args:
            a: Q = self._arg(a)
            if a.is_aggregate:
                self._having = (self._having & a) if self._having else a
            else:
                self._where = (self._where & a) if self._where else a
        return self

    def join(
        self,
        target: type[Model],
        on: Q | None = None,
        strategy: JoinStrategy = JoinStrategy.LEFT,
    ) -> Self:
        if not on:
            for field in target._foreign_fields.values():
                if (to := field.to) in self.relations and to != target:
                    on = field == to.id
                    break
            if not on:
                for rel in self.relations:
                    if rel == target:
                        continue
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

    def order_by(self, *args: Expression | OrderBy) -> Self:
        self._order_by.extend(
            self._list_arg([a if isinstance(a, OrderBy) else a.asc() for a in args])
        )
        return self

    def group_by(self, *fields: Field | Expression) -> Self:
        self._group_by_manual.extend(self._list_arg(fields))
        return self

    def limit(self, val: int) -> Self:
        self._limit = val
        return self

    def offset(self, val: int) -> Self:
        self._offset = val
        return self

    def __sql__(self, context: QueryContext) -> str:
        has_aggregate = any(
            value.is_aggregate
            for value in self._values.values()
            if isinstance(value, Expression)
        )

        cols, group_by = [], []

        for alias, value in self._values.items():
            sql_ = self._value(value, context)
            if has_aggregate and isinstance(value, Expression):
                if not value.is_aggregate and not value.is_windowed:
                    group_by.append(sql_)

            cols.append(f"{sql_} AS {quote_ident(alias)}")

        if has_aggregate and self._order_by:
            for a in self._order_by:
                wrapped = a.wrapped

                if isinstance(wrapped, Ref):
                    wrapped = self._values[wrapped.reference]

                if isinstance(wrapped, Expression):
                    if not wrapped.is_aggregate and not wrapped.is_windowed:
                        group_by.append(self._value(wrapped, context))

        sql = []

        if not context.level and (
            recursives := [m for m in self.relations if m._recursive]
        ):
            cte_context = context.sub()
            ctes = ", ".join(
                f"{quote_ident(m._alias)} AS ({m._query.__sql__(cte_context)})"
                for m in recursives
            )
            sql.append(f"WITH RECURSIVE {ctes}")

        sql.append(f"SELECT {', '.join(cols)}")

        if main_models := sorted(
            self.relations - self._joins.keys(), key=lambda m: m._virtual, reverse=True
        ):
            sql.append(f"FROM {', '.join(m.__sql__(context) for m in main_models)}")

        for target, join_data in self._joins.items():
            strategy = join_data["strategy"].value
            on_sql = join_data["on"].__sql__(context)
            sql.append(f"{strategy} {target.__sql__(context)} ON {on_sql}")

        if self._where:
            sql.append(f"WHERE {self._where.__sql__(context)}")

        manual_group_sql = [self._value(f, context) for f in self._group_by_manual]

        final_group_list = group_by + manual_group_sql

        if final_group_list:
            group_fields_unique = list(dict.fromkeys(final_group_list))

            if self._summary:
                summary = self._summary
                summary_sql_list = [self._value(f, context) for f in summary["fields"]]

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
            sql.append(f"HAVING {self._having.__sql__(context)}")

        if self._order_by:
            sql.append(
                f"ORDER BY {', '.join(e.__sql__(context) for e in self._order_by)}"
            )

        if self._limit is not None:
            sql.append(f"LIMIT {int(self._limit)}")
        if self._offset is not None:
            sql.append(f"OFFSET {int(self._offset)}")

        if self._lock:
            lock = self._lock
            parts = [lock["mode"]]
            if lock["of"]:
                parts.append(
                    f"OF {
                        ', '.join(quote_ident(context.get_alias(m)) for m in lock['of'])
                    }"
                )
            if lock["nowait"]:
                parts.append("NOWAIT")
            elif lock["skip_locked"]:
                parts.append("SKIP LOCKED")
            sql.append(" ".join(parts))

        return " ".join(sql)
