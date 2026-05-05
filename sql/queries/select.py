from typing import Any, Self, TypedDict

from sql.core.aggregates import WindowExpression
from sql.core.enums import GroupMode, JoinStrategy, LockMode
from sql.core.expressions import Expression
from sql.core.node import QueryContext
from sql.core.order import OrderBy
from sql.core.query import Q
from sql.core.raw import Ref
from sql.core.types import T_Model
from sql.fields.base import Field
from sql.models import RecursiveModel, TableModel
from sql.queries.base import RecursiveContext, ValuesQuery
from sql.queries.values import Item, List
from sql.utils import quote_ident


class LockDict(TypedDict):
    mode: LockMode
    of: set[T_Model]
    nowait: bool
    skip_locked: bool


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
        self._joins: dict[T_Model, JoinDict] = {}
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
        of: tuple[T_Model, ...],
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
        self, *of: T_Model, nowait: bool = False, skip_locked: bool = False
    ) -> Self:
        return self._set_lock(LockMode.UPDATE, of, nowait, skip_locked)

    def for_share(
        self, *of: T_Model, nowait: bool = False, skip_locked: bool = False
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
            if a.is_aggregation:
                self._having = (self._having & a) if self._having else a
            else:
                self._where = (self._where & a) if self._where else a
        return self

    def _auto_join(self, target: T_Model):
        for field in target._foreign_fields.values():
            if (to := field.to) in self.relations and to != target:
                if field.name == to.id.name and to == to.id.model:
                    for to_field in to._foreign_fields.values():
                        if to_field.model == to:
                            return field == to_field
                return field == to.id

        if issubclass(target, TableModel):
            for rel in self.relations:
                if rel == target:
                    continue
                for field in rel._foreign_fields.values():
                    if field.to == target:
                        return field == target.id

    def join(
        self,
        target: T_Model,
        on: Q | None = None,
        strategy: JoinStrategy = JoinStrategy.INNER,
    ) -> Self:
        on = on or self._auto_join(target)
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
        cols, group_by = [], []

        for alias, value in self._values.items():
            sql_ = context.value(value)
            if self._has_aggregate and isinstance(value, Expression):
                if not value.is_aggregation and not isinstance(value, WindowExpression):
                    group_by.append(sql_)

            cols.append(f"{sql_} AS {quote_ident(alias)}")

        if self._has_aggregate and self._order_by:
            for a in self._order_by:
                node = a.node

                if isinstance(node, Ref):
                    node = self._values[node.reference]

                if isinstance(node, Expression):
                    if not node.is_aggregation and not isinstance(
                        node, WindowExpression
                    ):
                        group_by.append(context.value(node))

        sql = []

        if not context.level and (
            recursives := [m for m in self.relations if isinstance(m, RecursiveModel)]
        ):
            cte_context = context.sub()
            sql_ = ", ".join(m.__sql__(cte_context) for m in recursives)
            sql.append(f"WITH RECURSIVE {sql_}")

        sql.append(f"SELECT {', '.join(cols)}")

        if main_models := sorted(
            self.relations - self._joins.keys(), key=lambda m: m._virtual, reverse=True
        ):
            sql.append(
                f"FROM {
                    ', '.join(
                        (
                            m.__sql_alias__(context)
                            if isinstance(m, RecursiveModel)
                            else m.__sql__(context)
                        )
                        for m in main_models
                    )
                }"
            )

        for m, join_data in self._joins.items():
            strategy = join_data["strategy"].value
            sql_ = join_data["on"].__sql__(context)
            target_sql = (
                m.__sql_alias__(context)
                if isinstance(m, RecursiveModel)
                else m.__sql__(context)
            )
            sql.append(f"{strategy} {target_sql} ON {sql_}")

        if self._where:
            sql.append(f"WHERE {self._where.__sql__(context)}")

        manual_group_sql = [context.value(f) for f in self._group_by_manual]

        final_group_list = group_by + manual_group_sql

        if final_group_list:
            group_fields_unique = list(dict.fromkeys(final_group_list))

            if self._summary:
                summary = self._summary
                summary_sql_list = [context.value(f) for f in summary["fields"]]

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
