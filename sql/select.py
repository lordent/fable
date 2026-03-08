from __future__ import annotations

from typing import Any, Self, TypedDict, cast
from enum import StrEnum

from .core import Cast, Expr, OrderBy, Q
from .field import Field
from .mixins import ExecutableMixin
from .model import Model, QueryModel
from .query import SelectValuesQuery

class JoinStrategy(StrEnum):
    LEFT = "JOIN"
    INNER = "INNER JOIN"
    RIGHT = "RIGHT JOIN"
    FULL = "FULL OUTER JOIN"

class LockDict(TypedDict):
    mode: str
    of: set[type[Model]]
    nowait: bool
    skip_locked: bool

class SummaryDict(TypedDict):
    mode: str
    fields: list[Expr | Field]

class GroupMode(StrEnum):
    ROLLUP = "ROLLUP"
    CUBE = "CUBE"
    SETS = "GROUPING SETS"

class JoinDict(TypedDict):
    on: Q
    strategy: JoinStrategy


class Select(ExecutableMixin, SelectValuesQuery):
    __slots__ = (
        "_joins",
        "_where",
        "_having",
        "_order_by",
        "_limit",
        "_offset",
        "_lock",
        "_summary",
    )

    def __init__(self, *args: Field, **kwargs: Expr | Any):
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
        self, mode: str, of: tuple[type[Model], ...], nowait: bool, skip_locked: bool
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
        return self._set_lock("FOR UPDATE", of, nowait, skip_locked)

    def for_share(
        self, *of: type[Model], nowait: bool = False, skip_locked: bool = False
    ) -> Self:
        return self._set_lock("FOR SHARE", of, nowait, skip_locked)

    def summary(self, *fields: Field | Expr, mode: GroupMode = GroupMode.ROLLUP) -> Self:
        self._summary = {
            "mode": mode,
            "fields": list(fields)
        }
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

    def join(self, target: type[Model], on: Q | None = None, strategy: JoinStrategy = JoinStrategy.LEFT) -> Self:
        if on is None:
            for field in target._foreign_fields.values():
                if field.to in self.relations:
                    on = field == field.to.id
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

    def order_by(self, *exprs: Expr | OrderBy) -> Self:
        for e in exprs:
            self._order_by.append(e if isinstance(e, OrderBy) else e.asc())
            self.relations |= e.relations
        return self

    def group_by(self, *fields: Field | Expr) -> Self:
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

    def as_table(self, target: str | type[T] | None = None) -> type[T] | type[Model]:
        if isinstance(target, type) and issubclass(target, Model):
            return cast(type[T], type(target.__name__, (target,), {"_table": self}))
        dynamic_fields = {}
        for alias, expr in self._values.items():
            field_meta: Field | None = None
            if isinstance(expr, Cast) and expr.field_instance:
                field_meta = expr.field_instance
            elif isinstance(expr, Field):
                field_meta = expr
            source_f = field_meta if field_meta else Field()
            f_clone = object.__new__(source_f.__class__)
            all_slots = set()
            for cls in source_f.__class__.__mro__:
                slots = getattr(cls, "__slots__", [])
                if isinstance(slots, str):
                    slots = [slots]
                all_slots.update(slots)
            for slot in all_slots:
                if hasattr(source_f, slot):
                    setattr(f_clone, slot, getattr(source_f, slot))
            f_clone.name = alias
            f_clone.relations = set()
            f_clone.is_aggregate = False
            dynamic_fields[alias] = f_clone
        alias = target if isinstance(target, str) else f"sub_{id(self)}"
        return type(
            alias, (QueryModel,), {"_table": self, "_alias": alias, **dynamic_fields}
        )

    def compile(self, params: list[Any]) -> str:
        has_aggregate = any(
            getattr(v, "is_aggregate", False) for v in self._values.values()
        )
        
        cols, group_by = [], []

        for alias, expr in self._values.items():
            val_sql = self._compile_val(expr, params)
            
            if has_aggregate and hasattr(expr, "is_aggregate"):
                e = cast(Expr, expr)
                if not e.is_aggregate and not getattr(e, "_window", None):
                    group_by.append(val_sql)
            
            cols.append(f'{val_sql} AS "{alias}"')

        if has_aggregate and self._order_by:
            for ob in self._order_by:
                ob_sql = self._compile_val(ob.wrapped, params)
                if not getattr(ob.wrapped, "is_aggregate", False):
                    group_by.append(ob_sql)

        sql = [f"SELECT {', '.join(cols)}"]
        
        joined_models = set(self._joins.keys())
        main_models = [m for m in self.relations if m not in joined_models]
        
        if main_models:
            sql.append(f"FROM {', '.join(self._fmt_table(m, params) for m in main_models)}")

        for target, join_data in self._joins.items():
            strategy = join_data["strategy"].value
            on_sql = join_data["on"].compile(params)
            sql.append(
                f"{strategy} {self._fmt_table(target, params)} ON {on_sql}"
            )

        if self._where:
            sql.append(f"WHERE {self._where.compile(params)}")
        
        manual_group_sql = [self._compile_val(f, params) for f in self._group_by_manual]
        
        final_group_list = group_by + manual_group_sql

        if final_group_list:
            group_fields_unique = list(dict.fromkeys(final_group_list))
            
            if self._summary:
                summary = self._summary
                summary_sql_list = [self._compile_val(f, params) for f in summary["fields"]]
                
                regular_group = [f for f in group_fields_unique if f not in summary_sql_list]
                
                summary_clause = f"{summary['mode'].value}({', '.join(summary_sql_list)})"
                
                if regular_group:
                    sql.append(f"GROUP BY {', '.join(regular_group)}, {summary_clause}")
                else:
                    sql.append(f"GROUP BY {summary_clause}")
            else:
                sql.append(f"GROUP BY {', '.join(group_fields_unique)}")

        if self._having:
            sql.append(f"HAVING {self._having.compile(params)}")

        if self._order_by:
            sql.append(f"ORDER BY {', '.join(e.compile(params) for e in self._order_by)}")

        if self._limit is not None:
            sql.append(f"LIMIT {self._limit}")
        if self._offset is not None:
            sql.append(f"OFFSET {self._offset}")

        if self._lock:
            lock = self._lock
            parts = [lock["mode"]]
            if lock["of"]:
                parts.append(f"OF {', '.join(f'\"{m._alias}\"' for m in lock['of'])}")
            if lock["nowait"]:
                parts.append("NOWAIT")
            elif lock["skip_locked"]:
                parts.append("SKIP LOCKED")
            sql.append(" ".join(parts))

        return " ".join(sql)

    def _fmt_table(self, m: type[Model] | type[QueryModel], params: list[Any]) -> str:
        if hasattr(m._table, "compile"):
            return f'({m._table.compile(params)}) AS "{m._alias}"'
        return (
            f'"{m._table}" AS "{m._alias}"' if m._alias != m._table else f'"{m._table}"'
        )
