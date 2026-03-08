from __future__ import annotations

from typing import Any, Self, TypeVar, cast

from .core import Cast, Expr, OrderBy, Q
from .field import Field
from .model import Model, QueryModel

T = TypeVar("T", bound="Model")


class SelectValuesMixin(Expr):
    __slots__ = ("_values",)

    def __init__(self, *args: Field, **kwargs: Expr | Any):
        super().__init__()

        self._values: dict[str, Expr | Any] = {}
        self.values(*args, **kwargs)

    def values(self, *args: Field, **kwargs: Expr | Any) -> Self:
        for f in args:
            if not isinstance(f, Field):
                raise ValueError(
                    f"Аргумент {f} должен быть Field. Для выражений используйте kwargs."
                )
            self._values[f.name] = f
            self.relations |= f.relations

        for alias, v in kwargs.items():
            self._values[alias] = v
            if hasattr(v, "relations"):
                self.relations |= cast(Expr, v).relations

        return self


class List(SelectValuesMixin):
    """
    Агрегация строк в JSONB массив объектов.
    SQL: COALESCE(JSONB_AGG(JSONB_BUILD_OBJECT(...)), '[]')
    """

    __slots__ = ()

    def __init__(self, *args: Field, **kwargs: Expr | Any):
        super().__init__(*args, **kwargs)
        self.is_aggregate = True

    def _json_build_recursive(self, fields: dict, params: list[Any]) -> str:
        tokens = []
        for name, value in fields.items():
            tokens.append(f"'{name}'")
            tokens.append(self._compile_val(value, params))
        return f"JSONB_BUILD_OBJECT({', '.join(tokens)})"

    def compile(self, params: list[Any]) -> str:
        inner_json = self._json_build_recursive(self._values, params)
        return f"COALESCE(JSONB_AGG({inner_json}), '[]'::jsonb)"


class Select(SelectValuesMixin):
    __slots__ = ("_joins", "_where", "_having", "_order_by", "_limit", "_offset")

    def __init__(self, *args: Field, **kwargs: Expr | Any):
        self._joins: dict[type[Model], Q] = {}
        self._where: Q | None = None
        self._having: Q | None = None
        self._order_by: list[OrderBy] = []
        self._limit: int | None = None
        self._offset: int | None = None

        super().__init__(*args, **kwargs)

    def filter(self, expr: Q) -> Self:
        """Авто-распределение: агрегаты в HAVING, остальное в WHERE."""
        if expr.is_aggregate:
            self._having = (self._having & expr) if self._having else expr
        else:
            self._where = (self._where & expr) if self._where else expr
        self.relations |= expr.relations
        return self

    def join(self, target: type[Model], on: Q | None = None) -> Self:
        """Smart Join: поиск связи через ForeignKey."""
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

        self._joins[target] = on
        self.relations |= {target} | on.relations
        return self

    def order_by(self, *exprs: Expr | OrderBy) -> Self:
        for e in exprs:
            self._order_by.append(e if isinstance(e, OrderBy) else e.asc())
            self.relations |= e.relations
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

        cols = []
        group_by = []

        for alias, expr in self._values.items():
            val_sql = self._compile_val(expr, params)

            if has_aggregate and hasattr(expr, "is_aggregate"):
                e = cast(Expr, expr)
                if not e.is_aggregate and not getattr(e, "_window", None):
                    group_by.append(val_sql)

            cols.append(f'{val_sql} AS "{alias}"')

        joined_models = set(self._joins.keys())
        main_models = [m for m in self.relations if m not in joined_models]

        sql = [f"SELECT {', '.join(cols)}"]
        if main_models:
            sql.append(
                f"FROM {', '.join(self._fmt_table(m, params) for m in main_models)}"
            )

        for target, on in self._joins.items():
            sql.append(
                f"JOIN {self._fmt_table(target, params)} ON {on.compile(params)}"
            )

        if self._where:
            sql.append(f"WHERE {self._where.compile(params)}")

        if group_by:
            sql.append(f"GROUP BY {', '.join(dict.fromkeys(group_by))}")

        if self._having:
            sql.append(f"HAVING {self._having.compile(params)}")

        if self._order_by:
            sql.append(
                f"ORDER BY {', '.join(e.compile(params) for e in self._order_by)}"
            )

        if self._limit is not None:
            sql.append(f"LIMIT {self._limit}")
        if self._offset is not None:
            sql.append(f"OFFSET {self._offset}")

        return " ".join(sql)

    def _fmt_table(self, m: type[Model] | type[QueryModel], params: list[Any]) -> str:
        if hasattr(m._table, "compile"):
            return f'({m._table.compile(params)}) AS "{m._alias}"'
        return (
            f'"{m._table}" AS "{m._alias}"' if m._alias != m._table else f'"{m._table}"'
        )
