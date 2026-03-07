from typing import Any, Self, cast

from .core import Expr, Q
from .field import Field
from .mixins import SelectValuesMixin
from .model import Model


class List(SelectValuesMixin, Expr):
    def _json_build_recursive(self, fields: dict, params: list) -> str:
        tokens = []
        for name, value in fields.items():
            tokens.append(f"'{name}'")
            if isinstance(value, Expr):
                tokens.append(value.compile(params))
            elif isinstance(value, dict):
                tokens.append(self._json_build_recursive(value, params))
            else:
                params.append(value)
                tokens.append(f"${len(params)}")
        return f"JSONB_BUILD_OBJECT({', '.join(tokens)})"

    def compile(self, params: list) -> str:
        sql = self._json_build_recursive(self._values, params)
        return f"COALESCE(JSONB_AGG({sql}), '[]'::jsonb)"


class Select(SelectValuesMixin, Expr):
    def __init__(self, *args: Field, **kwargs: Expr):
        super().__init__()
        self._joins: dict[type[Model], Expr] = {}
        self._filters: Q | None = None
        self._group_by = []
        self.values(*args, **kwargs)

    def filter(self, expr: Q) -> Self:
        self._filters = (self._filters & expr) if self._filters else expr
        self.relations |= expr.relations
        return self

    def join(self, target: type[Model], on: Expr | None = None) -> Self:
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
            raise Exception(
                f"No relation link found between {target._alias} and existing tables"
            )

        self._joins[target] = on
        self.relations |= {target} | on.relations
        return self

    def compile(self, params: list) -> str:
        has_agg = any(isinstance(v, List) for v in self._values.values())

        cols = []
        group_by = []

        for name, expr in self._values.items():
            is_expr = isinstance(expr, Expr)
            val_sql = expr.compile(params) if is_expr else str(expr)
            if has_agg and isinstance(expr, Field):
                group_by.append(val_sql)

            if isinstance(expr, Field) and expr.name == name:
                cols.append(val_sql)
            else:
                cols.append(f"{val_sql} AS {name}")

        from_rels = self.relations - set(self._joins.keys())

        def fmt(m: type[Model]):
            t = m._table
            if isinstance(t, Select):
                return f'({t.compile(params)}) AS "{m._alias}"'
            return f'"{t}" AS "{m._alias}"' if m._alias != t else f'"{t}"'

        sql = [f"SELECT {', '.join(cols)}"]

        if from_rels:
            sql.append(f"FROM {', '.join(fmt(m) for m in from_rels)}")

        for target, on in self._joins.items():
            sql.append(f"JOIN {fmt(target)} ON {on.compile(params)}")

        if self._filters:
            sql.append(f"WHERE {self._filters.compile(params)}")

        if group_by:
            sql.append(f"GROUP BY {', '.join(dict.fromkeys(group_by))}")

        return " ".join(sql)

    def as_table(self, alias: str = None) -> type[Model]:
        alias = alias or f"_t{id(self)}"
        fields = {name: Field() for name in self._values.keys()}
        sub_cls = type(
            alias,
            (Model,),
            {
                "_table": self,
                "_alias": alias,
                **fields,
            },
        )
        return cast(type[Model], sub_cls)

    def prepare(self) -> tuple[str, list[Any]]:
        params = []
        return self.compile(params), params

    def __repr__(self) -> str:
        return self.compile([])
