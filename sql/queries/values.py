from typing import Any

from sql.core.aggregates import AggregateExpression
from sql.core.base import Node, QueryContext
from sql.core.datatypes import Types
from sql.core.scalars import ScalarExpression
from sql.fields.base import Field
from sql.typings import typewith
from sql.utils import quote_literal


class ValuesNodeMixin(typewith(Node)):
    def __init__(self, *args: Field, **kwargs: dict[str, Any]):
        super().__init__()

        self._values: dict[str, Any] = {}
        self._has_aggregate = False

        if args or kwargs:
            self.values(*args, **kwargs)

    def _arg(self, value: Any):
        value = super()._arg(value)
        if isinstance(value, AggregateExpression):
            self._has_aggregate = True
        return value

    def values(self, *args: Field, **kwargs: Any):
        for f in args:
            if not isinstance(f, Field):
                raise ValueError(
                    f"Аргумент {f} должен быть Field. Для выражений используйте kwargs."
                )
            self._values[f.name] = self._arg(f)

        for alias, v in kwargs.items():
            self._values[alias] = self._arg(v)
        return self


class Item(ValuesNodeMixin, ScalarExpression):
    sql_type = Types.JSONB

    def _json_build_recursive(self, fields: dict, context: QueryContext):
        tokens = [
            f"{quote_literal(name)}, {self._value(value, context)}"
            for name, value in fields.items()
        ]
        return f"JSONB_BUILD_OBJECT({', '.join(tokens)})"

    def __sql__(self, context: QueryContext):
        return self._json_build_recursive(self._values, context)


class List(AggregateExpression, Item):
    def __sql__(self, context: QueryContext):
        return f"COALESCE(JSONB_AGG({super().__sql__(context)}), '[]'::jsonb)"
