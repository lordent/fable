from typing import Any

from sql.core.base import Node
from sql.core.expressions import Expression
from sql.core.types import Types
from sql.core.typings import typewith
from sql.fields.base import Field


class ValuesNodeMixin(typewith(Node)):
    def __init__(self, *args: Field, **kwargs: dict[str, Any]):
        super().__init__()

        self._values: dict[str, Any] = {}

        if args or kwargs:
            self.values(*args, **kwargs)

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


class Item(ValuesNodeMixin, Expression):
    sql_type = Types.JSONB

    def _json_build_recursive(self, fields: dict, params: list[Any]) -> str:
        tokens = [
            f"'{name}', {self._value(value, params)}" for name, value in fields.items()
        ]
        return f"JSONB_BUILD_OBJECT({', '.join(tokens)})"

    def __sql__(self, params: list[Any]):
        return self._json_build_recursive(self._values, params)


class List(Item):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.is_aggregate = True

    def __sql__(self, params: list[Any]) -> str:
        inner_json = super().__sql__(params)
        return f"COALESCE(JSONB_AGG({inner_json}), '[]'::jsonb)"
