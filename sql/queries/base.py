from __future__ import annotations

from typing import Any, Self, TypeVar

from ..core import E, Query
from ..fields import Field
from ..model import Model, QueryModel

T = TypeVar("T", bound="Model")


class QueryBuilder(Query):
    def as_model(self, base_model: type[QueryModel] = QueryModel):
        return base_model.factory(self)


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
