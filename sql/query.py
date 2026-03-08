from __future__ import annotations

from typing import Any, Self, TypeVar, cast

from .core import Expr
from .field import Field
from .model import Model

T = TypeVar("T", bound="Model")


class SelectValuesQuery(Expr):
    __slots__ = ("_values",)

    def __init__(self, *args: Field, **kwargs: Expr | Any):
        super().__init__()
        self._values: dict[str, Expr | Any] = {}
        if args or kwargs:
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


class List(SelectValuesQuery):
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


class Item(SelectValuesQuery):
    __slots__ = ()

    def _json_build_recursive(self, fields: dict, params: list[Any]) -> str:
        tokens = []
        for name, value in fields.items():
            tokens.append(f"'{name}'")
            tokens.append(self._compile_val(value, params))
        return f"JSONB_BUILD_OBJECT({', '.join(tokens)})"

    def compile(self, params: list[Any]) -> str:
        return self._json_build_recursive(self._values, params)
