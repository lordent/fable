from __future__ import annotations

from typing import TYPE_CHECKING, Any, Self, cast

from .core import Expr
from .mixins import ExecutableMixin

if TYPE_CHECKING:
    from .field import Field
    from .model import Model


class Insert(ExecutableMixin, Expr):
    __slots__ = ("model_cls", "_values", "_returning")

    def __init__(self, model_cls: type[Model]):
        super().__init__(relations={model_cls})
        self.model_cls = model_cls
        self._values: dict[str, Any] = {}
        self._returning: list[Field] = []

    def values(self, **kwargs: Any) -> Self:
        for key, value in kwargs.items():
            if key not in self.model_cls._fields:
                raise AttributeError(
                    f"Поле '{key}' не найдено в {self.model_cls.__name__}"
                )
            self._values[key] = value
            if hasattr(value, "relations"):
                self.relations |= cast(Expr, value).relations
        return self

    def returning(self, *fields: Field) -> Self:
        self._returning.extend(fields)
        for f in fields:
            if hasattr(f, "relations"):
                self.relations |= f.relations
        return self

    def compile(self, params: list[Any]) -> str:
        if not self._values:
            raise ValueError("INSERT требует вызова .values()")

        names = ", ".join(f'"{n}"' for n in self._values.keys())
        placeholders = ", ".join(
            self._compile_val(v, params) for v in self._values.values()
        )

        sql = [
            f'INSERT INTO "{self.model_cls._table}" ({names})',
            f"VALUES ({placeholders})",
        ]

        if self._returning:
            unique_fields = list(dict.fromkeys(self._returning))
            cols = ", ".join(f'"{f.name}"' for f in unique_fields)
            sql.append(f"RETURNING {cols}")
        else:
            sql.append('RETURNING "id"')

        return " ".join(sql)
