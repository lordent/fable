from __future__ import annotations

from typing import TYPE_CHECKING, Any, Self, cast

from .core import Expr, Q
from .field import Field
from .mixins import ExecutableMixin

if TYPE_CHECKING:
    from .model import Model


class Update(ExecutableMixin, Expr):
    __slots__ = ("model_cls", "_values", "_where", "_returning")

    def __init__(self, model_cls: type[Model]):
        super().__init__(relations={model_cls})
        self.model_cls = model_cls
        self._values: dict[str, Any] = {}
        self._where: Q | None = None
        self._returning: list[Field] = []

    def set(self, **kwargs: Any) -> Self:
        for key, value in kwargs.items():
            if key not in self.model_cls._fields:
                raise AttributeError(
                    f"Поле '{key}' не найдено в {self.model_cls.__name__}"
                )

            self._values[key] = value
            if hasattr(value, "relations"):
                self.relations |= cast(Expr, value).relations
        return self

    def filter(self, expr: Q) -> Self:
        self._where = (self._where & expr) if self._where else expr
        self.relations |= expr.relations
        return self

    def returning(self, *fields: Field) -> Self:
        self._returning.extend(fields)
        for f in fields:
            if hasattr(f, "relations"):
                self.relations |= f.relations
        return self

    def compile(self, params: list[Any]) -> str:
        if not self._values:
            raise ValueError("Нечего обновлять. Используйте .set()")

        set_parts = []
        for name, value in self._values.items():
            val_sql = self._compile_val(value, params)
            set_parts.append(f'"{name}" = {val_sql}')

        target_alias = self.model_cls._alias
        sql = [f'UPDATE "{self.model_cls._table}" AS "{target_alias}"']
        sql.append(f"SET {', '.join(set_parts)}")

        from_models = [
            m
            for m in self.relations
            if m != self.model_cls and not getattr(m, "_virtual", False)
        ]
        if from_models:
            from_parts = [f'"{m._table}" AS "{m._alias}"' for m in from_models]
            sql.append(f"FROM {', '.join(from_parts)}")

        if self._where:
            sql.append(f"WHERE {self._where.compile(params)}")

        prefix = f'"{target_alias}".' if from_models else ""

        if self._returning:
            cols = ", ".join(f'{prefix}"{f.name}"' for f in self._returning)
            sql.append(f"RETURNING {cols}")
        else:
            sql.append(f'RETURNING {prefix}"id"')

        return " ".join(sql)
