from __future__ import annotations

from typing import TYPE_CHECKING, Any, Self

from .core import Expr, Q
from .mixins import ExecutableMixin

if TYPE_CHECKING:
    from .model import Model


class Delete(ExecutableMixin, Expr):
    __slots__ = ("model_cls", "_where", "_returning")

    def __init__(self, model_cls: type[Model]):
        super().__init__(relations={model_cls})
        self.model_cls = model_cls
        self._where: Q | None = None
        self._returning: list[Any] = []

    def filter(self, expr: Q) -> Self:
        self._where = (self._where & expr) if self._where else expr
        self.relations |= expr.relations
        return self

    def returning(self, *fields: Any) -> Self:
        self._returning.extend(fields)
        return self

    def compile(self, params: list[Any]) -> str:
        target_alias = self.model_cls._alias
        sql = [f'DELETE FROM "{self.model_cls._table}" AS "{target_alias}"']

        using_models = [
            m
            for m in self.relations
            if m != self.model_cls and not getattr(m, "_virtual", False)
        ]

        if using_models:
            using_parts = [f'"{m._table}" AS "{m._alias}"' for m in using_models]
            sql.append(f"USING {', '.join(using_parts)}")

        if self._where:
            sql.append(f"WHERE {self._where.compile(params)}")

        if self._returning:
            prefix = f'"{target_alias}".' if using_models else ""
            unique_fields = list(dict.fromkeys(self._returning))
            cols = ", ".join(f'{prefix}"{f.name}"' for f in unique_fields)
            sql.append(f"RETURNING {cols}")

        return " ".join(sql)
