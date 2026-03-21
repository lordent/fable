from __future__ import annotations

from typing import TYPE_CHECKING, Any, Self

from sql.fields import Field
from sql.queries.base import QueryBuilder

from ..core import Q

if TYPE_CHECKING:
    from ..models import Model


class Delete(QueryBuilder):
    def __init__(self, model_cls: type[Model]):
        super().__init__()
        self.model_cls = model_cls
        self.relations = {model_cls}
        self._where: Q | None = None
        self._returning: list[Any] = []

    def filter(self, expr: Q) -> Self:
        self._where = (self._where & expr) if self._where else expr
        self.relations |= expr.relations
        return self

    def returning(self, *fields: Any) -> Self:
        self._returning.extend(fields)
        return self

    def __sql__(self, context: QueryContext) -> str:
        target_alias = self.model_cls._alias
        sql = [f"DELETE FROM {self.model_cls.__sql__(context)}"]

        using_models = [
            m for m in self.relations if m != self.model_cls and not m._virtual
        ]

        if using_models:
            using_parts = [m.__sql__(context) for m in using_models]
            sql.append(f"USING {', '.join(using_parts)}")

        if self._where:
            sql.append(f"WHERE {self._where.__sql__(context)}")

        if self._returning:
            prefix = f'"{target_alias}".' if using_models else ""
            unique_fields: list[Field] = list(dict.fromkeys(self._returning))
            cols = ", ".join(f'{prefix}"{f.name}"' for f in unique_fields)
            sql.append(f"RETURNING {cols}")

        return " ".join(sql)
