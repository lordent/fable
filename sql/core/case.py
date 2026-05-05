from typing import Any, TypeVar

from sql.core.aggregates import AggregateExpression
from sql.core.expressions import Expression
from sql.core.node import Node, QueryContext

T = TypeVar("T", bound="Case")


class Case(Expression):
    def __init__(self, default: Any = None):
        super().__init__()

        self._cases: list[tuple[Node, Any]] = []
        self._else: Any | None = self._arg(default) if default is not None else None

        if isinstance(self._else, Expression):
            self.sql_type = self._else.sql_type

    def when(self: T, condition: Any, then: Any) -> T:
        cond_node = self._arg(condition)
        then_node = self._arg(then)

        self._cases.append((cond_node, then_node))

        if self.sql_type is None and isinstance(then_node, Expression):
            self.sql_type = then_node.sql_type

        return self

    def __sql__(self, context: QueryContext) -> str:
        if not self._cases:
            return context.value(self._else) if self._else else "NULL"

        parts = ["CASE"]
        for cond, res in self._cases:
            parts.append(f"WHEN {cond.__sql__(context)} THEN {context.value(res)}")

        if self._else is not None:
            parts.append(f"ELSE {context.value(self._else)}")

        parts.append("END")
        return f"({' '.join(parts)})"


class AggregateCase(AggregateExpression, Case):
    pass
