from typing import Any, TypeVar

from sql.core.aggregates import AggregateExpression
from sql.core.base import Node, QueryContext
from sql.core.expressions import Expression, ScalarExpression

T = TypeVar("T", bound="CaseBase")


class CaseBase(Expression):
    def __init__(self, default: Any = None):
        super().__init__()

        self._cases: list[tuple[Node, Node]] = []
        self._else: Node | None = self._arg(default) if default is not None else None

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
            return self._value(self._else, context) if self._else else "NULL"

        parts = ["CASE"]
        for cond, res in self._cases:
            parts.append(f"WHEN {cond.__sql__(context)} THEN {res.__sql__(context)}")

        if self._else is not None:
            parts.append(f"ELSE {self._else.__sql__(context)}")

        parts.append("END")
        return f"({' '.join(parts)})"


class ScalarCase(ScalarExpression, CaseBase):
    pass


class AggregateCase(AggregateExpression, CaseBase):
    pass


class Case:
    Scalar = ScalarCase
    Aggregate = AggregateCase
