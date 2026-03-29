from typing import Any, Self

from sql.core.base import QueryContext
from sql.core.expressions import Expression, Q


class Case(Expression):
    def __init__(self, default: Any = None):
        super().__init__()
        self._cases: list[tuple[Q, Any]] = []
        self._else = self._arg(default)

    def when(self, condition: Q, then: Any) -> Self:
        cond = self._arg(condition)
        res = self._arg(then)
        self._cases.append((cond, res))
        return self

    def __sql__(self, context: QueryContext) -> str:
        parts = ["CASE"]
        for cond, res in self._cases:
            parts.append(
                f"WHEN {cond.__sql__(context)} THEN {self._value(res, context)}"
            )

        if self._else is not None:
            parts.append(f"ELSE {self._value(self._else, context)}")

        parts.append("END")
        return f"({' '.join(parts)})"
