from typing import Self

from .query import Q
from .typing import SQLExpression


class Aggregate(Q):
    function: str = ""

    def __init__(self, expression: SQLExpression, distinct: bool = False):
        self.expression = expression
        self.is_distinct = distinct
        deps = expression.dependencies if isinstance(expression, Q) else set()
        super().__init__("", _dependencies=deps)

    def distinct(self) -> Self:
        self.is_distinct = True
        return self

    def compile(self, args: list[object]) -> str:
        dist = "DISTINCT " if self.is_distinct else ""
        expr_sql = (
            self.expression.compile(args)
            if hasattr(self.expression, "compile")
            else str(self.expression)
        )
        return f"{self.function}({dist}{expr_sql})"


class Count(Aggregate):
    function = "COUNT"


class Sum(Aggregate):
    function = "SUM"


class Avg(Aggregate):
    function = "AVG"


class Max(Aggregate):
    function = "MAX"


class Min(Aggregate):
    function = "MIN"
