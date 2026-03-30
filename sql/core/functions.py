from typing import Any

from sql.core.aggregates import AggregateExpression
from sql.core.base import QueryContext
from sql.core.expressions import Expression, ScalarExpression
from sql.core.types import SqlType
from sql.typings import typewith


class FuncMixin(typewith(Expression)):
    name: str = None

    def __init_subclass__(cls):
        if not cls.name:
            cls.name = cls.__name__.upper()
        return super().__init_subclass__()

    def __init__(self, *args: Any, sql_type: SqlType = None):
        super().__init__(sql_type=sql_type)

        self.args = self._list_arg(args)

    def _render_args(self, context: QueryContext, prefix: str = "") -> str:
        args_sql = ", ".join(
            "*" if a == "*" else self._value(a, context) for a in self.args
        )
        return f"{self.name}({prefix}{args_sql})"


class ScalarFunc(FuncMixin, ScalarExpression):
    def __sql__(self, context: QueryContext) -> str:
        return self._render_args(context)


class AggregateFunc(FuncMixin, AggregateExpression):
    def __init__(self, *args: Any, sql_type=None, distinct: bool = False):
        super().__init__(*args, sql_type=sql_type)

        self.distinct = distinct

    def __sql__(self, context: QueryContext) -> str:
        prefix = "DISTINCT " if self.distinct else ""
        return self._render_args(context, prefix)


class UnaryAggregate(AggregateFunc):
    def __init__(
        self, expression: Expression, distinct: bool = False, sql_type: SqlType = None
    ):
        super().__init__(expression, distinct=distinct, sql_type=sql_type)
