from typing import Any, Self

from sql.core.expressions import Expression, Func, Q

from sql.core.base import QueryContext
from sql.core.types import SqlType, Types


class Aggregate(Func):
    def __init__(
        self, arg: Expression | str, distinct: bool = False, sql_type: SqlType = None
    ):
        super().__init__(
            self._get_name(),
            arg,
            sql_type=sql_type,
            is_aggregate=True,
        )

        self.distinct = distinct
        self._filter: Q | None = None

    def _get_name(self):
        return self.__class__.__name__.upper()

    def _render_args(self, context: QueryContext) -> str:
        prefix = "DISTINCT " if self.distinct else ""
        arg = self.args[0]
        if isinstance(arg, Expression):
            arg_sql = self._value(arg, context)
        else:
            arg_sql = arg
        return f"{prefix}{arg_sql}"

    def filter(self, condition: Q) -> Self:
        self._filter = self._arg(condition)
        return self

    def _render_filter(self, context: QueryContext) -> str:
        if not self._filter:
            return ""
        return f" FILTER (WHERE {self._filter.__sql__(context)})"


class Count(Aggregate):
    def __init__(self, arg: Any = "*", distinct: bool = False):
        super().__init__(arg, distinct=distinct, sql_type=Types.BIGINT)


class Sum(Aggregate):
    def __init__(self, arg: Expression, distinct: bool = False):
        super().__init__(arg, distinct=distinct, sql_type=arg.sql_type)


class Avg(Aggregate):
    def __init__(self, arg: Expression, distinct: bool = False):
        super().__init__(arg, distinct=distinct, sql_type=Types.NUMERIC)


class Min(Aggregate):
    def __init__(self, arg: Expression):
        super().__init__(arg, sql_type=arg.sql_type)


class Max(Aggregate):
    def __init__(self, arg: Expression):
        super().__init__(arg, sql_type=arg.sql_type)


class ArrayAgg(Aggregate):
    def __init__(self, arg: Expression, distinct: bool = False):
        base_type = arg.sql_type or Types.TEXT
        super().__init__(arg, distinct=distinct, sql_type=base_type[:])
