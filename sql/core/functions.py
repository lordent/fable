from typing import Any

from sql.core.datatypes import Types
from sql.core.enums import DatePart
from sql.core.expressions import Expression
from sql.core.node import QueryContext
from sql.core.types import T_SqlType


class ExpressionWrapper(Expression):
    def __init__(self, expression: Expression, sql_type=None):
        super().__init__()

        self.expression = self._arg(expression)
        self.sql_type = sql_type or expression.sql_type
        self.is_aggregation = expression.is_aggregation

    def __sql__(self, context: QueryContext):
        if self.sql_type:
            return f"({self.expression.__sql__(context)})::{self.sql_type}"
        return f"({self.expression.__sql__(context)})"


class Cast(ExpressionWrapper):
    def __init__(self, expression: Expression, to: T_SqlType):
        super().__init__(
            expression=expression,
            sql_type=self._get_sql_type(to),
        )


Expression.cast = lambda self, to: Cast(self, to)


class Extract(ExpressionWrapper):
    YEAR = DatePart.YEAR
    MONTH = DatePart.MONTH
    DAY = DatePart.DAY
    HOUR = DatePart.HOUR
    MINUTE = DatePart.MINUTE
    SECOND = DatePart.SECOND
    WEEK = DatePart.WEEK
    QUARTER = DatePart.QUARTER
    EPOCH = DatePart.EPOCH
    DOW = DatePart.DOW
    DOY = DatePart.DOY

    def __init__(self, expression: Expression, part: DatePart):
        super().__init__(expression=expression, sql_type=Types.INTEGER)

        self.part = part

    def __sql__(self, context: QueryContext) -> str:
        return f"EXTRACT({self.part.value} FROM {context.value(self.expression)})"


Expression.extract = lambda self, part: Extract(self, part)


class AtTimeZone(ExpressionWrapper):
    def __init__(
        self,
        expression: Expression,
        zone: str | Expression,
    ):
        super().__init__(expression=expression)

        self.zone = self._arg(zone)

    def __sql__(self, context: QueryContext):
        return f"({super().__sql__(context)} AT TIME ZONE {context.value(self.zone)})"


Expression.at_timezone = lambda self, zone: AtTimeZone(self, zone)


class Func(Expression):
    name: str = None

    def __init__(self, *args: Any):
        super().__init__()

        self.args = args = self._list_arg(args)
        self.is_aggregation = self.is_aggregation or self._get_aggregation(*args)
        self.sql_type = self.sql_type or self._get_sql_type(*args)

    def __sql_args__(self, context: QueryContext, prefix: str = "") -> str:
        args_sql = ", ".join(context.value(a) for a in self.args)
        return f"{self.name or self.__class__.__name__.upper()}({prefix}{args_sql})"

    def __sql__(self, context: QueryContext) -> str:
        return self.__sql_args__(context)


class Coalesce(Func):
    name = "COALESCE"


Expression.default = lambda self, default: Coalesce(self, default)


class ABS(Func):
    name = "ABS"


Expression.__abs__ = lambda self: ABS(self)
