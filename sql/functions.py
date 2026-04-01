from string.templatelib import Template
from typing import Any

from sql.core.aggregates import AggregateFunc, UnaryAggregate
from sql.core.base import QueryContext
from sql.core.converters import register_converter
from sql.core.enums import DatePart
from sql.core.expressions import Expression
from sql.core.raw import Raw
from sql.core.scalars import ScalarExpression, ScalarFunc
from sql.core.types import Types
from sql.utils import extract_template


@register_converter(Template)
class Concat(Expression):
    def __init__(self, value: Any, *args: Any):
        super().__init__(sql_type=Types.TEXT)

        if isinstance(value, Template):
            self.args = [self._arg(a) for a in extract_template(value)]
        else:
            self.args = [self._arg(a) for a in (value, *args)]

    def __sql__(self, context: QueryContext) -> str:
        return f"({' || '.join(self._value(a, context) for a in self.args)})"


class Sum(UnaryAggregate):
    args: list[Expression]

    def __init__(self, expression: Expression, distinct: bool = False):
        super().__init__(expression, distinct=distinct)

        self.sql_type = self.args[0].sql_type


class Count(UnaryAggregate):
    def __init__(self, expression: Any = None, distinct: bool = False):
        super().__init__(
            expression or Raw(t"*"), distinct=distinct, sql_type=Types.BIGINT
        )


class Avg(UnaryAggregate):
    sql_type = Types.NUMERIC


class Min(UnaryAggregate):
    pass


class Max(UnaryAggregate):
    pass


class Every(UnaryAggregate):
    sql_type = Types.BOOLEAN


class RowNumber(AggregateFunc):
    name = "ROW_NUMBER"

    def __init__(self):
        super().__init__(Raw(t"*"), sql_type=Types.BIGINT)

    def __sql_args__(self, context, prefix=""):
        return f"{self.name}()"


class Lag(AggregateFunc):
    def __init__(self, expression: Any, offset: int = 1, default: Any = None):
        super().__init__(expression, offset, default)

        if self.args and (node := self.args[0]) and isinstance(node, Expression):
            self.sql_type = node.sql_type


class Rank(AggregateFunc):
    pass


class DenseRank(AggregateFunc):
    name = "DENSE_RANK"


class Extract(ScalarExpression):
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

    def __init__(self, source: Expression, part: DatePart):
        super().__init__(sql_type=Types.INTEGER)

        self.source = self._arg(source)
        self.part = part

    def __sql__(self, context: QueryContext) -> str:
        return f"EXTRACT({self.part.value} FROM {self._value(self.source, context)})"


class Round(ScalarFunc):
    def __init__(self, expression: Any, precision: int = 0):
        super().__init__(expression, precision)


class Lower(ScalarFunc):
    sql_type = Types.TEXT


class Now(ScalarFunc):
    sql_type = Types.TIMESTAMPTZ

    def __init__(self, precision: int = None):
        args = [precision] if precision is not None else []

        super().__init__(*args)


class Age(ScalarFunc):
    sql_type = Types.INTERVAL

    def __init__(self, source: Expression, relative_to: Expression = None):
        if relative_to is None:
            super().__init__(source)
        else:
            super().__init__(source, relative_to)
