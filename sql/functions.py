from string.templatelib import Template
from typing import Any

from sql.core.aggregates import AggregateFunc, UnaryAggregate
from sql.core.converters import register_converter
from sql.core.datatypes import Types
from sql.core.expressions import Expression
from sql.core.functions import Func
from sql.core.node import QueryContext
from sql.core.raw import Raw
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
        return f"({' || '.join(context.value(a) for a in self.args)})"


class Sum(UnaryAggregate):
    sql_type = Types.NUMERIC


class Count(UnaryAggregate):
    sql_type = Types.BIGINT

    def __init__(self, expression: Any = None, distinct: bool = False):
        super().__init__(expression or Raw(t"*"), distinct=distinct)


class Avg(UnaryAggregate):
    sql_type = Types.NUMERIC


class Min(UnaryAggregate):
    sql_type = Types.NUMERIC


class Max(UnaryAggregate):
    sql_type = Types.NUMERIC


class Every(UnaryAggregate):
    sql_type = Types.BOOLEAN


class RowNumber(AggregateFunc):
    name = "ROW_NUMBER"
    sql_type = Types.BIGINT

    def __init__(self):
        super().__init__(Raw(t"*"))

    def __sql_args__(self, context, prefix=""):
        return f"{self.name}()"


class Lag(AggregateFunc):
    def __init__(self, expression: Any, offset: int = 1, default: Any = None):
        super().__init__(expression, offset, default)


class Rank(AggregateFunc):
    pass


class DenseRank(AggregateFunc):
    name = "DENSE_RANK"


class Round(Func):
    sql_type = Types.NUMERIC

    def __init__(self, expression: Any, precision: int = 0):
        super().__init__(expression, precision)


Expression.round = lambda self, precision=0: Round(self, precision)


class Lower(Func):
    sql_type = Types.TEXT


class Now(Func):
    sql_type = Types.TIMESTAMPTZ

    def __init__(self, precision: int = None):
        args = [precision] if precision is not None else []

        super().__init__(*args)


class Age(Func):
    sql_type = Types.INTERVAL

    def __init__(self, source: Expression, relative_to: Expression = None):
        if relative_to is None:
            super().__init__(source)
        else:
            super().__init__(source, relative_to)
