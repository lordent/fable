from string.templatelib import Template
from typing import Any

from sql.core.aggregates import AggregateExpression
from sql.core.expressions import Expression
from sql.core.helpers import from_python
from sql.core.node import Node, QueryContext
from sql.core.types import T_SqlType
from sql.utils import extract_template, quote_ident


class Ref(Expression):
    def __init__(self, reference: str):
        super().__init__()

        self.reference = reference

    def __sql__(self, context: QueryContext):
        return quote_ident(self.reference)


class Value:
    __slots__ = ("value", "sql_type")

    def __init__(self, value: Any, sql_type: T_SqlType = None):
        self.value = value
        self.sql_type = sql_type or from_python(value)


class Raw(Expression):
    Aggregate: type[AggregateRaw]

    def __init__(self, value: Any):
        super().__init__()

        if isinstance(value, Template):
            self.args = [
                self._arg(a) for a in extract_template(value, validator=self.escape)
            ]
        elif not isinstance(value, Node):
            value: Value = value if isinstance(value, Value) else Value(value)
            self.sql_type = value.sql_type
            self.args = [value]

    def escape(self, v: Any):
        return v if isinstance(v, Node | Value) else Value(v)

    def __sql_argument__(self, argument: Any, context: QueryContext):
        return str(argument)

    def __sql__(self, context: QueryContext) -> str:
        parts = []
        for a in self.args:
            if isinstance(a, Value):
                parts.append(f"{context.value(a.value)}::{a.sql_type}")
            elif isinstance(a, Node):
                parts.append(context.value(a))
            else:
                parts.append(self.__sql_argument__(a, context))

        body = "".join(parts)
        return f"({body})::{self.sql_type}" if self.sql_type else f"({body})"


class AggregateRaw(AggregateExpression, Raw):
    pass


Raw.Aggregate = AggregateRaw
