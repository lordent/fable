from datetime import date, datetime, timedelta
from decimal import Decimal
from string.templatelib import Template
from typing import Any
from uuid import UUID

from sql.core.aggregates import AggregateExpression
from sql.core.base import Node, QueryContext
from sql.core.expressions import Expression, ScalarExpression
from sql.core.types import Types
from sql.utils import extract_template, quote_ident


class _Ref(Expression):
    def __init__(self, reference: str):
        super().__init__()

        self.reference = reference

    def __sql__(self, context: QueryContext):
        return quote_ident(self.reference)


class Ref:
    class Scalar(ScalarExpression, _Ref):
        pass

    class Aggregate(AggregateExpression, _Ref):
        pass


class _Raw(Expression):
    # TODO: проработать шаблоны с аргументами

    def __init__(self, value: Any):
        super().__init__()

        if isinstance(value, Template):
            self.args = [self._arg(a) for a in extract_template(value)]
        else:
            self.args = [self.from_python(value)]

    def from_python(self, value: Any):
        if isinstance(value, int):
            self.sql_type = Types.BIGINT
        elif isinstance(value, (float, Decimal)):
            self.sql_type = Types.NUMERIC
        elif isinstance(value, (datetime, date)):
            self.sql_type = Types.TIMESTAMPTZ
        elif isinstance(value, timedelta):
            self.sql_type = Types.INTERVAL
        elif isinstance(value, UUID):
            self.sql_type = Types.UUID
        elif isinstance(value, (list, dict)):
            self.sql_type = Types.JSONB
        else:
            raise TypeError(f"Тип {type(value)} не поддерживается в Raw")
        return value

    def __sql__(self, context: QueryContext) -> str:
        if type_ := self.sql_type or "":
            type_ = f"::{type_}"
        return f"({
            ''.join(
                self._value(a, context) if isinstance(a, Node) else str(a)
                for a in self.args
            )
        }){type_}"


class Raw:
    class Scalar(ScalarExpression, _Raw):
        pass

    class Aggregate(AggregateExpression, _Raw):
        pass
