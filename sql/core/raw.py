from datetime import date, datetime, timedelta
from decimal import Decimal
from string.templatelib import Template
from typing import Any
from uuid import UUID

from sql.core.base import Node, QueryContext
from sql.core.expressions import AggregateExpression, Expression, ScalarExpression
from sql.core.types import Types
from sql.utils import extract_template, quote_ident


class Ref(Expression):
    Scalar: type[ScalarRef]
    Aggregate: type[AggregateRef]

    def __init__(self, reference: str):
        super().__init__()

        self.reference = reference

    def __sql__(self, context: QueryContext):
        return quote_ident(self.reference)


class ScalarRef(ScalarExpression, Ref):
    pass


class AggregateRef(AggregateExpression, Ref):
    pass


Ref.Scalar = ScalarRef
Ref.Aggregate = AggregateRef

ACCEPT_TYPES = (
    Node,
    int,
    float,
    Decimal,
    datetime,
    date,
    timedelta,
    UUID,
    list,
    dict,
)


class Raw(Expression):
    Scalar: type[ScalarRaw]
    Aggregate: type[AggregateRaw]

    def __init__(self, value: Any):
        super().__init__()

        if isinstance(value, Template):
            self.args = [
                self._arg(a) for a in extract_template(value, validator=self.validator)
            ]
        else:
            self.args = [self.from_python(value)]

    def validator(self, value: Any):
        if isinstance(value, ACCEPT_TYPES):
            return value
        raise TypeError(f"Тип {type(value)} не поддерживается в Raw")

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


class ScalarRaw(ScalarExpression, Raw):
    pass


class AggregateRaw(AggregateExpression, Raw):
    pass


Raw.Scalar = ScalarRaw
Raw.Aggregate = AggregateRaw
