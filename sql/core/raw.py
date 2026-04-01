from datetime import date, datetime
from decimal import Decimal
from string.templatelib import Template
from typing import Any
from uuid import UUID

from sql.core.aggregates import AggregateExpression
from sql.core.base import Node, QueryContext
from sql.core.expressions import Expression
from sql.core.scalars import ScalarExpression
from sql.core.types import AggregateType, ScalarType, Types
from sql.utils import extract_template, quote_ident, quote_literal


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


ScalarType.Ref = Ref.Scalar = ScalarRef
AggregateType.Ref = Ref.Aggregate = AggregateRef


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
            self.args = [self.validator(value)]

    def validator(self, value: Any):
        if isinstance(value, Node):
            return value

        if isinstance(value, datetime | date):
            value = f"{quote_literal(value.isoformat())}::{Types.TIMESTAMPTZ}"
        elif isinstance(value, UUID):
            value = f"{quote_literal(value.hex)}::{Types.UUID}"
        elif not isinstance(value, int | float | Decimal):
            raise TypeError(f"Тип {type(value)} не поддерживается в Raw")

        return value

    def __sql__(self, context: QueryContext) -> str:
        return f"({
            ''.join(
                self._value(a, context) if isinstance(a, Node) else str(a)
                for a in self.args
            )
        })"


class ScalarRaw(ScalarExpression, Raw):
    pass


class AggregateRaw(AggregateExpression, Raw):
    pass


ScalarType.Raw = Raw.Scalar = ScalarRaw
AggregateType.Raw = Raw.Aggregate = AggregateRaw
