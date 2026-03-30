from typing import TYPE_CHECKING, Any, overload

from sql.core.base import Node, OrderBy
from sql.core.types import SqlType

if TYPE_CHECKING:
    from sql.core.aggregates import (
        AggregateAtTimeZone,
        AggregateCast,
        AggregateCoalesce,
        AggregateQ,
    )
    from sql.fields.base import Field


class Expression(Node):
    Q = type["Q"]
    Coalesce = type["Coalesce"]
    Cast = type["Cast"]
    AtTimeZone = type["AtTimeZone"]

    sql_type: SqlType = None

    def __init__(self, sql_type: str = None):
        super().__init__()

        self.sql_type = sql_type or self.sql_type

    def _q(self, left: Any, op: str, right: Any, sql_type: SqlType):
        return next(
            (a.Q for a in (left, right) if isinstance(a, AggregateType)),
            Q,
        )(left, op, right, sql_type=sql_type)

    # -- Преобразование типов ---

    @overload
    def cast(self: AggregateType, to: SqlType | Field) -> AggregateCast: ...

    @overload
    def cast(self: ScalarExpression, to: SqlType | Field) -> Cast: ...

    def cast(self, to: SqlType | Field) -> Cast | AggregateCast:
        return (self.Cast if isinstance(self, AggregateType) else Cast)(self, to)

    @overload
    def __rshift__(self: AggregateType, to: SqlType | Field) -> AggregateCast: ...

    @overload
    def __rshift__(self: ScalarExpression, to: SqlType | Field) -> Cast: ...

    def __rshift__(self, to: SqlType | Field) -> Cast | AggregateCast:
        return self.cast(to)

    # -- Таймзона ---

    @overload
    def at_timezone(
        self: AggregateType, zone: str | Expression
    ) -> AggregateAtTimeZone: ...

    @overload
    def at_timezone(self: ScalarExpression, zone: str | Expression) -> AtTimeZone: ...

    def at_timezone(self, zone: str | Expression) -> AtTimeZone | AggregateAtTimeZone:
        return (self.AtTimeZone if isinstance(self, AggregateType) else AtTimeZone)(
            self, zone
        )

    # -- Coalesce ---

    @overload
    def default(self: AggregateType, other: Any) -> AggregateCoalesce: ...

    @overload
    def default(self: ScalarExpression, other: AggregateType) -> AggregateCoalesce: ...

    @overload
    def default(self: ScalarExpression, other: Any) -> Coalesce: ...

    def default(self, other: Any) -> Coalesce | AggregateCoalesce:
        return (
            self.Coalesce
            if isinstance(self, AggregateType) or isinstance(other, AggregateType)
            else Coalesce
        )(self, other, sql_type=self.sql_type)

    # --- Сортировка ---

    def asc(self, nulls_first=None) -> OrderBy:
        return OrderBy(self, OrderBy.Direction.ASC, nulls_first)

    def desc(self, nulls_first=None) -> OrderBy:
        return OrderBy(self, OrderBy.Direction.DESC, nulls_first)

    # --- Арифметика ---

    @overload
    def __add__(self: AggregateType, other: Any) -> AggregateQ: ...

    @overload
    def __add__(self: Any, other: AggregateType) -> AggregateQ: ...

    @overload
    def __add__(self: ScalarExpression, other: ScalarExpression) -> Q: ...

    @overload
    def __add__(self: ScalarExpression, other: Any) -> Q: ...

    def __add__(self, other: Any) -> AggregateQ | Q:
        return self._q(self, "+", other, sql_type=self.sql_type)

    # __radd__, __rmul__


class AggregateType(Expression):
    pass


class ScalarType(Expression):
    pass


class ScalarExpression(ScalarType):
    pass


class Cast(ScalarExpression):
    def __init__(self, expression: Expression, to: SqlType):
        self.expression, self.to = self._arg(expression), to


class Q(ScalarExpression):
    def __init__(self, left: Any, op: str, right: Any, sql_type: SqlType = None):
        super().__init__(sql_type=sql_type)

        self.left, self.op, self.right = self._arg(left), op, self._arg(right)


class Coalesce(ScalarExpression):
    def __init__(self, *expressions: Any, sql_type: SqlType | None = None):
        super().__init__(sql_type=sql_type)

        self.expressions = self._list_arg(expressions)


class AtTimeZone(ScalarExpression):
    def __init__(
        self,
        expression: Expression,
        zone: str | Expression,
    ):
        super().__init__(sql_type=expression.sql_type)

        self.expression, self.zone = self._arg(expression), self._arg(zone)


Expression.Q = Q
Expression.Coalesce = Coalesce
Expression.Cast = Cast
Expression.AtTimeZone = AtTimeZone
