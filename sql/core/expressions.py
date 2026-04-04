from typing import TYPE_CHECKING, Any, overload

from sql.core.base import COLLECTION_TYPES, Node
from sql.core.datatypes import Types
from sql.core.order import OrderBy
from sql.core.types import AggregateType, ScalarType, T_SqlType

if TYPE_CHECKING:
    from sql.core.aggregates import (
        AggregateAtTimeZone,
        AggregateCast,
        AggregateCoalesce,
        AggregateExpression,
        AggregateQ,
    )
    from sql.core.scalars import AtTimeZone, Cast, Coalesce, Q, ScalarExpression


def q(
    left: Expression,
    op: str,
    right: Expression,
    sql_type: T_SqlType = None,
) -> Q | AggregateQ:
    return next(
        (AggregateType.Q for a in (left, right) if isinstance(a, AggregateType)),
        ScalarType.Q,
    )(left, op, right, sql_type=sql_type)


class Expression(Node):
    sql_type: T_SqlType = None

    def __init__(self, sql_type: str = None):
        super().__init__()

        self.sql_type = sql_type or self.sql_type

    # --- Подсветка типов ---

    @overload
    def __add__(self: AggregateExpression, other: Any) -> AggregateQ: ...
    @overload
    def __add__(self: Any, other: AggregateExpression) -> AggregateQ: ...
    @overload
    def __add__(self: ScalarExpression, other: Any) -> Q: ...

    @overload
    def __sub__(self: AggregateExpression, other: Any) -> AggregateQ: ...
    @overload
    def __sub__(self: Any, other: AggregateExpression) -> AggregateQ: ...
    @overload
    def __sub__(self: ScalarExpression, other: Any) -> Q: ...

    @overload
    def __mul__(self: AggregateExpression, other: Any) -> AggregateQ: ...
    @overload
    def __mul__(self: Any, other: AggregateExpression) -> AggregateQ: ...
    @overload
    def __mul__(self: ScalarExpression, other: Any) -> Q: ...

    @overload
    def __truediv__(self: AggregateExpression, other: Any) -> AggregateQ: ...
    @overload
    def __truediv__(self: Any, other: AggregateExpression) -> AggregateQ: ...
    @overload
    def __truediv__(self: ScalarExpression, other: Any) -> Q: ...

    @overload
    def __mod__(self: AggregateExpression, other: Any) -> AggregateQ: ...
    @overload
    def __mod__(self: Any, other: AggregateExpression) -> AggregateQ: ...
    @overload
    def __mod__(self: ScalarExpression, other: Any) -> Q: ...

    @overload
    def __eq__(self: AggregateExpression, other: Any) -> AggregateQ: ...
    @overload
    def __eq__(self: Any, other: AggregateExpression) -> AggregateQ: ...
    @overload
    def __eq__(self: ScalarExpression, other: Any) -> Q: ...

    @overload
    def __ne__(self: AggregateExpression, other: Any) -> AggregateQ: ...
    @overload
    def __ne__(self: Any, other: AggregateExpression) -> AggregateQ: ...
    @overload
    def __ne__(self: ScalarExpression, other: Any) -> Q: ...

    @overload
    def __gt__(self: AggregateExpression, other: Any) -> AggregateQ: ...
    @overload
    def __gt__(self: Any, other: AggregateExpression) -> AggregateQ: ...
    @overload
    def __gt__(self: ScalarExpression, other: Any) -> Q: ...

    @overload
    def __ge__(self: AggregateExpression, other: Any) -> AggregateQ: ...
    @overload
    def __ge__(self: Any, other: AggregateExpression) -> AggregateQ: ...
    @overload
    def __ge__(self: ScalarExpression, other: Any) -> Q: ...

    @overload
    def __lt__(self: AggregateExpression, other: Any) -> AggregateQ: ...
    @overload
    def __lt__(self: Any, other: AggregateExpression) -> AggregateQ: ...
    @overload
    def __lt__(self: ScalarExpression, other: Any) -> Q: ...

    @overload
    def __le__(self: AggregateExpression, other: Any) -> AggregateQ: ...
    @overload
    def __le__(self: Any, other: AggregateExpression) -> AggregateQ: ...
    @overload
    def __le__(self: ScalarExpression, other: Any) -> Q: ...

    @overload
    def __and__(self: AggregateExpression, other: Any) -> AggregateQ: ...
    @overload
    def __and__(self: Any, other: AggregateExpression) -> AggregateQ: ...
    @overload
    def __and__(self: ScalarExpression, other: Any) -> Q: ...

    @overload
    def __or__(self: AggregateExpression, other: Any) -> AggregateQ: ...
    @overload
    def __or__(self: Any, other: AggregateExpression) -> AggregateQ: ...
    @overload
    def __or__(self: ScalarExpression, other: Any) -> Q: ...

    @overload
    def __invert__(self: AggregateExpression, other: Any) -> AggregateQ: ...
    @overload
    def __invert__(self: Any, other: AggregateExpression) -> AggregateQ: ...
    @overload
    def __invert__(self: ScalarExpression, other: Any) -> Q: ...

    # -- Преобразование типов ---

    @overload
    def cast(self: AggregateExpression, to: T_SqlType) -> AggregateCast: ...

    @overload
    def cast(self: ScalarExpression, to: T_SqlType) -> Cast: ...

    def cast(self, to: T_SqlType) -> Cast | AggregateCast:
        return (
            AggregateType.Cast if isinstance(self, AggregateType) else ScalarType.Cast
        )(self, to)

    @overload
    def __rshift__(self: AggregateExpression, to: T_SqlType) -> AggregateCast: ...

    @overload
    def __rshift__(self: ScalarExpression, to: T_SqlType) -> Cast: ...

    def __rshift__(self, to: T_SqlType) -> Cast | AggregateCast:
        return self.cast(to)

    # -- Таймзона ---

    @overload
    def at_timezone(
        self: AggregateExpression, zone: str | Expression
    ) -> AggregateAtTimeZone: ...

    @overload
    def at_timezone(self: ScalarExpression, zone: str | Expression) -> AtTimeZone: ...

    def at_timezone(self, zone: str | Expression) -> AtTimeZone | AggregateAtTimeZone:
        return (
            AggregateType.AtTimeZone
            if isinstance(self, AggregateType)
            else ScalarType.AtTimeZone
        )(self, zone)

    # -- Coalesce ---

    @overload
    def default(self: AggregateExpression, other: Any) -> AggregateCoalesce: ...

    @overload
    def default(
        self: ScalarExpression, other: AggregateExpression
    ) -> AggregateCoalesce: ...

    @overload
    def default(self: ScalarExpression, other: Any) -> Coalesce: ...

    def default(self, other: Any) -> Coalesce | AggregateCoalesce:
        return (
            AggregateType.Coalesce
            if isinstance(self, AggregateType) or isinstance(other, AggregateType)
            else ScalarType.Coalesce
        )(self, other, sql_type=self.sql_type)

    # --- Сортировка ---

    def asc(self, nulls_first=None) -> OrderBy:
        return OrderBy(self, OrderBy.Direction.ASC, nulls_first)

    def desc(self, nulls_first=None) -> OrderBy:
        return OrderBy(self, OrderBy.Direction.DESC, nulls_first)

    # --- Логические операции ---

    def __and__(self, other: Any) -> Q | AggregateQ:
        return q(self, "AND", other, sql_type=Types.BOOLEAN)

    def __or__(self, other: Any) -> Q | AggregateQ:
        return q(self, "OR", other, sql_type=Types.BOOLEAN)

    def __invert__(self) -> Q | AggregateQ:
        return q(None, "NOT", self, sql_type=Types.BOOLEAN)

    # --- Сравнение ---

    def __eq__(self, other: Any) -> Q | AggregateQ:
        if other is None:
            return q(self, "IS NULL", None, sql_type=Types.BOOLEAN)
        if isinstance(other, COLLECTION_TYPES):
            return q(self, "IN", other, sql_type=Types.BOOLEAN)
        return q(self, "=", other, sql_type=Types.BOOLEAN)

    def __ne__(self, other: Any) -> Q | AggregateQ:
        if other is None:
            return q(self, "IS NOT NULL", None)
        return q(self, "!=", other, sql_type=Types.BOOLEAN)

    def __lt__(self, other: Any) -> Q | AggregateQ:
        return q(self, "<", other, sql_type=Types.BOOLEAN)

    def __le__(self, other: Any) -> Q | AggregateQ:
        return q(self, "<=", other, sql_type=Types.BOOLEAN)

    def __gt__(self, other: Any) -> Q | AggregateQ:
        return q(self, ">", other, sql_type=Types.BOOLEAN)

    def __ge__(self, other: Any) -> Q | AggregateQ:
        return q(self, ">=", other, sql_type=Types.BOOLEAN)

    # --- Арифметика ---

    def __add__(self, other: Any) -> Q | AggregateQ:
        return q(self, "+", other)

    def __sub__(self, other: Any) -> Q | AggregateQ:
        return q(self, "-", other)

    def __mul__(self, other: Any) -> Q | AggregateQ:
        return q(self, "*", other)

    def __truediv__(self, other: Any) -> Q | AggregateQ:
        return q(self, "/", other)

    def __mod__(self, other: Any) -> Q | AggregateQ:
        return q(self, "%", other)

    # --- Массивы и JSON ---

    def contains(self, other: Any) -> Q | AggregateQ:
        return q(
            self,
            "@>",
            [other] if not isinstance(other, COLLECTION_TYPES) else other,
            sql_type=Types.BOOLEAN,
        )

    def overlap(self, other: Any) -> Q | AggregateQ:
        return q(self, "&&", other, sql_type=Types.BOOLEAN)

    def __getitem__(self, key: str | int) -> Q | AggregateQ:
        return q(self, "->", str(key), sql_type=Types.JSONB)

    def text(self, key: str | int) -> Q | AggregateQ:
        return q(self, "->>", str(key), sql_type=Types.TEXT)

    # --- Разное ---

    def dist(self, other: Any) -> Q | AggregateQ:
        return q(self, "<->", other, sql_type=Types.DOUBLE_PRECISION)
