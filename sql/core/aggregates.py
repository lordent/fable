from types import EllipsisType
from typing import Any, Literal, Self

from sql.core.base import QueryContext
from sql.core.enums import FrameBound, FrameMode
from sql.core.expressions import Expression
from sql.core.order import OrderBy
from sql.core.scalars import AtTimeZone, Cast, Coalesce, Func, Q, ScalarExpression
from sql.core.types import AggregateType, T_SqlType


class AggregateExpression(AggregateType, Expression):
    def filter(self, condition: Expression) -> FilteredAggregate:
        return FilteredAggregate(self, condition)

    def over(
        self,
        partition_by: list[Expression] = None,
        order_by: list[OrderBy] = None,
    ) -> WindowExpression:
        return WindowExpression(self, partition_by, order_by)


class AggregateQ(AggregateExpression, Q):
    pass


class AggregateCoalesce(AggregateExpression, Coalesce):
    pass


class AggregateCast(AggregateExpression, Cast):
    pass


class AggregateAtTimeZone(AggregateExpression, AtTimeZone):
    pass


class AggregateFunc(AggregateExpression, Func):
    def __init__(
        self,
        *args: Any,
        distinct: bool = False,
        sql_type: T_SqlType = None,
    ):
        super().__init__(*args, sql_type=sql_type)

        self.distinct = distinct

    def __sql__(self, context: QueryContext) -> str:
        return self.__sql_args__(context, "DISTINCT " if self.distinct else "")


class UnaryAggregate(AggregateFunc):
    def __init__(
        self, expression: Expression, distinct: bool = False, sql_type: T_SqlType = None
    ):
        super().__init__(expression, distinct=distinct, sql_type=sql_type)


class FilteredAggregate(AggregateExpression):
    def __init__(self, expression: AggregateExpression, condition: Expression):
        super().__init__()

        self.expression, self.condition = self._arg(expression), self._arg(condition)

    def __sql__(self, context: QueryContext) -> str:
        agg_sql = self.expression.__sql__(context)
        cond_sql = self.condition.__sql__(context)
        return f"{agg_sql} FILTER (WHERE {cond_sql})"


class WindowExpression(ScalarExpression):
    def __init__(
        self,
        expression: AggregateExpression,
        partition_by: Expression | list[Expression] = None,
        order_by: OrderBy | list[OrderBy] | Expression | list[Expression] = None,
    ):
        super().__init__(sql_type=expression.sql_type)

        self.expression = self._arg(expression)
        self.partition_by: list[Expression] = self._list_arg(partition_by)
        self.order_by: list[Expression] = self._list_arg(order_by)
        self._mode = FrameMode.ROWS
        self._frame: (
            tuple[
                FrameMode,
                int | Literal[FrameBound.START],
                int | Literal[FrameBound.END, FrameBound.CURRENT],
            ]
            | None
        ) = None

    @property
    def rows(self) -> Self:
        self._mode = FrameMode.ROWS
        return self

    @property
    def range(self) -> Self:
        self._mode = FrameMode.RANGE
        return self

    def __getitem__(
        self, item: slice[int | EllipsisType | None, int | EllipsisType | None]
    ) -> Self:
        start = FrameBound.START if item.start in (Ellipsis, None) else item.start
        stop = (
            FrameBound.END
            if item.stop is Ellipsis
            else (item.stop if item.stop is not None else FrameBound.CURRENT)
        )
        self._frame = (self._mode, start, stop)
        return self

    def __sql_bound__(self, bound: Any) -> str:
        if isinstance(bound, int):
            if bound == 0:
                return str(FrameBound.CURRENT)
            return f"{abs(bound)} PRECEDING" if bound > 0 else f"{abs(bound)} FOLLOWING"
        return str(bound)

    def __sql__(self, context: QueryContext) -> str:
        agg_sql = self.expression.__sql__(context)

        parts = []

        if self.partition_by:
            parts.append(
                f"PARTITION BY {
                    (', '.join(p.__sql__(context) for p in self.partition_by))
                }"
            )

        if self.order_by:
            parts.append(
                f"ORDER BY {
                    (
                        ', '.join(
                            o.__sql__(context)
                            if isinstance(o, OrderBy)
                            else o.asc().__sql__(context)
                            for o in self.order_by
                        )
                    )
                }"
            )

        if self._frame:
            mode, start, end = self._frame
            parts.append(
                f"{mode.value} BETWEEN {self.__sql_bound__(start)} "
                f"AND {self.__sql_bound__(end)}"
            )

        spec_sql = f"({' '.join(parts)})" if parts else "()"
        return f"{agg_sql} OVER {spec_sql}"


AggregateType.Q = AggregateQ
AggregateType.Cast = AggregateCast
AggregateType.Coalesce = AggregateCoalesce
AggregateType.AtTimeZone = AggregateAtTimeZone
