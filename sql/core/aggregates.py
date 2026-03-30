from sql.core.base import OrderBy, QueryContext
from sql.core.expressions import (
    AggregateType,
    AtTimeZone,
    Cast,
    Coalesce,
    Expression,
    Q,
    ScalarExpression,
)


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


AggregateExpression.Q = AggregateQ
AggregateExpression.Coalesce = AggregateCoalesce
AggregateExpression.Cast = AggregateCast
AggregateExpression.AtTimeZone = AggregateAtTimeZone


class FilteredAggregate(AggregateExpression):
    def __init__(self, expression: AggregateExpression, condition: Expression):
        self.expression, self.condition = self._arg(expression), self._arg(condition)

    def __sql__(self, context: QueryContext) -> str:
        agg_sql = self.expression.__sql__(context)
        cond_sql = self.condition.__sql__(context)
        return f"{agg_sql} FILTER (WHERE {cond_sql})"


class WindowExpression(ScalarExpression):
    def __init__(
        self,
        expression: AggregateExpression,
        partition_by: list[Expression] = None,
        order_by: list[OrderBy] = None,
    ):
        super().__init__(sql_type=expression.sql_type)

        self.expression = self._arg(expression)
        self.partition_by = self._list_arg(partition_by)
        self.order_by = self._list_arg(order_by)

    def __sql__(self, context: QueryContext) -> str:
        parts = []

        if self.partition_by:
            p_sql = ", ".join(p.__sql__(context) for p in self.partition_by)
            parts.append(f"PARTITION BY {p_sql}")

        if self.order_by:
            o_sql = ", ".join(o.__sql__(context) for o in self.order_by)
            parts.append(f"ORDER BY {o_sql}")

        window_spec = f"({' '.join(parts)})" if parts else "()"
        return f"{self.expression.__sql__(context)} OVER {window_spec}"
