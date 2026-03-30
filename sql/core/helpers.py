from typing import Any, overload

from sql.core.aggregates import AggregateExpression
from sql.core.expressions import (
    AggregateCoalesce,
    Coalesce,
    Expression,
    ScalarExpression,
)


@overload
def coalesce(first: AggregateExpression, *others: Any) -> AggregateCoalesce: ...


@overload
def coalesce(first: Any, *others: AggregateExpression) -> AggregateCoalesce: ...


@overload
def coalesce(first: ScalarExpression, *others: Any) -> Coalesce: ...


def coalesce(*args: Any) -> Coalesce | AggregateCoalesce:
    is_aggregate, sql_type = False, None
    for a in args:
        if isinstance(a, Expression):
            sql_type = sql_type or a.sql_type
            if isinstance(a, AggregateExpression):
                is_aggregate = True
            if sql_type and is_aggregate:
                break

    return (AggregateCoalesce if is_aggregate else Coalesce)(*args, sql_type=sql_type)
