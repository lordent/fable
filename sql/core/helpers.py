from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, get_args, get_origin, overload
from uuid import UUID

from sql.core.aggregates import AggregateCoalesce, AggregateExpression
from sql.core.expressions import Expression
from sql.core.scalars import Coalesce, ScalarExpression
from sql.core.types import SqlType, Types


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


def from_python(value: Any) -> SqlType:
    origin_type = value if isinstance(value, type) else type(value)
    generic_origin = get_origin(origin_type)
    if generic_origin is list or origin_type is list:
        args = get_args(origin_type)
        if args:
            return from_python(args[0])[:]
        return Types.JSONB

    mapping = {
        str: Types.TEXT,
        int: Types.BIGINT,
        float: Types.DOUBLE_PRECISION,
        bool: Types.BOOLEAN,
        Decimal: Types.NUMERIC,
        datetime: Types.TIMESTAMPTZ,
        date: Types.DATE,
        time: Types.TIME,
        timedelta: Types.INTERVAL,
        UUID: Types.UUID,
        bytes: Types.BYTEA,
        dict: Types.JSONB,
        None.__class__: Types.TEXT,
    }

    for py_type, sql_type in mapping.items():
        if issubclass(origin_type, py_type):
            return sql_type

    return Types.TEXT
