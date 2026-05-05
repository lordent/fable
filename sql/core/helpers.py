from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

from sql.core.datatypes import SqlType, Types
from sql.core.node import COLLECTION_TYPES


def from_python(value: Any) -> SqlType:
    origin_type = value if isinstance(value, type) else type(value)
    if isinstance(value, COLLECTION_TYPES):
        if arg := next(iter(value), None):
            return from_python(arg)[:]
        raise TypeError("Невозможно определить тип")

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

    raise TypeError("Невозможно определить тип")
