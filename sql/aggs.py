from typing import Any

from .core import E
from .enums import SqlType, Types
from .func import Func


class Aggregate(Func):
    def __init__(self, arg: E | str, distinct: bool = False, sql_type: SqlType = None):
        super().__init__(
            self._get_name(),
            arg,
            sql_type=sql_type,
            is_aggregate=True,
        )
        self.distinct = distinct

    def _get_name(self):
        return self.__class__.__name__

    def _render_args(self, params: list[Any]) -> str:
        prefix = "DISTINCT " if self.distinct else ""
        arg = self.args[0]
        if isinstance(arg, E):
            arg_sql = self._value(arg, params)
        else:
            arg_sql = arg
        return f"{prefix}{arg_sql}"


class Count(Aggregate):
    def __init__(self, arg: Any = "*", distinct: bool = False):
        super().__init__(arg, distinct=distinct, sql_type=Types.BIGINT)


class Sum(Aggregate):
    def __init__(self, arg: E, distinct: bool = False):
        super().__init__(arg, distinct=distinct, sql_type=arg.sql_type)


class Avg(Aggregate):
    def __init__(self, arg: E, distinct: bool = False):
        super().__init__(arg, distinct=distinct, sql_type=Types.NUMERIC)


class Min(Aggregate):
    def __init__(self, arg: E):
        super().__init__(arg, sql_type=arg.sql_type)


class Max(Aggregate):
    def __init__(self, arg: E):
        super().__init__(arg, sql_type=arg.sql_type)


class ArrayAgg(Aggregate):
    def __init__(self, arg: E, distinct: bool = False):
        base_type = arg.sql_type or Types.TEXT
        super().__init__(arg, distinct=distinct, sql_type=base_type[:])
