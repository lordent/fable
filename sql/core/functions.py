from enum import StrEnum
from typing import Any

from sql.core.expressions import Expression, Func, Raw
from sql.core.types import SqlType, Types


def Rank():
    return Func("RANK")


def DenseRank():
    return Func("DENSE_RANK")


def RowNumber():
    return Func("ROW_NUMBER")


def Now():
    return Func("NOW")


def Age(source: Any, relative_to: Any | None = None):
    args = [source]
    if relative_to is not None:
        args.append(relative_to)

    return Func("AGE", *args, sql_type=Types.INTERVAL)


class DatePart(StrEnum):
    YEAR = "YEAR"
    MONTH = "MONTH"
    DAY = "DAY"
    HOUR = "HOUR"
    MINUTE = "MINUTE"
    SECOND = "SECOND"
    WEEK = "WEEK"
    QUARTER = "QUARTER"
    EPOCH = "EPOCH"
    DOW = "DOW"
    DOY = "DOY"


class Extract(Expression):
    YEAR = DatePart.YEAR
    MONTH = DatePart.MONTH
    DAY = DatePart.DAY
    HOUR = DatePart.HOUR
    MINUTE = DatePart.MINUTE
    SECOND = DatePart.SECOND
    WEEK = DatePart.WEEK
    QUARTER = DatePart.QUARTER
    EPOCH = DatePart.EPOCH
    DOW = DatePart.DOW
    DOY = DatePart.DOY

    def __init__(self, source: Any, part: DatePart):
        super().__init__(sql_type=Types.INTEGER)

        self.source = self._arg(source)
        self.part = part

    def __sql__(self, context: QueryContext) -> str:
        return Raw(t"EXTRACT({self.part.value} FROM {self.source})").__sql__(context)


def Coalesce(*args: Any, sql_type: SqlType | None = None):
    return Func("COALESCE", *args, sql_type=sql_type)
