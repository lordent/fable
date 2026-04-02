from enum import StrEnum


class OrderDirections(StrEnum):
    DESC = "DESC"
    ASC = "ASC"


class FrameMode(StrEnum):
    ROWS = "ROWS"
    RANGE = "RANGE"
    GROUPS = "GROUPS"


class FrameBound(StrEnum):
    CURRENT = "CURRENT ROW"
    START = "UNBOUNDED PRECEDING"
    END = "UNBOUNDED FOLLOWING"


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


class JoinStrategy(StrEnum):
    LEFT = "LEFT JOIN"
    INNER = "INNER JOIN"
    RIGHT = "RIGHT JOIN"
    FULL = "FULL OUTER JOIN"


class LockMode(StrEnum):
    UPDATE = "FOR UPDATE"
    SHARE = "FOR SHARE"


class GroupMode(StrEnum):
    ROLLUP = "ROLLUP"
    CUBE = "CUBE"
    SETS = "GROUPING SETS"
