from typing import TYPE_CHECKING, Any

from sql.core.expressions import Q
from sql.core.functions import Age, Extract
from sql.core.types import Types
from sql.fields.base import Field

if TYPE_CHECKING:
    pass


class ArrayField[F: "Field"](Field):
    def __init__(self, base_field: F, **kwargs):
        super().__init__(**kwargs)

        self.base_field = base_field
        self.sql_type = self.base_field.sql_type[:]


# --- Числа ---


class SmallIntField(Field):
    sql_type = Types.SMALLINT


class IntField(Field):
    sql_type = Types.INT


class BigIntField(Field):
    sql_type = Types.BIGINT


class SmallSerialField(Field):
    sql_type = Types.SMALLSERIAL


class SerialField(Field):
    sql_type = Types.SERIAL


class BigSerialField(Field):
    sql_type = Types.BIGSERIAL


class RealField(Field):
    sql_type = Types.REAL


class DoubleField(Field):
    sql_type = Types.DOUBLE_PRECISION


class NumericField(Field):
    def __init__(self, precision: int = 10, scale: int = 2, **kwargs):
        super().__init__(sql_type=Types.NUMERIC(precision, scale), **kwargs)


class DecimalField(NumericField):
    pass


# --- Текст ---


class TextField(Field):
    def __init__(
        self, max_length: int | None = None, similarity_threshold: float = 0.3, **kwargs
    ):
        sql_type = Types.VARCHAR(max_length) if max_length else Types.TEXT
        super().__init__(sql_type=sql_type, **kwargs)

        self.similarity_threshold = similarity_threshold

    def similar(self, other: Any, threshold: float | None = None) -> Q:
        return self.dist(other) < (threshold or self.similarity_threshold)


class CharField(Field):
    def __init__(self, max_length: int, **kwargs):
        super().__init__(sql_type=Types.VARCHAR(max_length), **kwargs)


class FixedCharField(Field):
    def __init__(self, length: int, **kwargs):
        super().__init__(sql_type=Types.CHAR(length), **kwargs)


class CitextField(Field):
    sql_type = Types.CITEXT


# --- Дата и Время ---


class DateField(Field):
    sql_type = Types.DATE

    @property
    def age(self):
        return Extract(Age(self), part=Extract.YEAR)

    @property
    def year(self):
        return Extract(self, part=Extract.YEAR)

    @property
    def month(self):
        return Extract(self, part=Extract.MONTH)


class TimeField(Field):
    def __init__(self, precision: int | None = None, with_tz: bool = False, **kwargs):
        t = Types.TIMETZ if with_tz else Types.TIME
        super().__init__(
            sql_type=t(precision) if precision is not None else t, **kwargs
        )


class TimestampField(Field):
    def __init__(self, precision: int | None = None, with_tz: bool = True, **kwargs):
        t = Types.TIMESTAMPTZ if with_tz else Types.TIMESTAMP
        super().__init__(
            sql_type=t(precision) if precision is not None else t, **kwargs
        )


class IntervalField(Field):
    sql_type = Types.INTERVAL


class TimeZoneField(Field):
    sql_type = Types.TEXT


# --- Логика и Бинарные данные ---


class BoolField(Field):
    sql_type = Types.BOOLEAN


class ByteaField(Field):
    sql_type = Types.BYTEA


class BitField(Field):
    def __init__(self, length: int = 1, varying: bool = False, **kwargs):
        t = Types.VARBIT if varying else Types.BIT
        super().__init__(sql_type=t(length), **kwargs)


# --- JSON / XML / Специфические ---


class JsonField(Field):
    sql_type = Types.JSON


class JsonbField(Field):
    sql_type = Types.JSONB


class XmlField(Field):
    sql_type = Types.XML


class UuidField(Field):
    sql_type = Types.UUID


class MoneyField(Field):
    sql_type = Types.MONEY


# --- Сетевые типы ---


class InetField(Field):
    sql_type = Types.INET


class CidrField(Field):
    sql_type = Types.CIDR


class MacAddrField(Field):
    sql_type = Types.MACADDR


class MacAddr8Field(Field):
    sql_type = Types.MACADDR8


# --- Геометрия ---


class PointField(Field):
    sql_type = Types.POINT


class LineField(Field):
    sql_type = Types.LINE


class LsegField(Field):
    sql_type = Types.LSEG


class BoxField(Field):
    sql_type = Types.BOX


class PathField(Field):
    sql_type = Types.PATH


class PolygonField(Field):
    sql_type = Types.POLYGON


class CircleField(Field):
    sql_type = Types.CIRCLE


# --- Поиск и Расширения ---


class TsVectorField(Field):
    sql_type = Types.TSVECTOR


class TsQueryField(Field):
    sql_type = Types.TSQUERY


class HstoreField(Field):
    sql_type = Types.HSTORE


class LtreeField(Field):
    sql_type = Types.LTREE


# --- Диапазоны ---


class Int4RangeField(Field):
    sql_type = Types.INT4RANGE


class Int8RangeField(Field):
    sql_type = Types.INT8RANGE


class NumRangeField(Field):
    sql_type = Types.NUMRANGE


class TsRangeField(Field):
    sql_type = Types.TSRANGE


class TsTzRangeField(Field):
    sql_type = Types.TSTZRANGE


class DateRangeField(Field):
    sql_type = Types.DATERANGE
