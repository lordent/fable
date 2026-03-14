import copy
from collections.abc import Callable
from enum import StrEnum
from functools import cached_property
from string.templatelib import Template
from typing import TYPE_CHECKING, Any, Self, TypeVar, cast

from sql.core import Concat, E, Func, Q
from sql.enums import SqlType, Types
from sql.func import Extract

if TYPE_CHECKING:
    from .model import Model

F = TypeVar("F", bound="Field")


class ReferentialAction(StrEnum):
    CASCADE = "CASCADE"
    RESTRICT = "RESTRICT"
    SET_NULL = "SET NULL"
    SET_DEFAULT = "SET DEFAULT"
    NO_ACTION = "NO ACTION"


class FieldMeta(type):
    def __call__(cls: type[F], *args, **kwargs) -> F:
        return cast(F, FieldFactory(cls, args, kwargs))


class FieldFactory(E):
    def __init__(self, field_cls: type[Field], args: tuple, kwargs: dict):
        self.field_cls, self.args, self.kwargs = field_cls, args, kwargs

    def factory(self, owner: type[Model] = None, name: str = None):
        try:
            instance: Field = type.__call__(
                self.field_cls, *copy.deepcopy(self.args), **copy.deepcopy(self.kwargs)
            )
            instance.bind = self.factory
            if owner and name:
                setattr(owner, name, instance)
                instance.__set_name__(owner, name)
            return instance
        except NameError:
            pass

    def __set_name__(self, owner: type[Model], name: str):
        return self.factory(owner, name)

    @cached_property
    def sql_type(self):
        return self.factory().sql_type


class Field(E, metaclass=FieldMeta):
    sql_type: SqlType = Types.TEXT

    def __set_name__(self, owner: type[Model], name: str):
        self.model, self.name = owner, name
        self.relations.add(self.model)
        owner._fields[name] = self

    def bind(self, owner: type[Model], name: str) -> Self: ...

    def __sql__(self, params: list[Any]) -> str:
        return f'"{self.model._alias}"."{self.name}"'


class ForeignField[M: type["Model"]](Field):
    to: type[Model]
    sql_type = Types.BIGINT

    def __init__(
        self, to: M, on_delete: ReferentialAction = ReferentialAction.CASCADE, **kwargs
    ):
        super().__init__(**kwargs)
        self.to = to
        self.on_delete = on_delete

    def __set_name__(self, owner: type[Model], name: str):
        super().__set_name__(owner, name)

        owner._foreign_fields[name] = self


class ArrayField[F: "Field"](Field):
    def __init__(self, base_field: F, **kwargs):
        super().__init__(**kwargs)
        self.base_field: F = cast(FieldFactory, base_field).factory()
        self.sql_type = self.base_field.sql_type[:]


class ComputedField(Field):
    def __init__(self, expression: Callable[[], Q | Template] | Q | Template, **kwargs):
        super().__init__(**kwargs)

        expression = expression() if callable(expression) else expression

        if isinstance(expression, Template):
            expression = Concat(expression)

        self.is_aggregate = expression.is_aggregate
        self.relations |= expression.relations
        self.sql_type = expression.sql_type or self.sql_type
        self.expression = expression

    def __sql__(self, params: list[Any]) -> str:
        return self.expression.__sql__(params)


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
    def __init__(self, max_length: int | None = None, **kwargs):
        sql_type = Types.VARCHAR(max_length) if max_length else Types.TEXT
        super().__init__(sql_type=sql_type, **kwargs)


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
        return Extract(Func("YEAR FROM AGE", self))

    @property
    def year(self):
        return Extract(Func("YEAR FROM", self))

    @property
    def month(self):
        return Extract(Func("MONTH FROM", self))


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
