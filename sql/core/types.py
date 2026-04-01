from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sql.core.aggregates import (
        AggregateAtTimeZone,
        AggregateCast,
        AggregateCoalesce,
        AggregateQ,
    )
    from sql.core.case import AggregateCase, ScalarCase
    from sql.core.raw import AggregateRaw, AggregateRef, ScalarRaw, ScalarRef
    from sql.core.scalars import AtTimeZone, Cast, Coalesce, Q
    from sql.fields.base import Field
    from sql.models import Model, ProxyModel


class SqlType:
    def __init__(self, name: str, args=None, array_dim=""):
        self.name = name
        self.args = args or []
        self.array_dim = array_dim

    def __call__(self, *args):
        """Поддержка точности и параметров: NUMERIC(12, 2), TIME(6)"""
        return SqlType(self.name, args, array_dim=self.array_dim)

    def __getitem__(self, item):
        """Поддержка массивов: DATE[:], INT[10], TEXT[:][:]"""
        size = "" if isinstance(item, slice) else str(item)
        return SqlType(self.name, self.args, array_dim=f"{self.array_dim}[{size}]")

    def __str__(self):
        args_str = f"({', '.join(map(str, self.args))})" if self.args else ""
        return f"{self.name}{args_str}{self.array_dim}"

    def __repr__(self):
        return f"'{self.__str__()}'"


class SqlTypeMeta(type):
    def __new__(mcs, name, bases, attrs: dict[str, Any]):
        for key, value in attrs.items():
            if isinstance(value, str) and not key.startswith("__"):
                attrs[key] = SqlType(value)
        return super().__new__(mcs, name, bases, attrs)

    def __getattr__(cls, name: str):
        return SqlType(name.replace("_", " "))


class Types(metaclass=SqlTypeMeta):
    # Числа
    SMALLINT = "SMALLINT"
    INT = INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    NUMERIC = "NUMERIC"
    DECIMAL = "DECIMAL"
    REAL = "REAL"
    DOUBLE_PRECISION = "DOUBLE PRECISION"
    SMALLSERIAL = "SMALLSERIAL"
    SERIAL = "SERIAL"
    BIGSERIAL = "BIGSERIAL"

    # Текст
    TEXT = "TEXT"
    VARCHAR = "VARCHAR"
    CHAR = "CHAR"
    CITEXT = "CITEXT"  # Расширение citext

    # Дата и время
    DATE = "DATE"
    TIME = "TIME"
    TIMETZ = "TIME WITH TIME ZONE"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMPTZ = "TIMESTAMP WITH TIME ZONE"
    INTERVAL = "INTERVAL"

    # Логика и бинарные данные
    BOOLEAN = BOOL = "BOOLEAN"
    BYTEA = "BYTEA"
    BIT = "BIT"
    VARBIT = "VARBIT"

    # JSON и XML
    JSON = "JSON"
    JSONB = "JSONB"
    XML = "XML"

    # Сетевые типы
    INET = "INET"
    CIDR = "CIDR"
    MACADDR = "MACADDR"
    MACADDR8 = "MACADDR8"

    # Геометрия
    POINT = "POINT"
    LINE = "LINE"
    LSEG = "LSEG"
    BOX = "BOX"
    PATH = "PATH"
    POLYGON = "POLYGON"
    CIRCLE = "CIRCLE"

    # Специфические / Расширения
    UUID = "UUID"
    MONEY = "MONEY"
    TSVECTOR = "TSVECTOR"
    TSQUERY = "TSQUERY"
    HSTORE = "HSTORE"
    LTREE = "LTREE"

    # Диапазоны (Range Types)
    INT4RANGE = "INT4RANGE"
    INT8RANGE = "INT8RANGE"
    NUMRANGE = "NUMRANGE"
    DATERANGE = "DATERANGE"
    TSRANGE = "TSRANGE"
    TSTZRANGE = "TSTZRANGE"

    # Мультидиапазоны (PostgreSQL 14+)
    INT4MULTIRANGE = "INT4MULTIRANGE"
    INT8MULTIRANGE = "INT8MULTIRANGE"
    NUMMULTIRANGE = "NUMMULTIRANGE"
    DATEMULTIRANGE = "DATEMULTIRANGE"
    TSMULTIRANGE = "TSMULTIRANGE"
    TSTZMULTIRANGE = "TSTZMULTIRANGE"


class ScalarType:
    Case: type[ScalarCase]
    Q: type[Q]
    Cast: type[Cast]
    Coalesce: type[Coalesce]
    AtTimeZone: type[AtTimeZone]
    Ref: type[ScalarRef]
    Raw: type[ScalarRaw]


class AggregateType:
    Case: type[AggregateCase]
    Q: type[AggregateQ]
    Cast: type[AggregateCast]
    Coalesce: type[AggregateCoalesce]
    AtTimeZone: type[AggregateAtTimeZone]
    Ref: type[AggregateRef]
    Raw: type[AggregateRaw]


class QueryType:
    pass


type T_SqlType = SqlType | Field | None
type T_Model = type[Model] | ProxyModel
