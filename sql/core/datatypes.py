from typing import Any


class SqlType:
    def __init__(
        self,
        name: str,
        size: int | None = None,
        align: int | None = None,
        args=None,
        array_dim="",
    ):
        self.name = name
        self.size = size
        self.align = align
        self.args = args or []
        self.array_dim = array_dim

    def __call__(self, *args):
        return SqlType(self.name, self.size, self.align, args, array_dim=self.array_dim)

    def __getitem__(self, item):
        size = "" if isinstance(item, slice) else str(item)
        return SqlType(
            self.name, -1, self.align, self.args, array_dim=f"{self.array_dim}[{size}]"
        )

    def __str__(self):
        args_str = f"({', '.join(map(str, self.args))})" if self.args else ""
        return f"{self.name}{args_str}{self.array_dim}"


class SqlTypeMeta(type):
    def __new__(mcs, name, bases, attrs: dict[str, Any]):
        for key, value in attrs.items():
            if key.startswith("__"):
                continue
            attrs[key] = SqlType(value[0], value[1], value[2])
        return super().__new__(mcs, name, bases, attrs)

    def __getattr__(cls, name: str):
        return SqlType(name.replace("_", " "))


class Types(metaclass=SqlTypeMeta):
    # Числа
    SMALLINT = "SMALLINT", 2, 2
    INT = INTEGER = "INTEGER", 4, 4
    BIGINT = "BIGINT", 8, 8
    NUMERIC = "NUMERIC", -1, 4
    DECIMAL = "DECIMAL", -1, 4
    REAL = "REAL", 4, 4
    DOUBLE_PRECISION = "DOUBLE PRECISION", 8, 8
    SMALLSERIAL = "SMALLSERIAL", 2, 2
    SERIAL = "SERIAL", 4, 4
    BIGSERIAL = "BIGSERIAL", 8, 8

    # Текст
    TEXT = "TEXT", -1, 4
    VARCHAR = "VARCHAR", -1, 4
    CHAR = "CHAR", -1, 4
    CITEXT = "CITEXT", -1, 4

    # Дата и время
    DATE = "DATE", 4, 4
    TIME = "TIME", 8, 8
    TIMETZ = "TIME WITH TIME ZONE", 12, 4
    TIMESTAMP = "TIMESTAMP", 8, 8
    TIMESTAMPTZ = "TIMESTAMP WITH TIME ZONE", 8, 8
    INTERVAL = "INTERVAL", 16, 8

    # Логика и бинарные данные
    BOOLEAN = BOOL = "BOOLEAN", 1, 1
    BYTEA = "BYTEA", -1, 4
    BIT = "BIT", -1, 4
    VARBIT = "VARBIT", -1, 4

    # JSON и XML
    JSON = "JSON", -1, 4
    JSONB = "JSONB", -1, 4
    XML = "XML", -1, 4

    # Сетевые типы
    INET = "INET", -1, 4
    CIDR = "CIDR", -1, 4
    MACADDR = "MACADDR", 6, 1
    MACADDR8 = "MACADDR8", 8, 8

    # Геометрия
    POINT = "POINT", 16, 8
    LINE = "LINE", 32, 8
    LSEG = "LSEG", 32, 8
    BOX = "BOX", 32, 8
    PATH = "PATH", -1, 8
    POLYGON = "POLYGON", -1, 8
    CIRCLE = "CIRCLE", 24, 8

    # Специфические / Расширения
    UUID = "UUID", 16, 1
    MONEY = "MONEY", 8, 8
    TSVECTOR = "TSVECTOR", -1, 4
    TSQUERY = "TSQUERY", -1, 4
    HSTORE = "HSTORE", -1, 4
    LTREE = "LTREE", -1, 4

    # Диапазоны (Range Types)
    INT4RANGE = "INT4RANGE", -1, 8
    INT8RANGE = "INT8RANGE", -1, 8
    NUMRANGE = "NUMRANGE", -1, 8
    DATERANGE = "DATERANGE", -1, 8
    TSRANGE = "TSRANGE", -1, 8
    TSTZRANGE = "TSTZRANGE", -1, 8

    # Мультидиапазоны (PostgreSQL 14+)
    INT4MULTIRANGE = "INT4MULTIRANGE", -1, 8
    INT8MULTIRANGE = "INT8MULTIRANGE", -1, 8
    NUMMULTIRANGE = "NUMMULTIRANGE", -1, 8
    DATEMULTIRANGE = "DATEMULTIRANGE", -1, 8
    TSMULTIRANGE = "TSMULTIRANGE", -1, 8
    TSTZMULTIRANGE = "TSTZMULTIRANGE", -1, 8
