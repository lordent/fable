from __future__ import annotations

import re
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Self, cast

from .core import Expr, Q, SqlType

if TYPE_CHECKING:
    from .model import Model


class ReferentialAction(StrEnum):
    CASCADE = "CASCADE"
    RESTRICT = "RESTRICT"
    SET_NULL = "SET NULL"
    SET_DEFAULT = "SET DEFAULT"
    NO_ACTION = "NO ACTION"


class Field(Expr):
    __slots__ = ("pk", "nullable", "unique", "default", "index", "name", "model")

    sql_type: str | SqlType = SqlType.TEXT

    def __init__(
        self,
        pk: bool = False,
        nullable: bool = True,
        unique: bool = False,
        default: Any = None,
        index: bool = False,
    ):
        super().__init__(is_aggregate=False)
        self.pk = pk
        self.nullable = nullable
        self.unique = unique
        self.default = default
        self.index = index
        self.name: str = ""
        self.model: type[Model] | None = None

    def __set_name__(self, owner: type[Model], name: str):
        self.name = name
        self.model = owner
        owner._fields[name] = self

    def __get__(self, instance: Any, owner: type[Model]) -> Self:
        if instance is not None:
            return instance.__dict__.get(self.name)

        clone = object.__new__(self.__class__)
        clone.name = self.name
        clone.pk = self.pk
        clone.nullable = self.nullable
        clone.unique = self.unique
        clone.default = self.default
        clone.index = self.index
        clone.model = owner
        clone.relations = {owner}
        clone.is_aggregate = False
        clone._window = None
        return cast(Self, clone)

    def compile(self, params: list[Any]) -> str:
        alias = self.model._alias if self.model else "unknown"
        return f'"{alias}"."{self.name}"'

    def matches(self, pattern: str | re.Pattern) -> Q:
        flags = 0
        if isinstance(pattern, re.Pattern):
            flags = pattern.flags
            pattern = pattern.pattern

        operator = "~*" if flags & re.IGNORECASE else "~"
        return Q(operator, self, pattern)

    def icontains(self, value: Any) -> Q:
        return Q("ILIKE", self, f"%{value}%")

    def istartswith(self, value: Any) -> Q:
        return Q("ILIKE", self, f"{value}%")

    def iendswith(self, value: Any) -> Q:
        return Q("ILIKE", self, f"%{value}")

    def __str__(self):
        return self.compile([])


class TextField(Field):
    sql_type = SqlType.TEXT

    def __init__(self, similarity_threshold: float = 0.3, **kwargs):
        super().__init__(**kwargs)
        self.similarity_threshold = similarity_threshold

    def similar(self, other: Any, threshold: float | None = None) -> Q:
        return (self % other) < (threshold or self.similarity_threshold)


class IntField(Field):
    sql_type = SqlType.INT


class BigIntField(Field):
    sql_type = SqlType.BIGINT


class SerialField(Field):
    sql_type = "BIGSERIAL"

    def __init__(self, **kwargs):
        kwargs.update({"pk": True, "nullable": False})
        super().__init__(**kwargs)


class ArrayField(Field):
    __slots__ = ("base_field", "sql_type")

    def __init__(self, base_field: type[Field] | Field, **kwargs):
        self.base_field = base_field
        b_type = base_field.sql_type if hasattr(base_field, "sql_type") else "TEXT"
        self.sql_type = f"{b_type}[]"
        super().__init__(**kwargs)

    def __getitem__(self, index: int) -> Q:
        pg_index = index + 1 if index >= 0 else index
        return Q("[]", self, pg_index)

    def overlaps(self, other: list | Expr) -> Q:
        return Q("&&", self, other)


class JSONBField(Field):
    sql_type = SqlType.JSONB

    def __getitem__(self, key: str | int) -> Q:
        return Q("->>", self, key)


class TimestampField(Field):
    sql_type = SqlType.TIMESTAMPTZ

    def __init__(self, auto_now: bool = False, **kwargs):
        super().__init__(**kwargs)
        if auto_now:
            from .core import Now

            self.default = Now()


class TimeField(Field):
    sql_type = SqlType.TIME


class TimeZoneField(Field):
    sql_type = SqlType.TEXT


class ForeignKey(Field):
    __slots__ = ("to", "on_delete")

    sql_type = SqlType.BIGINT

    def __init__(
        self,
        to: type[Model],
        on_delete: ReferentialAction = ReferentialAction.CASCADE,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.to = to
        self.on_delete = on_delete

    def __set_name__(self, owner: type[Model], name: str):
        super().__set_name__(owner, name)
        owner._foreign_fields[name] = self
