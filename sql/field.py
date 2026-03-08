from __future__ import annotations

import re
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Self, cast

from .core import Expr, Q, SqlType, Now, Extract, Func

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
        
        all_slots = set()
        for cls in self.__class__.__mro__:
            slots = getattr(cls, "__slots__", [])
            if isinstance(slots, str):
                slots = [slots]
            all_slots.update(slots)

        for slot in all_slots:
            if hasattr(self, slot):
                setattr(clone, slot, getattr(self, slot))

        clone.model = owner
        clone.relations = {owner}
        clone._window = None
        clone.is_aggregate = False
        
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
    __slots__ = ("similarity_threshold", )

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


class NumericField(Field):
    sql_type = SqlType.NUMERIC

    def __init__(self, precision: int = 10, scale: int = 2, **kwargs):
        super().__init__(**kwargs)

        self.sql_type = f"NUMERIC({precision}, {scale})"


class DecimalField(NumericField):
    pass


class DateField(Field):
    sql_type = SqlType.DATE

    @property
    def age(self) -> Expr:
        return Extract(Func("YEAR FROM AGE", self))

    @property
    def year(self) -> Expr:
        return Extract(Func("YEAR FROM", self))

    @property
    def month(self) -> Expr:
        return Extract(Func("MONTH FROM", self))
