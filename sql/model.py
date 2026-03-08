from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, TypeVar, cast

if TYPE_CHECKING:
    from .query import Select

from .field import Field, ForeignKey, SerialField

T = TypeVar("T", bound="Model")


class ModelMeta(type):
    def __new__(mcs, name: str, bases: tuple[type[Model], ...], attrs: dict[str, Any]):
        fields: dict[str, Field] = {}
        foreign_fields: dict[str, ForeignKey] = {}

        for base in bases:
            if hasattr(base, "_fields"):
                fields.update(base._fields)
            if hasattr(base, "_foreign_fields"):
                foreign_fields.update(base._foreign_fields)

        attrs.setdefault("_table", name.lower())
        attrs.setdefault("_alias", name)
        attrs["_fields"] = fields
        attrs["_foreign_fields"] = foreign_fields

        return super().__new__(mcs, name, bases, attrs)

    def __getitem__(cls: type[T], alias: str) -> type[T]:
        return cast(type[T], type(cls.__name__, (cls,), {"_alias": alias}))

    def __iter__(cls: type[T]) -> Iterator[Field]:
        for field in cls._fields.values():
            yield getattr(cls, field.name)

    def __hash__(cls: type[T]) -> int:
        return hash(cls._alias)

    def __eq__(cls: type[T], other: type[Model]) -> bool:
        if not isinstance(other, ModelMeta):
            return False
        return cls._table == other._table and cls._alias == other._alias


class Model(metaclass=ModelMeta):
    _table: str
    _alias: str
    _fields: dict[str, Field]
    _foreign_fields: dict[str, ForeignKey]

    id = SerialField()

    def __init__(self, **kwargs: Any):
        """Для создания экземпляров: user = User(id=1, name='Bob')"""
        for key, value in kwargs.items():
            if key in self._fields:
                self.__dict__[key] = value

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} table={self._table} alias={self._alias}>"


class QueryModel(Model):
    _virtual = True
    _table: Select
