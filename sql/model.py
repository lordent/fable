from collections.abc import Iterator
from typing import TypeVar, cast

from .field import Field, ForeignKey, SerialField

T = TypeVar("T", bound="Model")


class ModelMeta(type):
    def __new__(mcs, name: str, bases: tuple[type, ...], initial: dict):
        initial.setdefault("_table", name.lower())
        initial.setdefault("_alias", name)
        initial.setdefault("_fields", {})
        initial.setdefault("_foreign_fields", {})
        return super().__new__(mcs, name, bases, initial)

    def __getitem__(cls: type[T], alias: str) -> type[T]:
        return cast(type[T], type(cls.__name__, (cls,), {"_alias": alias}))

    def __iter__(cls: type[T]) -> Iterator[Field]:
        for name in cls._fields:
            yield getattr(cls, name)

    def __hash__(cls: type[T]):
        return hash(cls._alias)

    def __eq__(cls: type[T], other: type[Model]):
        return hash(cls) == hash(other)


class Model(metaclass=ModelMeta):
    _table: str
    _alias: str
    _fields: dict[str, Field]
    _foreign_fields: dict[str, ForeignKey]
    id = SerialField()
