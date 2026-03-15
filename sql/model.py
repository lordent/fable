from collections.abc import Iterator
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Self, TypeVar

from sql.app import Application, get_app_for_module
from sql.db import ConnectionManager, TransactionContext

from .fields import BigSerialField, Field, ForeignField

if TYPE_CHECKING:
    from .queries.base import QueryBuilder

M = TypeVar("M", bound="Model")


class ModelMeta(type):
    def __new__(mcs, name: str, bases: tuple[type[Model], ...], attrs: dict[str, Any]):
        attrs.setdefault("_table", name.lower())
        attrs.setdefault("_alias", name)
        attrs.setdefault("_fields", {})
        attrs.setdefault("_foreign_fields", {})
        attrs.setdefault("_app", None)

        cls: type[Model] = super().__new__(mcs, name, bases, attrs)

        parent_fields: dict[str, Field]

        for base in cls.__mro__[1:]:
            if parent_fields := getattr(base, "_fields", None):
                for name, attr in parent_fields.items():
                    if name not in cls._fields:
                        attr.bind(cls, name)

        if not cls._app and not cls._virtual and bases:
            cls._app = get_app_for_module(cls.__module__)

        return cls

    def __getitem__(cls: type[M], alias: str) -> type[M]:
        return cls.as_alias(alias)

    def __iter__(cls: type[M]) -> Iterator[Field]:
        for field in cls._fields.values():
            yield getattr(cls, field.name)

    def __hash__(cls: type[M]) -> int:
        return hash(cls._alias)

    def __eq__(cls: type[M], other: type[Model]) -> bool:
        if not isinstance(other, ModelMeta):
            return False
        return cls._table == other._table and cls._alias == other._alias


class Model(metaclass=ModelMeta):
    _app: Application
    _virtual = False
    _table: str
    _alias: str | None = None
    _fields: dict[str, Field] = {}
    _foreign_fields: dict[str, ForeignField[Model]] = {}

    id = BigSerialField()

    @classmethod
    @lru_cache(maxsize=128)
    def as_alias(cls: type[Self], alias: str) -> type[Self]:
        return type(
            f"{cls.__name__}_{alias}",
            (cls,),
            {"_alias": alias, "_table": cls._table, "_virtual": True},
        )

    @classmethod
    def __sql__(cls: type[Self], params: list[Any]) -> str:
        if issubclass(cls, QueryModel):
            return f'({cls._query.__sql__(params)}) AS "{cls._alias}"'
        return (
            f'"{cls._table}" AS "{cls._alias}"'
            if cls._alias != cls._table
            else f'"{cls._table}"'
        )

    @classmethod
    def atomic(cls, checkpoint: bool = True):
        return TransactionContext(cls._app.name, checkpoint)

    @classmethod
    def connection(cls):
        return ConnectionManager(cls._app.name)


class QueryModel(Model):
    _virtual = True
    _query: QueryBuilder

    @classmethod
    def factory(cls, query: QueryBuilder):
        alias = f"sub{id(query)}"
        initial = {"_alias": alias, "_query": query}
        cls = type(f"{cls.__name__}_{alias}", (cls,), initial)
        for name, value in query._values.items():
            if isinstance(value, Field):
                value.bind(cls, name)
            else:
                Field().bind(cls, name)
        return cls
