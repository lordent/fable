from collections.abc import Iterator
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Self, TypeVar

from sql.app import Application, get_app_for_module
from sql.core.base import Node, QueryContext
from sql.db import ConnectionManager, TransactionContext
from sql.fields.base import Field, ForeignField
from sql.fields.fields import BigSerialField
from sql.utils import quote_ident

if TYPE_CHECKING:
    from sql.queries.base import ValuesQuery

M = TypeVar("M", bound="Model")


class ModelMeta(type):
    def __new__(mcs, name: str, bases: tuple[type[Model], ...], attrs: dict[str, Any]):
        attrs.setdefault("_table", name.lower())
        attrs.setdefault("_alias", name)
        attrs.setdefault("_app", None)
        attrs["_fields"] = {}
        attrs["_foreign_fields"] = {}

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

    # --- Рекурсивные методы ----

    def __iand__(cls: type[M], other: ValuesQuery):
        return cls << (cls._query & other)

    def __ior__(cls: type[M], other: ValuesQuery):
        return cls << (cls._query | other)

    def __lshift__(cls: type[M], other: Node):
        cls._query = other
        return cls


class Model(metaclass=ModelMeta):
    _app: Application
    _virtual = False
    _table: str
    _alias: str | None = None
    _fields: dict[str, Field] = {}
    _foreign_fields: dict[str, ForeignField[Model]] = {}
    _recursive = False

    id = BigSerialField(primary=True)

    @classmethod
    @lru_cache(maxsize=128)
    def as_alias(cls: type[Self], alias: str) -> type[Self]:
        return type(
            f"{cls.__name__}_{alias}",
            (cls,),
            {"_alias": f"{cls._alias}_{alias}", "_table": cls._table, "_virtual": True},
        )

    @classmethod
    def __sql__(cls: type[Self], context: QueryContext) -> str:
        if cls._recursive:
            return quote_ident(context.get_alias(cls))

        if issubclass(cls, QueryModel):
            return f"({cls._query.__sql__(context.sub())}) AS {quote_ident(cls._alias)}"
        return f"{quote_ident(cls._table)} AS {quote_ident(context.get_alias(cls))}"

    @classmethod
    def atomic(cls, checkpoint: bool = True):
        return TransactionContext(cls._app.name, checkpoint)

    @classmethod
    def connection(cls):
        return ConnectionManager(cls._app.name)


class QueryModel(Model):
    _virtual = True
    _query: ValuesQuery

    @classmethod
    def factory(cls, query: ValuesQuery):
        alias = f"sub{id(query)}"
        initial = {
            "_alias": alias,
            "_query": query,
        }

        if relation := next(iter(query.relations), None):
            initial["_app"] = relation._app

        cls = type(f"{cls.__name__}_{alias}", (cls,), initial)
        for name, value in query._values.items():
            if isinstance(value, Field):
                value.bind(cls, name)
            else:
                Field().bind(cls, name)
        return cls
