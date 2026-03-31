from collections.abc import Iterator
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Self, TypeVar

from sql.app import Application, get_app_for_module
from sql.core.base import Node, QueryContext
from sql.core.expressions import Expression
from sql.db import ConnectionManager, TransactionContext
from sql.fields.base import Field, ForeignField
from sql.fields.fields import BigSerialField
from sql.utils import quote_ident

if TYPE_CHECKING:
    from sql.queries.base import ValuesQuery

M = TypeVar("M", bound="Model")


class ModelMeta(type):
    def __new__(mcs, name: str, bases: tuple[type[Model], ...], attrs: dict[str, Any]):
        attrs.setdefault("_virtual", False)
        attrs.setdefault("_source", name.lower())
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
        return cls._source == other._source and cls._alias == other._alias


class QueryModelMeta(ModelMeta):
    def __new__(mcs, name, bases, attrs):
        attrs["_virtual"] = True

        return super().__new__(mcs, name, bases, attrs)


class RecursiveModelMeta(QueryModelMeta):
    def __iand__(cls: type[RecursiveModel], other: ValuesQuery) -> type[RecursiveModel]:
        return cls << (cls._source & other)

    def __ior__(cls: type[RecursiveModel], other: ValuesQuery) -> type[RecursiveModel]:
        return cls << (cls._source | other)

    def __lshift__(cls: type[RecursiveModel], other: Node) -> type[RecursiveModel]:
        cls._source = other
        return cls


class Model(metaclass=ModelMeta):
    _app: Application
    _virtual = False
    _source: str
    _alias: str | None = None
    _fields: dict[str, Field] = {}
    _foreign_fields: dict[str, ForeignField[TableModel]] = {}

    @classmethod
    @lru_cache(maxsize=128)
    def as_alias(cls: type[Self], alias: str) -> type[Self]:
        return type(
            f"{cls.__name__}_{alias}",
            (cls,),
            {
                "_alias": f"{cls._alias}_{alias}",
                "_source": cls._source,
                "_virtual": True,
                "__module__": cls.__module__,
            },
        )

    @classmethod
    def __sql__(cls: type[Self], context: QueryContext):
        return f"{quote_ident(cls._source)} AS {quote_ident(context.get_alias(cls))}"

    @classmethod
    def __sql_alias__(cls: type[Self], context: QueryContext):
        base_alias = cls._alias
        if context.level > 0:
            return f"{base_alias}_s{context.level}"
        return base_alias

    @classmethod
    def atomic(cls, checkpoint: bool = True):
        return TransactionContext(cls._app.name, checkpoint)

    @classmethod
    def connection(cls):
        return ConnectionManager(cls._app.name)


class TableModel(Model):
    _virtual = True

    id = BigSerialField(primary=True)


class QueryModel(Model, metaclass=QueryModelMeta):
    _source: ValuesQuery

    @classmethod
    def factory(cls: type[Self], source: ValuesQuery):
        alias = f"sub{id(source)}"
        initial = {
            "_alias": alias,
            "_source": source,
        }

        for relation in source.relations:
            if app := relation._app:
                initial["_app"] = app
                initial["__module__"] = relation.__module__
                break

        cls = type(f"{cls.__name__}_{alias}", (cls,), initial)
        for name, value in source._values.items():
            if isinstance(value, Field):
                if value.primary:
                    value = ForeignField(to=value.model)
                value.bind(cls, name)
            else:
                Field(
                    sql_type=(
                        value.sql_type if isinstance(value, Expression) else None
                    ),
                ).bind(cls, name)

        return cls

    @classmethod
    def __sql__(cls: type[Self], context: QueryContext):
        return f"({cls._source.__sql__(context.sub())}) AS {quote_ident(cls._alias)}"


class RecursiveModel(QueryModel, metaclass=RecursiveModelMeta):
    @classmethod
    def __sql__(cls: type[Self], context: QueryContext):
        return f"{quote_ident(cls._alias)} AS ({cls._source.__sql__(context)})"

    @classmethod
    def __sql_alias__(cls: type[Self], context: QueryContext):
        return cls._alias
