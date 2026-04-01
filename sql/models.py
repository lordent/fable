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
                for name, field in parent_fields.items():
                    if name not in cls._fields:
                        field.bind(cls, name)

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


class ProxyModel:
    _virtual = True
    _app: Application
    _fields: dict[str, Field] = {}
    _foreign_fields: dict[str, ForeignField[TableModel]] = {}

    def __init__(self, source: type[Model], alias: str):
        self._source = source
        self._alias = alias
        self._fields = {}
        self._foreign_fields = {}

        self._bind_fields()
        self._bind_app()

    def _bind_fields(self):
        for name, field in self._source._fields.items():
            field.bind(self, name)

    def _bind_app(self):
        self._app = self._source._app

    def __sql__(self, context: QueryContext):
        return (
            f"{quote_ident(self._source._source)} "
            f"AS {quote_ident(context.get_alias(self))}"
        )

    def __sql_alias__(self, context: QueryContext):
        base_alias = self._alias
        if context.level > 0:
            return f"{base_alias}_s{context.level}"
        return base_alias

    def __hash__(self) -> int:
        return hash(self._alias)

    def __iter__(self) -> Iterator[Field]:
        for field in self._fields.values():
            yield getattr(self, field.name)


class QueryModel(ProxyModel):
    _source: ValuesQuery

    def __init__(self, source: ValuesQuery, alias: str = None):
        super().__init__(source, alias=alias or f"sub{id(source)}")

    def _bind_fields(self):
        for name, field in self._source._values.items():
            if isinstance(field, Field):
                if field.primary:
                    field = ForeignField(to=field.model)
            else:
                field = Field(
                    sql_type=(
                        field.sql_type if isinstance(field, Expression) else None
                    ),
                )
            field.bind(self, name)

    def _bind_app(self):
        self._app = self._source.app

    def __getitem__(self, alias: str):
        return self.__class__(alias)

    def __sql__(self, context: QueryContext):
        return f"({self._source.__sql__(context.sub())}) AS {quote_ident(self._alias)}"


class RecursiveModel(QueryModel):
    def __iand__(self, other: ValuesQuery) -> Self:
        return self << (self._source & other)

    def __ior__(self: type[RecursiveModel], other: ValuesQuery) -> Self:
        return self << (self._source | other)

    def __lshift__(self: type[RecursiveModel], other: Node) -> Self:
        self._source = other
        return self

    def __sql__(self, context: QueryContext):
        return f"{quote_ident(self._alias)} AS ({self._source.__sql__(context)})"

    def __sql_alias__(self, context: QueryContext):
        return self._alias


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
        return ProxyModel(cls, f"{cls.__name__}_{alias}")

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
