from collections.abc import Iterator
from copy import copy
from typing import TYPE_CHECKING, Any, Self

from .field import Field
from .typing import SQLExpression

if TYPE_CHECKING:
    from .insert import Insert
    from .query import Q
    from .select import Select


class MetaManager(type):
    def __new__(
        mcs, name: str, bases: tuple[type, ...], initial: dict[str, Any]
    ) -> type:
        cls: Meta = super().__new__(mcs, name, bases, initial)
        if "model" in initial:
            cls.fields = {}

            model_name = cls.model.__name__.lower()
            table_name = getattr(cls, "table_name", None) or model_name
            alias = getattr(cls, "alias", None) or table_name

            cls.table_name = table_name
            cls.alias = alias
        return cls


class Meta(metaclass=MetaManager):
    alias: str
    table_name: str
    fields: dict[str, Field]
    model: type[Model]
    is_virtual: bool = False


class ModelManager(type):
    models: set[type[Model]] = set()

    def __new__(
        mcs, name: str, bases: tuple[type, ...], initial: dict[str, Any]
    ) -> type:
        if not bases:
            return super().__new__(mcs, name, bases, initial)

        alias: str = initial.pop("__alias__", "")
        cls: Model = super().__new__(mcs, name, bases, initial)

        base_meta = getattr(cls, "Meta", Meta)
        cls.Meta = MetaManager(
            "Meta",
            (base_meta, Meta),
            {
                "model": cls,
                "alias": alias or getattr(base_meta, "alias", None),
                "table_name": getattr(base_meta, "table_name", None),
            },
        )

        if not alias:
            mcs.models.add(cls)

        for base in reversed(cls.__mro__):
            for key, value in base.__dict__.items():
                if isinstance(value, Field):
                    bind_field(cls, key, value)

        return cls

    def __iter__(cls: type[Model]) -> Iterator[Field]:
        return iter(cls.Meta.fields.values())

    def __getitem__(cls: type[Model], alias: str) -> Self:
        new_cls = type(
            cls.__name__,
            (cls,),
            {
                "__alias__": alias,
            },
        )
        for name, field in cls.Meta.fields.items():
            bind_field(new_cls, name, field)
        return new_cls

    def __str__(cls: type[Model]) -> str:
        m = cls.Meta
        if m.alias and m.alias != m.table_name:
            return f'"{m.table_name}" "{m.alias}"'
        return f'"{m.table_name}"'

    def __hash__(cls: type[Model]) -> int:
        return hash((cls.Meta.table_name, cls.Meta.alias))

    def __eq__(cls: type[Model], other: type[Model]) -> bool:
        if isinstance(other, ModelManager):
            return (
                cls.Meta.table_name == other.Meta.table_name
                and cls.Meta.alias == other.Meta.alias
            )
        return False

    @staticmethod
    def virtual(name: str, fields: dict[str, Field]) -> Self:
        new_cls = type(
            name,
            (Model,),
            {
                "__alias__": name,
                "Meta": type(
                    "Meta",
                    (),
                    {
                        "table_name": name,
                        "alias": name,
                        "fields": {},
                        "is_virtual": True,
                    },
                ),
            },
        )

        for f_name, field in fields.items():
            bind_field(new_cls, f_name, field)

        return new_cls


class Model(metaclass=ModelManager):
    class Meta(Meta):
        pass

    id = Field(column_type="serial", nullable=False, primary=True)

    @classmethod
    def select(cls, *args: Q, **kwargs: SQLExpression) -> Select:
        from .select import Select

        s = Select(*args, **kwargs)
        s.dependencies.add(cls)
        return s

    @classmethod
    def insert(cls, **values: SQLExpression) -> Insert:
        from .insert import Insert

        return Insert(cls, **values)


def bind_field(cls: type[Model], name: str, field: Field) -> Field:
    new_field = copy(field)
    new_field.table = cls
    new_field.name = new_field.name or name
    new_field.dependencies = {cls}

    if hasattr(new_field, "to") and new_field.to == "Self":
        new_field.to = cls

    setattr(cls, name, new_field)
    cls.Meta.fields[name] = new_field
    return new_field
