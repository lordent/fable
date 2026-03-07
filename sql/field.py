from typing import TYPE_CHECKING, Any, Self, TypeVar, cast

from .core import Expr, Q

if TYPE_CHECKING:
    from .model import Model

T = TypeVar("T", bound="Field")


class Field(Expr):
    sql_type: str = "TEXT"

    def __init__(
        self,
        pk: bool = False,
        nullable: bool = True,
        unique: bool = False,
        default: Any = None,
    ):
        super().__init__()
        self.pk = pk
        self.nullable = nullable
        self.unique = unique
        self.default = default
        self.name: str = ""
        self.model: type[Model] = None

    def __set_name__(self, owner: type[Model], name: str):
        self.name, self.model = name, owner
        owner._fields[name] = self

    def __get__(self, instance: Any, owner: type[Model]) -> Self:
        clone = object.__new__(self.__class__)
        clone.__dict__.update(self.__dict__)
        clone.model, clone.relations = owner, {owner}
        return cast(Self, clone)

    def compile(self, params: list[Any]) -> str:
        return f'"{self.model._alias}"."{self.name}"'

    def __repr__(self) -> str:
        return self.compile([])


class SerialField(Field):
    sql_type = "BIGSERIAL"

    def __init__(self, **kwargs):
        kwargs.setdefault("pk", True)
        kwargs.setdefault("nullable", False)
        super().__init__(**kwargs)


class IntField(Field):
    sql_type = "INTEGER"


class BigIntField(Field):
    sql_type = "BIGINT"


class TextField(Field):
    sql_type = "TEXT"

    def __init__(self, threshold=0.3, **kwargs):
        super().__init__(**kwargs)

        self.threshold = threshold

    def __mod__(self, other: Any) -> Q:
        if isinstance(other, tuple):
            val, threshold = other
            return Q("<", self.dist(val), threshold)

        return Q("<", self.dist(other), self.threshold)

    def dist(self, other: Any) -> Expr:
        return Q("<->", self, other)


class JSONBField(Field):
    sql_type = "JSONB"


class TimestampField(Field):
    sql_type = "TIMESTAMP WITH TIME ZONE"

    def __init__(self, auto_now: bool = False, **kwargs):
        super().__init__(**kwargs)

        if auto_now:
            self.default = "NOW()"


class TimeField(Field):
    sql_type = "TIME"

    def between(self, start: Any, end: Any) -> Q:
        return Q("BETWEEN", self, (start, end))


class TimeZoneField(Field):
    sql_type = "TEXT"


class BooleanField(Field):
    sql_type = "BOOLEAN"


class ForeignKey(Field):
    def __init__(self, to: type[Model], on_delete: str = "CASCADE", **kwargs):
        super().__init__(**kwargs)

        self.to = to
        self.on_delete = on_delete
        self.sql_type = "BIGINT"

    def __set_name__(self, owner: type[Model], name: str):
        super().__set_name__(owner, name)

        owner._foreign_fields[name] = self


class ArrayIndex(Expr):
    def __init__(self, field: ArrayField, index: int, relations=None):
        super().__init__(relations)

        self.field = field
        self.index = index

    def compile(self, params: list) -> str:
        field_sql = self.field.compile(params)

        params.append(self.index)
        return f"{field_sql}[${len(params)}]"


class ArrayField(Field):
    def __init__(self, base_field: type[Field], **kwargs):
        super().__init__(**kwargs)

        self.base_field = base_field
        self.sql_type = f"{base_field.sql_type}[]"

    def __ge__(self, other: Any) -> Q:
        return Q("@>", self, other)

    def __le__(self, other: Any) -> Q:
        return Q("<@", self, other)

    def __mul__(self, other: Any) -> Q:
        return Q("&&", self, other)

    def __getitem__(self, index: int) -> Expr:
        return ArrayIndex(self, index, relations=self.relations)
