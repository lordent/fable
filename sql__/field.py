from typing import TYPE_CHECKING

from .query import Column, Q
from .typing import SQLExpression

if TYPE_CHECKING:
    from .model import Model


class Field(Column):
    def __init__(
        self,
        name: str | None = None,
        column_type: str = "text",
        primary: bool = False,
        nullable: bool = True,
        unique: bool = False,
        default: SQLExpression = None,
    ):
        self.name = name
        self.column_type = column_type
        self.primary = primary
        self.nullable = nullable
        self.unique = unique
        self.default = default
        self.table: Model = None

        super().__init__("")


class TextField(Field):
    def __init__(self, **kwargs):
        super().__init__(column_type="text", **kwargs)

    def __mod__(self, val: SQLExpression | tuple[SQLExpression, float]) -> Q:
        if isinstance(val, tuple):
            value, distance = val
            return Q(
                f"({self.get_sql([])} <-> {{}} < {{}})",
                value,
                distance,
                _dependencies=self.dependencies,
            )

        return self.__operand__("%", val)

    def ilike(self, val: SQLExpression) -> Q:
        return self.__operand__("ILIKE", val)


class IntegerField(Field):
    def __init__(self, **kwargs):
        super().__init__(column_type="integer", **kwargs)


class PrimaryKey(Field):
    def __init__(self, **kwargs):
        kwargs.setdefault("primary", True)
        kwargs.setdefault("nullable", False)
        super().__init__(column_type="serial", **kwargs)


class LTreeField(Field):
    def __init__(self, **kwargs):
        super().__init__(column_type="ltree", **kwargs)

    def __matmul__(self, val: SQLExpression) -> Q:
        return self.__operand__("@>", val)

    def __lshift__(self, val: SQLExpression) -> Q:
        return self.__operand__("<@", val)

    def __pow__(self, val: SQLExpression) -> Q:
        return self.__operand__("~", val)


class JSONBField(Field):
    def __init__(self, **kwargs):
        super().__init__(column_type="jsonb", **kwargs)

    def __getitem__(self, key: str | int) -> Q:
        return Q(f"{self.get_sql([])} ->> {{}}", key, _dependencies=self.dependencies)

    def contains(self, val: dict | list) -> Q:
        return self.__operand__("@>", val)


class ForeignKey(IntegerField):
    def __init__(self, to: type[Model] | str, **kwargs):
        self.to = to
        super().__init__(**kwargs)
