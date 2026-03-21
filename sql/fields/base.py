from collections.abc import Callable
from enum import StrEnum
from string.templatelib import Template
from typing import TYPE_CHECKING, Any, Literal, Self, TypeVar, cast

from sql.core.expressions import Concat, Expression, Q
from sql.core.types import SqlType, Types
from sql.fields.factory import FieldFactory

if TYPE_CHECKING:
    from ..models import Model

F = TypeVar("F", bound="Field")


class ReferentialAction(StrEnum):
    CASCADE = "CASCADE"
    RESTRICT = "RESTRICT"
    SET_NULL = "SET NULL"
    SET_DEFAULT = "SET DEFAULT"
    NO_ACTION = "NO ACTION"


class FieldMeta(type):
    def __call__(cls: type[F], *args, **kwargs) -> F:
        return cast(F, FieldFactory(cls, args, kwargs))


class Field(Expression, metaclass=FieldMeta):
    sql_type: SqlType = Types.TEXT

    def __set_name__(self, owner: type[Model], name: str):
        self.model, self.name = owner, name
        self.relations.add(self.model)
        owner._fields[name] = self

    def bind(self, owner: type[Model], name: str) -> Self: ...

    def __sql__(self, params: list[Any]) -> str:
        return f'"{self.model._alias}"."{self.name}"'

    def __hash__(self):
        return hash((self.model, self.name))


class ForeignField[M: type["Model"]](Field):
    CASCADE = ReferentialAction.CASCADE
    RESTRICT = ReferentialAction.RESTRICT
    SET_NULL = ReferentialAction.SET_NULL
    SET_DEFAULT = ReferentialAction.SET_DEFAULT
    NO_ACTION = ReferentialAction.NO_ACTION

    to: M
    sql_type = Types.BIGINT

    def __init__(
        self,
        to: M | Literal["Self"],
        on_delete: ReferentialAction = ReferentialAction.CASCADE,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.to = to
        self.on_delete = on_delete

    def __set_name__(self, owner: M, name: str):
        super().__set_name__(owner, name)

        if self.to == "Self":
            self.to = self.model

        owner._foreign_fields[name] = self


class ComputedField(Field):
    expression: Expression

    def __init__(self, expression: Callable[[], Q | Template] | Q | Template, **kwargs):
        super().__init__(**kwargs)

        self.expression = None
        self._expression = expression

    def __get__(self, instance, owner: type[Model]):
        if not self.expression:
            expression = self._expression
            expression = expression() if callable(expression) else expression

            if isinstance(expression, Template):
                expression = Concat(expression)

            self.sql_type = expression.sql_type or self.sql_type
            self.expression = self._arg(expression)
        return self

    def __sql__(self, params: list[Any]) -> str:
        return self.expression.__sql__(params)
