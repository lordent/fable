from collections.abc import Callable
from copy import deepcopy
from enum import StrEnum
from string.templatelib import Template
from typing import TYPE_CHECKING, Literal, Self, TypeVar

from sql.core.base import QueryContext
from sql.core.expressions import Expression, Q, ScalarExpression
from sql.core.types import SqlType, Types
from sql.functions import Concat
from sql.utils import quote_ident

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
        blueprint = deepcopy((args, kwargs))
        instance: Field = type.__call__(cls, *args, **kwargs)
        instance._blueprint = blueprint
        return instance


class Field(ScalarExpression, metaclass=FieldMeta):
    _blueprint: tuple[tuple, dict]
    sql_type: SqlType = Types.TEXT

    def __init__(self, sql_type=None, primary=False):
        super().__init__(sql_type)

        self.primary = primary

    def __set_name__(self, owner: type[Model], name: str):
        self.model, self.name = owner, name
        self.relations.add(self.model)
        owner._fields[name] = self

    def bind(self, owner: type[Model] = None, name: str = None) -> Self:
        args, kwargs = deepcopy(self._blueprint)
        instance = type(self)(*args, **kwargs)
        if owner and name:
            setattr(owner, name, instance)
            instance.__set_name__(owner, name)
        return instance

    def __sql__(self, context: QueryContext) -> str:
        return f"{quote_ident(context.get_alias(self.model))}.{quote_ident(self.name)}"

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

    def __sql__(self, context: QueryContext) -> str:
        return self.expression.__sql__(context)
