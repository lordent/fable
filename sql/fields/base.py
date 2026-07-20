from collections.abc import Callable
from enum import StrEnum
from string.templatelib import Template
from typing import TYPE_CHECKING, Literal, TypeVar

from sql.core.datatypes import Types
from sql.core.expressions import Expression
from sql.core.fields import FieldBlueprint, FieldMeta
from sql.core.node import QueryContext
from sql.core.query import Q
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


class Field(Expression, metaclass=FieldMeta):
    __blueprint__: FieldBlueprint
    sql_type = Types.TEXT

    def __init__(self, sql_type=None, primary=False):
        super().__init__(sql_type)

        self.primary = primary

    def __set_name__(self, owner: type[Model], name: str):
        self.contribute_to_model(owner, name)

    def contribute_to_model(self, model: type[Model], name: str):
        self.model, self.name = model, name
        self.relations.add(self.model)
        model._fields[name] = self
        setattr(model, name, self)

    def factory(self, model: type[Model], name: str) -> Field:
        field = self.__blueprint__.factory()
        field.contribute_to_model(model, name)
        return field

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

    def contribute_to_model(self, model: M, name: str):
        super().contribute_to_model(model, name)

        if self.to == "Self":
            self.to = self.model

        model._foreign_fields[name] = self


class ComputedField(Field):
    expression: Expression

    def __init__(self, expression: Callable[[], Q | Template] | Q | Template, **kwargs):
        super().__init__(**kwargs)

        self.expression = None
        self._expression = expression

    def __get__(self, instance: ComputedField, owner: type[Model]):
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
