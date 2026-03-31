from enum import StrEnum
from typing import TYPE_CHECKING, Any

from sql.core.converters import get_converter

if TYPE_CHECKING:
    from sql.models import Model

COLLECTION_TYPES = list, tuple, set


class QueryContext:
    __slots__ = "params", "level"

    def __init__(self, params: list = None, level: int = 0):
        self.params = params if params is not None else []
        self.level = level

    def get_alias(self, model: type[Model]) -> str:
        return model.__sql_alias__(self)

    def add_param(self, value: Any) -> str:
        self.params.append(value)
        return f"${len(self.params)}"

    def sub(self):
        return self.__class__(params=self.params, level=self.level + 1)


class Node:
    relations: set[Model]
    isolated: bool = False

    def __init__(self):
        self.relations = set()

    def _arg(self, value: Any) -> Node:
        if converter := get_converter(type(value)):
            value: Node = converter(value)

        if isinstance(value, Node) and not value.isolated:
            self.relations |= value.relations

        return value

    def _list_arg(self, value: Any) -> list[Node]:
        if value is None:
            return []
        if isinstance(value, COLLECTION_TYPES):
            return [self._arg(a) for a in value]
        return [self._arg(value)]

    def _value(self, value: Any, context: QueryContext) -> str:
        if isinstance(value, Node):
            return value.__sql__(context)

        if value is None:
            return "NULL"

        return context.add_param(value)

    def __sql__(self, context: QueryContext) -> str:
        raise NotImplementedError()

    def prepare(self):
        context = QueryContext()
        yield self.__sql__(context)
        yield from context.params

    def __str__(self):
        return self.__sql__(QueryContext())


class WrappedNodeMixin(Node):
    def __init__(self, wrapped: Node, **kwargs):
        super().__init__(**kwargs)

        self.wrapped: Node = self._arg(wrapped)

    def __sql__(self, context: QueryContext):
        return f"{self.wrapped.__sql__(context)}"


class OrderDirections(StrEnum):
    DESC = "DESC"
    ASC = "ASC"


class OrderBy(WrappedNodeMixin, Node):
    Direction = OrderDirections

    def __init__(
        self, wrapped: Node, direction: OrderDirections, nulls_first: bool = None
    ):
        super().__init__(wrapped=wrapped)

        self.direction, self.nulls_first = direction, nulls_first

    def __sql__(self, context: QueryContext):
        sql = f"{super().__sql__(context)} {self.direction.value}"

        if self.nulls_first is True:
            sql += " NULLS FIRST"
        elif self.nulls_first is False:
            sql += " NULLS LAST"

        return sql
