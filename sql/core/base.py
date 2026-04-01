from typing import Any

from sql.core.converters import get_converter
from sql.core.types import QueryType, T_Model

COLLECTION_TYPES = list, tuple, set


class QueryContext:
    __slots__ = "params", "level"

    def __init__(self, params: list = None, level: int = 0):
        self.params = params if params is not None else []
        self.level = level

    def get_alias(self, model: T_Model) -> str:
        return model.__sql_alias__(self)

    def add_param(self, value: Any) -> str:
        self.params.append(value)
        return f"${len(self.params)}"

    def sub(self):
        return self.__class__(params=self.params, level=self.level + 1)


class Node:
    relations: set[T_Model]

    def __init__(self):
        self.relations = set()

    def _arg(self, value: Any) -> Node:
        if converter := get_converter(type(value)):
            value: Node = converter(value)

        if isinstance(value, Node) and not isinstance(value, QueryType):
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
