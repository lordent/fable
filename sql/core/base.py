from enum import StrEnum
from functools import lru_cache
from typing import TYPE_CHECKING, Any

from sql.core.typings import typewith

if TYPE_CHECKING:
    from ..models import Model


_CONVERTERS: dict[type, type[Node]] = {}


def register_converter(from_type: type):
    def wrapper(converter_cls: type[Node]):
        _CONVERTERS[from_type] = converter_cls
        return converter_cls

    return wrapper


@lru_cache
def _get_converter(value_type: type) -> type[Node] | None:
    if converter := _CONVERTERS.get(value_type):
        return converter

    for target_type, converter in _CONVERTERS.items():
        if issubclass(value_type, target_type):
            return converter


class QueryContext:
    def __init__(self, params: list = None, level: int = 0):
        self.params = params if params is not None else []
        self.level = level

    def get_alias(self, model: type[Model]) -> str:
        base_alias = model._alias
        if self.level > 0 and not model._recursive:
            final_alias = f"{base_alias}_s{self.level}"
        else:
            final_alias = base_alias
        return final_alias

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

    def _arg(self, value: Any):
        if converter := _get_converter(type(value)):
            value: Node = converter(value)

        if isinstance(value, Node) and not value.isolated:
            self.relations |= value.relations

        return value

    def _list_arg(self, value: Any):
        if value is None:
            return []
        if isinstance(value, (list, tuple, set)):
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


class WrappedNodeMixin(typewith(Node)):
    def __init__(self, wrapped: Node, **kwargs):
        super().__init__(**kwargs)

        self.wrapped: Node = self._arg(wrapped)

    def __sql__(self, context: QueryContext):
        return f"{self.wrapped.__sql__(context)}"


class OrderDirections(StrEnum):
    DESC = "DESC"
    ASC = "ASC"


class OrderBy(WrappedNodeMixin, Node):
    def __init__(self, wrapped: Node, direction: OrderDirections):
        super().__init__(wrapped=wrapped)

        self.direction = direction

    def __sql__(self, context: QueryContext):
        return f"{super().__sql__(context)} {self.direction.value}"
