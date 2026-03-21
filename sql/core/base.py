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
        if isinstance(value, (list, tuple)):
            return [self._arg(a) for a in value]
        return [self._arg(value)]

    def _value(self, value: Any, params: list[Any]) -> str:
        if isinstance(value, Node):
            return value.__sql__(params)

        if value is None:
            return "NULL"

        params.append(value)
        return f"${len(params)}"

    def __sql__(self, params: list[Any]) -> str:
        raise NotImplementedError()

    def prepare(self):
        params = []
        yield self.__sql__(params)
        yield from params

    def __str__(self):
        return self.__sql__([])


class WrappedNodeMixin(typewith(Node)):
    def __init__(self, wrapped: Node, **kwargs):
        super().__init__(**kwargs)

        self.wrapped: Node = self._arg(wrapped)

    def __sql__(self, params: list[Any]):
        return f"{self.wrapped.__sql__(params)}"


class OrderDirections(StrEnum):
    DESC = "DESC"
    ASC = "ASC"


class OrderBy(WrappedNodeMixin, Node):
    def __init__(self, wrapped: Node, direction: OrderDirections):
        super().__init__(wrapped=wrapped)

        self.direction = direction

    def __sql__(self, params: list[Any]):
        return f"{super().__sql__(params)} {self.direction.value}"
