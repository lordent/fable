from functools import lru_cache
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .base import Node

_CONVERTERS: dict[type, type[Node]] = {}


def register_converter(from_type: type):
    def wrapper(converter_cls: type[Node]):
        _CONVERTERS[from_type] = converter_cls
        return converter_cls

    return wrapper


@lru_cache
def get_converter(value_type: type) -> type[Node] | None:
    if converter := _CONVERTERS.get(value_type):
        return converter

    for target_type, converter in _CONVERTERS.items():
        if issubclass(value_type, target_type):
            return converter
