from typing import TYPE_CHECKING, TypeVar

T = TypeVar("T")


def typewith[T](base_class: type[T]) -> type[T]:
    if TYPE_CHECKING:
        return base_class
    return object
