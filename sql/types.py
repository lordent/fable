from typing import TYPE_CHECKING, Any, TypeVar, cast

T = TypeVar("T")


def with_type[T](cls: type[T]) -> type[T]:
    if TYPE_CHECKING:
        return cls
    return cast(Any, object)
