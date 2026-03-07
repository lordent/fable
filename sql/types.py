from typing import TYPE_CHECKING, Any, TypeVar

T = TypeVar("T")


def with_type(cls: Any) -> Any:
    if TYPE_CHECKING:
        return cls
    return object
