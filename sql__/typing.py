from collections.abc import Mapping
from typing import TYPE_CHECKING, Literal, Protocol, Union, runtime_checkable

if TYPE_CHECKING:
    from .model import ModelManager
    from .query import Q


@runtime_checkable
class Compilable(Protocol):
    def compile(self, args: list[object]) -> str: ...


SQLExpression = Union["Q", str, int, float, bool, None]
ColumnValue = Mapping[str, SQLExpression]
Direction = Literal["up", "down"]
Dependency = Union[type["ModelManager"], "Q", Compilable]
