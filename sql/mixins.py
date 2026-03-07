from typing import TYPE_CHECKING, Self

from .core import Expr
from .field import Field

if TYPE_CHECKING:
    pass


class SelectValuesMixin:
    def __init__(self, *args: Field, **kwargs: Expr):
        super().__init__()

        self._values: dict[str, Expr] = {}

    def values(self, *args: Field, **kwargs: Expr) -> Self:
        for f in args:
            self._values[f.name] = f
            self.relations |= f.relations
        for k, v in kwargs.items():
            self._values[k] = v
            if isinstance(v, Expr):
                self.relations |= v.relations
        return self
