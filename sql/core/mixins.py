from __future__ import annotations

from typing import TYPE_CHECKING

from sql.core.base import Node, QueryContext
from sql.typings import typewith

if TYPE_CHECKING:
    pass


class WrappedNodeMixin(typewith(Node)):
    def __init__(self, wrapped: Node, **kwargs):
        super().__init__(**kwargs)

        self.wrapped: Node = self._arg(wrapped)

    def __sql__(self, context: QueryContext):
        return f"{self.wrapped.__sql__(context)}"
