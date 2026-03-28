from typing import TYPE_CHECKING

from sql.core.base import (
    Node,
    OrderBy,
    QueryContext,
)

if TYPE_CHECKING:
    from sql.core.expressions import Expression


class Window(Node):
    def __init__(
        self,
        partition_by: list[Expression] | tuple[Expression] | Expression | None = None,
        order_by: list[OrderBy | Expression]
        | tuple[OrderBy | Expression]
        | OrderBy
        | None = None,
    ):
        super().__init__()

        self._partition_by: list[Expression] = self._list_arg(partition_by)
        self._order_by: list[Expression] = self._list_arg(order_by)

    def __sql__(self, context: QueryContext) -> str:
        parts = []
        if self._partition_by:
            p_sql = ", ".join(self._value(p, context) for p in self._partition_by)
            parts.append(f"PARTITION BY {p_sql}")

        if self._order_by:
            o_sql = ", ".join(
                o.__sql__(context)
                if isinstance(o, OrderBy)
                else o.asc().__sql__(context)
                for o in self._order_by
            )
            parts.append(f"ORDER BY {o_sql}")

        return " ".join(parts)
