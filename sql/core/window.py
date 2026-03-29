from enum import StrEnum
from typing import TYPE_CHECKING, Any, Self

from sql.core.base import (
    Node,
    OrderBy,
    QueryContext,
)

if TYPE_CHECKING:
    from sql.core.expressions import Expression


class FrameMode(StrEnum):
    ROWS = "ROWS"
    RANGE = "RANGE"
    GROUPS = "GROUPS"


class FrameBound(StrEnum):
    CURRENT = "CURRENT ROW"
    START = "UNBOUNDED PRECEDING"
    END = "UNBOUNDED FOLLOWING"


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
        self._mode = FrameMode.ROWS
        self._frame: tuple[FrameMode, Any, Any] | None = None

    @property
    def rows(self) -> Self:
        self._mode = FrameMode.ROWS
        return self

    @property
    def range(self) -> Self:
        self._mode = FrameMode.RANGE
        return self

    def __getitem__(self, item: slice) -> Self:
        start = FrameBound.START if item.start in (Ellipsis, None) else abs(item.start)
        end = (
            FrameBound.END
            if item.stop is Ellipsis
            else (item.stop or FrameBound.CURRENT)
        )

        self._frame = (self._mode, start, end)
        return self

    def _render_bound(self, bound: Any) -> str:
        if isinstance(bound, int):
            return f"{bound} PRECEDING" if bound > 0 else f"{abs(bound)} FOLLOWING"
        return str(bound)

    def __sql__(self, context: QueryContext) -> str:
        parts = []
        if self._partition_by:
            parts.append(
                f"PARTITION BY {
                    ', '.join(self._value(p, context) for p in self._partition_by)
                }"
            )

        if self._order_by:
            parts.append(
                f"ORDER BY {
                    ', '.join(
                        o.__sql__(context)
                        if isinstance(o, OrderBy)
                        else o.asc().__sql__(context)
                        for o in self._order_by
                    )
                }"
            )

        if self._frame:
            mode, start, end = self._frame
            parts.append(
                f"{mode.value} BETWEEN {self._render_bound(start)}"
                f" AND {self._render_bound(end)}"
            )

        return " ".join(parts)


# class Window(Node):
#     def __init__(
#         self,
#         partition_by: list[Expression] | tuple[Expression] | Expression | None = None,
#         order_by: list[OrderBy | Expression]
#         | tuple[OrderBy | Expression]
#         | OrderBy
#         | None = None,
#     ):
#         super().__init__()

#         self._partition_by: list[Expression] = self._list_arg(partition_by)
#         self._order_by: list[Expression] = self._list_arg(order_by)

#     def __sql__(self, context: QueryContext) -> str:
#         parts = []
#         if self._partition_by:
#             p_sql = ", ".join(self._value(p, context) for p in self._partition_by)
#             parts.append(f"PARTITION BY {p_sql}")

#         if self._order_by:
#             o_sql = ", ".join(
#                 o.__sql__(context)
#                 if isinstance(o, OrderBy)
#                 else o.asc().__sql__(context)
#                 for o in self._order_by
#             )
#             parts.append(f"ORDER BY {o_sql}")

#         return " ".join(parts)
