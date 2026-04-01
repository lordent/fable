from sql.core.base import Node, QueryContext
from sql.core.enums import OrderDirections
from sql.core.mixins import WrappedNodeMixin


class OrderBy(WrappedNodeMixin, Node):
    Direction = OrderDirections

    def __init__(
        self, wrapped: Node, direction: OrderDirections, nulls_first: bool = None
    ):
        super().__init__(wrapped=wrapped)

        self.direction, self.nulls_first = direction, nulls_first

    def __sql__(self, context: QueryContext):
        sql = f"{super().__sql__(context)} {self.direction.value}"

        if self.nulls_first is True:
            sql += " NULLS FIRST"
        elif self.nulls_first is False:
            sql += " NULLS LAST"

        return sql
