from sql.core.enums import OrderDirections
from sql.core.node import Node, QueryContext


class OrderBy(Node):
    Direction = OrderDirections

    def __init__(
        self, node: Node, direction: OrderDirections, nulls_first: bool = None
    ):
        super().__init__()

        self.node, self.direction, self.nulls_first = node, direction, nulls_first

    def __sql__(self, context: QueryContext):
        sql = f"{self.node.__sql__(context)} {self.direction.value}"

        if self.nulls_first is True:
            sql += " NULLS FIRST"
        elif self.nulls_first is False:
            sql += " NULLS LAST"

        return sql
