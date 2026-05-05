from typing import Any

from sql.core.datatypes import Types
from sql.core.expressions import Expression
from sql.core.node import COLLECTION_TYPES, QueryContext
from sql.core.types import T_SqlType

NEGATION_OPS = {"!=", "NOT IN", "<>"}
ARRAY_OPS = {"=", "IN"} | NEGATION_OPS


class Q(Expression):
    def __init__(self, left: Any, op: str, right: Any, sql_type: T_SqlType = None):
        super().__init__()

        left, self.op, right = self._arg(left), op, self._arg(right)

        self.sql_type = self._get_sql_type(sql_type, left, right)
        self.is_aggregation = self._get_aggregation(left, right)
        self.left, self.right = left, right

    def __sql__(self, context: QueryContext) -> str:
        left, op, right = self.left, self.op, self.right

        if op == "NOT":
            return f"(NOT {context.value(right)})"

        if isinstance(right, COLLECTION_TYPES) and op in ARRAY_OPS:
            if not right:
                return "(1=0)"

            raw_type = Types.TEXT
            if isinstance(left, Expression):
                raw_type = str(left.sql_type or raw_type).replace("[]", "")

            placeholder = context.add_param(list(right))

            new_op = "!= ALL" if op in NEGATION_OPS else "= ANY"

            return f"({context.value(left)} {new_op}({placeholder}::{raw_type}[]))"

        l_sql = context.value(left)

        if op in ("IS NULL", "IS NOT NULL"):
            return f"({l_sql} {op})"

        return f"({l_sql} {op} {context.value(right)})"


Expression.Q = Q
