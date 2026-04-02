from typing import Any

from sql.core.base import COLLECTION_TYPES, Node, QueryContext
from sql.core.expressions import Expression
from sql.core.types import QueryType, ScalarType, T_SqlType, Types


class ScalarExpression(ScalarType, Expression):
    pass


class Cast(ScalarExpression):
    def __init__(self, expression: Expression, to: T_SqlType):
        if isinstance(to, Expression):
            sql_type = to.sql_type
        else:
            sql_type = to

        super().__init__(sql_type=sql_type)

        self.expression = self._arg(expression)

    def __sql__(self, context: QueryContext):
        return f"({self.expression.__sql__(context)})::{self.sql_type}"


NEGATION_OPS = {"!=", "NOT IN", "<>"}
ARRAY_OPS = {"=", "IN"} | NEGATION_OPS


class Q(ScalarExpression):
    def __init__(self, left: Any, op: str, right: Any, sql_type: T_SqlType = None):
        super().__init__(sql_type=sql_type)

        self.left, self.op, self.right = self._arg(left), op, self._arg(right)

        if not sql_type:
            self.sql_type = self._sql_type()

    def _sql_type(self):
        left_sql_type, right_sql_type = None, None
        if isinstance(self.left, Expression):
            left_sql_type = self.left.sql_type
        if isinstance(self.right, Expression):
            right_sql_type = self.right.sql_type
        return left_sql_type or right_sql_type

    def __sql__(self, context: QueryContext) -> str:
        left, op, right = self.left, self.op, self.right

        if isinstance(right, COLLECTION_TYPES) and op in ARRAY_OPS:
            if not right:
                return "(1=0)"

            l_sql = self._value(left, context)
            raw_type = Types.TEXT
            if isinstance(left, Expression):
                raw_type = str(left.sql_type or raw_type).replace("[]", "")

            placeholder = context.add_param(list(right))

            new_op = "!= ALL" if op in NEGATION_OPS else "= ANY"

            return f"({l_sql} {new_op}({placeholder}::{raw_type}[]))"

        l_sql = self._value(left, context)

        if op in ("IS NULL", "IS NOT NULL"):
            return f"({l_sql} {op})"

        if isinstance(right, Node) and isinstance(right, QueryType):
            r_sql = f"({right.__sql__(context)})"
        else:
            r_sql = self._value(right, context)

        return f"({l_sql} {op} {r_sql})"


class Func(ScalarExpression):
    name: str = None

    def __init__(self, *args: Any, sql_type: T_SqlType = None):
        super().__init__(sql_type=sql_type)

        self.args = self._list_arg(args)

    def __sql_args__(self, context: QueryContext, prefix: str = "") -> str:
        args_sql = ", ".join(self._value(a, context) for a in self.args)
        return f"{self.name or self.__class__.__name__.upper()}({prefix}{args_sql})"


class ScalarFunc(Func):
    def __sql__(self, context: QueryContext) -> str:
        return self.__sql_args__(context)


class Coalesce(ScalarFunc):
    name = "COALESCE"


class AtTimeZone(ScalarExpression):
    def __init__(
        self,
        expression: Expression,
        zone: str | Expression,
    ):
        super().__init__(sql_type=expression.sql_type)

        self.expression, self.zone = self._arg(expression), self._arg(zone)

    def __sql__(self, context: QueryContext):
        return (
            f"({self.expression.__sql__(context)} "
            f"AT TIME ZONE {self._value(self.zone, context)})"
        )


ScalarType.Q = Q
ScalarType.Cast = Cast
ScalarType.Coalesce = Coalesce
ScalarType.AtTimeZone = AtTimeZone
