from enum import StrEnum
from types import EllipsisType
from typing import TYPE_CHECKING, Any, Literal, Self, overload

from sql.core.base import COLLECTION_TYPES, Node, OrderBy, QueryContext
from sql.core.types import SqlType, Types
from sql.typings import typewith

if TYPE_CHECKING:
    from sql.fields.base import Field


type T_SqlType = SqlType | Field | None


def q(
    left: Expression, op: str, right: Expression, sql_type: T_SqlType = None
) -> Q | AggregateQ:
    return next(
        (AggregateQ for a in (left, right) if isinstance(a, AggregateExpression)),
        Q,
    )(left, op, right, sql_type=sql_type)


class Expression(Node):
    sql_type: T_SqlType = None

    def __init__(self, sql_type: str = None):
        super().__init__()

        self.sql_type = sql_type or self.sql_type

    # --- Подсветка типов ---

    @overload
    def __add__(self: AggregateExpression, other: Any) -> AggregateQ: ...
    @overload
    def __add__(self: Any, other: AggregateExpression) -> AggregateQ: ...
    @overload
    def __add__(self: ScalarExpression, other: Any) -> Q: ...

    @overload
    def __sub__(self: AggregateExpression, other: Any) -> AggregateQ: ...
    @overload
    def __sub__(self: Any, other: AggregateExpression) -> AggregateQ: ...
    @overload
    def __sub__(self: ScalarExpression, other: Any) -> Q: ...

    @overload
    def __mul__(self: AggregateExpression, other: Any) -> AggregateQ: ...
    @overload
    def __mul__(self: Any, other: AggregateExpression) -> AggregateQ: ...
    @overload
    def __mul__(self: ScalarExpression, other: Any) -> Q: ...

    @overload
    def __truediv__(self: AggregateExpression, other: Any) -> AggregateQ: ...
    @overload
    def __truediv__(self: Any, other: AggregateExpression) -> AggregateQ: ...
    @overload
    def __truediv__(self: ScalarExpression, other: Any) -> Q: ...

    @overload
    def __mod__(self: AggregateExpression, other: Any) -> AggregateQ: ...
    @overload
    def __mod__(self: Any, other: AggregateExpression) -> AggregateQ: ...
    @overload
    def __mod__(self: ScalarExpression, other: Any) -> Q: ...

    @overload
    def __eq__(self: AggregateExpression, other: Any) -> AggregateQ: ...
    @overload
    def __eq__(self: Any, other: AggregateExpression) -> AggregateQ: ...
    @overload
    def __eq__(self: ScalarExpression, other: Any) -> Q: ...

    @overload
    def __ne__(self: AggregateExpression, other: Any) -> AggregateQ: ...
    @overload
    def __ne__(self: Any, other: AggregateExpression) -> AggregateQ: ...
    @overload
    def __ne__(self: ScalarExpression, other: Any) -> Q: ...

    @overload
    def __gt__(self: AggregateExpression, other: Any) -> AggregateQ: ...
    @overload
    def __gt__(self: Any, other: AggregateExpression) -> AggregateQ: ...
    @overload
    def __gt__(self: ScalarExpression, other: Any) -> Q: ...

    @overload
    def __ge__(self: AggregateExpression, other: Any) -> AggregateQ: ...
    @overload
    def __ge__(self: Any, other: AggregateExpression) -> AggregateQ: ...
    @overload
    def __ge__(self: ScalarExpression, other: Any) -> Q: ...

    @overload
    def __lt__(self: AggregateExpression, other: Any) -> AggregateQ: ...
    @overload
    def __lt__(self: Any, other: AggregateExpression) -> AggregateQ: ...
    @overload
    def __lt__(self: ScalarExpression, other: Any) -> Q: ...

    @overload
    def __le__(self: AggregateExpression, other: Any) -> AggregateQ: ...
    @overload
    def __le__(self: Any, other: AggregateExpression) -> AggregateQ: ...
    @overload
    def __le__(self: ScalarExpression, other: Any) -> Q: ...

    @overload
    def __and__(self: AggregateExpression, other: Any) -> AggregateQ: ...
    @overload
    def __and__(self: Any, other: AggregateExpression) -> AggregateQ: ...
    @overload
    def __and__(self: ScalarExpression, other: Any) -> Q: ...

    @overload
    def __or__(self: AggregateExpression, other: Any) -> AggregateQ: ...
    @overload
    def __or__(self: Any, other: AggregateExpression) -> AggregateQ: ...
    @overload
    def __or__(self: ScalarExpression, other: Any) -> Q: ...

    @overload
    def __invert__(self: AggregateExpression, other: Any) -> AggregateQ: ...
    @overload
    def __invert__(self: Any, other: AggregateExpression) -> AggregateQ: ...
    @overload
    def __invert__(self: ScalarExpression, other: Any) -> Q: ...

    # -- Преобразование типов ---

    @overload
    def cast(self: AggregateExpression, to: T_SqlType) -> AggregateCast: ...

    @overload
    def cast(self: ScalarExpression, to: T_SqlType) -> Cast: ...

    def cast(self, to: T_SqlType) -> Cast | AggregateCast:
        return (AggregateCast if isinstance(self, AggregateExpression) else Cast)(
            self, to
        )

    @overload
    def __rshift__(self: AggregateExpression, to: T_SqlType) -> AggregateCast: ...

    @overload
    def __rshift__(self: ScalarExpression, to: T_SqlType) -> Cast: ...

    def __rshift__(self, to: T_SqlType) -> Cast | AggregateCast:
        return self.cast(to)

    # -- Таймзона ---

    @overload
    def at_timezone(
        self: AggregateExpression, zone: str | Expression
    ) -> AggregateAtTimeZone: ...

    @overload
    def at_timezone(self: ScalarExpression, zone: str | Expression) -> AtTimeZone: ...

    def at_timezone(self, zone: str | Expression) -> AtTimeZone | AggregateAtTimeZone:
        return (
            AggregateAtTimeZone if isinstance(self, AggregateExpression) else AtTimeZone
        )(self, zone)

    # -- Coalesce ---

    @overload
    def default(self: AggregateExpression, other: Any) -> AggregateCoalesce: ...

    @overload
    def default(
        self: ScalarExpression, other: AggregateExpression
    ) -> AggregateCoalesce: ...

    @overload
    def default(self: ScalarExpression, other: Any) -> Coalesce: ...

    def default(self, other: Any) -> Coalesce | AggregateCoalesce:
        return (
            AggregateCoalesce
            if isinstance(self, AggregateExpression)
            or isinstance(other, AggregateExpression)
            else Coalesce
        )(self, other, sql_type=self.sql_type)

    # --- Сортировка ---

    def asc(self, nulls_first=None) -> OrderBy:
        return OrderBy(self, OrderBy.Direction.ASC, nulls_first)

    def desc(self, nulls_first=None) -> OrderBy:
        return OrderBy(self, OrderBy.Direction.DESC, nulls_first)

    # --- Логические операции ---

    def __and__(self, other: Any) -> Q | AggregateQ:
        return q(self, "AND", other, sql_type=Types.BOOLEAN)

    def __or__(self, other: Any) -> Q | AggregateQ:
        return q(self, "OR", other, sql_type=Types.BOOLEAN)

    def __invert__(self) -> Q | AggregateQ:
        return q(None, "NOT", self, sql_type=Types.BOOLEAN)

    # --- Сравнение ---

    def __eq__(self, other: Any) -> Q | AggregateQ:
        if other is None:
            return q(self, "IS NULL", None, sql_type=Types.BOOLEAN)
        if isinstance(other, COLLECTION_TYPES):
            return q(self, "IN", other, sql_type=Types.BOOLEAN)
        return q(self, "=", other, sql_type=Types.BOOLEAN)

    def __ne__(self, other: Any) -> Q | AggregateQ:
        if other is None:
            return q(self, "IS NOT NULL", None)
        return q(self, "!=", other, sql_type=Types.BOOLEAN)

    def __lt__(self, other: Any) -> Q | AggregateQ:
        return q(self, "<", other, sql_type=Types.BOOLEAN)

    def __le__(self, other: Any) -> Q | AggregateQ:
        return q(self, "<=", other, sql_type=Types.BOOLEAN)

    def __gt__(self, other: Any) -> Q | AggregateQ:
        return q(self, ">", other, sql_type=Types.BOOLEAN)

    def __ge__(self, other: Any) -> Q | AggregateQ:
        return q(self, ">=", other, sql_type=Types.BOOLEAN)

    # --- Арифметика ---

    def __add__(self, other: Any) -> Q | AggregateQ:
        return q(self, "+", other)

    def __sub__(self, other: Any) -> Q | AggregateQ:
        return q(self, "-", other)

    def __mul__(self, other: Any) -> Q | AggregateQ:
        return q(self, "*", other)

    def __truediv__(self, other: Any) -> Q | AggregateQ:
        return q(self, "/", other)

    def __mod__(self, other: Any) -> Q | AggregateQ:
        return q(self, "%", other)

    # --- Массивы и JSON ---

    def contains(self, other: Any) -> Q | AggregateQ:
        return q(
            self,
            "@>",
            [other] if not isinstance(other, COLLECTION_TYPES) else other,
            sql_type=Types.BOOLEAN,
        )

    def overlap(self, other: Any) -> Q | AggregateQ:
        return q(self, "&&", other, sql_type=Types.BOOLEAN)

    def __getitem__(self, key: str | int) -> Q | AggregateQ:
        return q(self, "->", str(key), sql_type=Types.JSONB)

    def text(self, key: str | int) -> Q | AggregateQ:
        return q(self, "->>", str(key), sql_type=Types.TEXT)

    # --- Разное ---

    def dist(self, other: Any) -> Q | AggregateQ:
        return q(self, "<->", other, sql_type=Types.DOUBLE_PRECISION)


# -- Скаляры --


class ScalarExpression(Expression):
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

        if isinstance(right, (list, tuple, set)) and op in ARRAY_OPS:
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

        if isinstance(right, Node) and right.isolated:
            r_sql = f"({right.__sql__(context)})"
        else:
            r_sql = self._value(right, context)

        return f"({l_sql} {op} {r_sql})"


class FuncMixin(typewith(Expression)):
    name: str = None

    def __init__(self, *args: Any, sql_type: T_SqlType = None):
        super().__init__(sql_type=sql_type)

        self.args = self._list_arg(args)

    def __sql_args__(self, context: QueryContext, prefix: str = "") -> str:
        args_sql = ", ".join(self._value(a, context) for a in self.args)
        return f"{self.name or self.__class__.__name__.upper()}({prefix}{args_sql})"


class ScalarFunc(FuncMixin, ScalarExpression):
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


# --- Агрегаты ----


class AggregateExpression(Expression):
    def filter(self, condition: Expression) -> FilteredAggregate:
        return FilteredAggregate(self, condition)

    def over(
        self,
        partition_by: list[Expression] = None,
        order_by: list[OrderBy] = None,
    ) -> WindowExpression:
        return WindowExpression(self, partition_by, order_by)


class AggregateQ(AggregateExpression, Q):
    pass


class AggregateCoalesce(AggregateExpression, Coalesce):
    pass


class AggregateCast(AggregateExpression, Cast):
    pass


class AggregateAtTimeZone(AggregateExpression, AtTimeZone):
    pass


class AggregateFunc(FuncMixin, AggregateExpression):
    def __init__(
        self,
        *args: Any,
        distinct: bool = False,
        sql_type: T_SqlType = None,
    ):
        super().__init__(*args, sql_type=sql_type)

        self.distinct = distinct

    def __sql__(self, context: QueryContext) -> str:
        return self.__sql_args__(context, "DISTINCT " if self.distinct else "")


class UnaryAggregate(AggregateFunc):
    def __init__(
        self, expression: Expression, distinct: bool = False, sql_type: T_SqlType = None
    ):
        super().__init__(expression, distinct=distinct, sql_type=sql_type)


class FilteredAggregate(AggregateExpression):
    def __init__(self, expression: AggregateExpression, condition: Expression):
        super().__init__()

        self.expression, self.condition = self._arg(expression), self._arg(condition)

    def __sql__(self, context: QueryContext) -> str:
        agg_sql = self.expression.__sql__(context)
        cond_sql = self.condition.__sql__(context)
        return f"{agg_sql} FILTER (WHERE {cond_sql})"


class FrameMode(StrEnum):
    ROWS = "ROWS"
    RANGE = "RANGE"
    GROUPS = "GROUPS"


class FrameBound(StrEnum):
    CURRENT = "CURRENT ROW"
    START = "UNBOUNDED PRECEDING"
    END = "UNBOUNDED FOLLOWING"


class WindowExpression(ScalarExpression):
    def __init__(
        self,
        expression: AggregateExpression,
        partition_by: Expression | list[Expression] = None,
        order_by: OrderBy | list[OrderBy] | Expression | list[Expression] = None,
    ):
        super().__init__(sql_type=expression.sql_type)

        self.expression = self._arg(expression)
        self.partition_by: list[Expression] = self._list_arg(partition_by)
        self.order_by: list[Expression] = self._list_arg(order_by)
        self._mode = FrameMode.ROWS
        self._frame: (
            tuple[
                FrameMode,
                int | Literal[FrameBound.START],
                int | Literal[FrameBound.END, FrameBound.CURRENT],
            ]
            | None
        ) = None

    @property
    def rows(self) -> Self:
        self._mode = FrameMode.ROWS
        return self

    @property
    def range(self) -> Self:
        self._mode = FrameMode.RANGE
        return self

    def __getitem__(
        self, item: slice[int | EllipsisType | None, int | EllipsisType | None]
    ) -> Self:
        start = FrameBound.START if item.start in (Ellipsis, None) else item.start
        stop = (
            FrameBound.END
            if item.stop is Ellipsis
            else (item.stop if item.stop is not None else FrameBound.CURRENT)
        )
        self._frame = (self._mode, start, stop)
        return self

    def __sql_bound__(self, bound: Any) -> str:
        if isinstance(bound, int):
            if bound == 0:
                return str(FrameBound.CURRENT)
            return f"{abs(bound)} PRECEDING" if bound > 0 else f"{abs(bound)} FOLLOWING"
        return str(bound)

    def __sql__(self, context: QueryContext) -> str:
        agg_sql = self.expression.__sql__(context)

        parts = []

        if self.partition_by:
            parts.append(
                f"PARTITION BY {
                    (', '.join(p.__sql__(context) for p in self.partition_by))
                }"
            )

        if self.order_by:
            parts.append(
                f"ORDER BY {
                    (
                        ', '.join(
                            o.__sql__(context)
                            if isinstance(o, OrderBy)
                            else o.asc().__sql__(context)
                            for o in self.order_by
                        )
                    )
                }"
            )

        if self._frame:
            mode, start, end = self._frame
            parts.append(
                f"{mode.value} BETWEEN {self.__sql_bound__(start)} "
                f"AND {self.__sql_bound__(end)}"
            )

        spec_sql = f"({' '.join(parts)})" if parts else "()"
        return f"{agg_sql} OVER {spec_sql}"
