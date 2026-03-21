from itertools import zip_longest
from string.templatelib import Template
from typing import TYPE_CHECKING, Any

from sql.core.base import (
    Node,
    OrderBy,
    OrderDirections,
    WrappedNodeMixin,
    register_converter,
)
from sql.core.types import SqlType, Types
from sql.fields.factory import FieldFactory

if TYPE_CHECKING:
    from sql.fields.base import Field


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

    def __sql__(self, params: list[Any]) -> str:
        parts = []
        if self._partition_by:
            p_sql = ", ".join(self._value(p, params) for p in self._partition_by)
            parts.append(f"PARTITION BY {p_sql}")

        if self._order_by:
            o_sql = ", ".join(
                o.__sql__(params) if isinstance(o, OrderBy) else o.asc().__sql__(params)
                for o in self._order_by
            )
            parts.append(f"ORDER BY {o_sql}")

        return " ".join(parts)


class Expression(Node):
    sql_type: SqlType | None = None
    is_aggregate = False
    is_windowed = False

    def __init__(
        self,
        sql_type: SqlType | None = None,
        is_aggregate=False,
        is_windowed=False,
    ):
        super().__init__()

        self.sql_type = sql_type or self.sql_type
        self.is_aggregate = is_aggregate
        self.is_windowed = is_windowed

    def _arg(self, value: Any):
        value = super()._arg(value)

        if isinstance(value, Expression):
            self.is_aggregate |= value.is_aggregate
            self.is_windowed |= value.is_windowed

        return value

    # --- Логические операции ---

    def __and__(self, other: Any) -> Q:
        return Q(self, "AND", other, sql_type=Types.BOOLEAN)

    def __or__(self, other: Any) -> Q:
        return Q(self, "OR", other, sql_type=Types.BOOLEAN)

    def __invert__(self) -> Q:
        return Q(None, "NOT", self, sql_type=Types.BOOLEAN)

    # --- Сравнение ---

    def __eq__(self, other: Any) -> Q:
        if other is None:
            return Q(self, "IS NULL", None)
        if isinstance(other, (list, tuple, set)):
            return Q(self, "IN", other)
        return Q(self, "=", other, sql_type=Types.BOOLEAN)

    def __ne__(self, other: Any) -> Q:
        if other is None:
            return Q(self, "IS NOT NULL", None)
        return Q(self, "!=", other, sql_type=Types.BOOLEAN)

    def __lt__(self, other: Any) -> Q:
        return Q(self, "<", other, sql_type=Types.BOOLEAN)

    def __le__(self, other: Any) -> Q:
        return Q(self, "<=", other, sql_type=Types.BOOLEAN)

    def __gt__(self, other: Any) -> Q:
        return Q(self, ">", other, sql_type=Types.BOOLEAN)

    def __ge__(self, other: Any) -> Q:
        return Q(self, ">=", other, sql_type=Types.BOOLEAN)

    # --- Арифметика ---

    def __add__(self, other: Any) -> Q:
        return Q(self, "+", other, sql_type=self.sql_type)

    def __sub__(self, other: Any) -> Q:
        return Q(self, "-", other, sql_type=self.sql_type)

    def __mul__(self, other: Any) -> Q:
        return Q(self, "*", other, sql_type=self.sql_type)

    def __truediv__(self, other: Any) -> Q:
        return Q(self, "/", other, sql_type=self.sql_type)

    def __mod__(self, other: Any) -> Q:
        return Q(self, "%", other, sql_type=self.sql_type)

    # --- Сортировка ---

    def asc(self) -> OrderBy:
        return OrderBy(self, OrderDirections.ASC)

    def desc(self) -> OrderBy:
        return OrderBy(self, OrderDirections.DESC)

    # --- Приведение типов ---

    def cast(self, to: str | SqlType | type[Field] | Field):
        return Cast(self, to)

    def __rshift__(self, to: str | SqlType | type[Field] | Field):
        return self.cast(to)

    def at_timezone(self, zone: str | Expression):
        return AtTimeZone(self, zone)

    # --- Разное ---

    def dist(self, other: Any) -> Q:
        return Q(self, "<->", other, sql_type=Types.DOUBLE_PRECISION)


class Q(Expression):
    def __init__(self, left: Any, op: str, right: Any, sql_type: SqlType | None = None):
        super().__init__(sql_type=sql_type)

        self.left = self._arg(left)
        self.op = op
        self.right = self._arg(right)

    def __sql__(self, params: list[Any]) -> str:
        left, op, right = self.left, self.op, self.right

        if isinstance(right, (list, tuple, set)):
            if not right:
                return "(1=0)"

            l_sql = self._value(left, params)

            base_type = Types.TEXT
            if isinstance(left, Expression):
                base_type = left.sql_type or base_type

            params.append(list(right))
            placeholder = f"${len(params)}"

            op = "!= ALL" if op in ("!=", "NOT IN", "<>") else "= ANY"
            return f"({l_sql} {op}({placeholder}::{base_type}[]))"

        l_sql = self._value(left, params)

        if isinstance(right, Node) and right.isolated:
            r_sql = f"({right.__sql__(params)})"
        else:
            r_sql = self._value(right, params)

        return f"({l_sql} {self.op} {r_sql})"


def _extract_template(value: Template):
    for s, interp in zip_longest(value.strings, value.interpolations):
        if s:
            yield s
        if interp:
            yield interp.value


@register_converter(Template)
class Concat(Expression):
    def __init__(self, value: Template | Any, *args: Any):
        super().__init__(sql_type=Types.TEXT)

        if isinstance(value, Template):
            self.args = [self._arg(a) for a in _extract_template(value)]
        else:
            self.args = [self._arg(a) for a in (value, *args)]

    def __sql__(self, params: list[Any]) -> str:
        return f"({' || '.join(self._value(a, params) for a in self.args)})"


class Raw(Expression):
    def __init__(self, value: Template):
        super().__init__(sql_type=Types.TEXT)

        self.args = [self._arg(a) for a in _extract_template(value)]

    def __sql__(self, params: list[Any]) -> str:
        return f"({
            ''.join(
                self._value(a, params) if isinstance(a, Expression) else a
                for a in self.args
            )
        })"


class Cast(WrappedNodeMixin, Expression):
    def __init__(self, wrapped: Expression, to: str | Expression | FieldFactory):
        super().__init__(wrapped=wrapped, is_aggregate=wrapped.is_aggregate)

        if isinstance(to, FieldFactory):
            to = to.factory()

        self.to = to

    def __sql__(self, params: list) -> str:
        to = self.to
        if isinstance(to, Expression):
            to = to.sql_type
        return f"({super().__sql__(params)})::{to}"


class AtTimeZone(WrappedNodeMixin, Expression):
    def __init__(self, wrapped: Expression, zone: str | Expression):
        super().__init__(wrapped=wrapped, is_aggregate=wrapped.is_aggregate)

        self.zone = self._arg(zone)

    def __sql__(self, params: list[Any]) -> str:
        return (
            f"({super().__sql__(params)} AT TIME ZONE {self._value(self.zone, params)})"
        )


class Func(Expression):
    def __init__(
        self, name: str, *args, sql_type: SqlType | None = None, is_aggregate=False
    ):
        super().__init__(sql_type=sql_type, is_aggregate=is_aggregate)

        self.name = name.upper()
        self.args = self._list_arg(args)
        self.window: Window | None = None

    def over(self, partition_by=None, order_by=None):
        self.window = self._arg(Window(partition_by, order_by))
        self.is_windowed = True
        return self

    def _render_args(self, params: list[Any]) -> str:
        return ", ".join(self._value(arg, params) for arg in self.args)

    def __sql__(self, params: list[Any]) -> str:
        sql = f"{self.name}({self._render_args(params)})"
        if self.window:
            sql = f"{sql} OVER ({self.window.__sql__(params)})"
        return sql
