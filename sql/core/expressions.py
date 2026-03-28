from datetime import date, datetime, timedelta
from decimal import Decimal
from string.templatelib import Template
from typing import TYPE_CHECKING, Any
from uuid import UUID

from sql.core.base import (
    Node,
    OrderBy,
    OrderDirections,
    QueryContext,
    WrappedNodeMixin,
    register_converter,
)
from sql.core.types import SqlType, Types
from sql.core.window import Window
from sql.utils import extract_template, quote_ident

if TYPE_CHECKING:
    from sql.fields.base import Field


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
            if self.sql_type is None:
                self.sql_type = value.sql_type

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

    # --- Массивы и JSON ---

    def contains(self, other: Any) -> Q:
        return Q(
            self,
            "@>",
            [other] if not isinstance(other, (list, tuple, set)) else other,
            sql_type=Types.BOOLEAN,
        )

    def overlap(self, other: Any) -> Q:
        return Q(self, "&&", other, sql_type=Types.BOOLEAN)

    def __getitem__(self, key: str | int) -> Q:
        return Q(self, "->", str(key), sql_type=Types.JSONB)

    def text(self, key: str | int) -> Q:
        return Q(self, "->>", str(key), sql_type=Types.TEXT)

    # --- Разное ---

    def default(self, other: Any):
        return Coalesce(self, other)

    def dist(self, other: Any) -> Q:
        return Q(self, "<->", other, sql_type=Types.DOUBLE_PRECISION)


NEGATION_OPS = {"!=", "NOT IN", "<>"}
ARRAY_OPS = {"=", "IN"} | NEGATION_OPS


class Q(Expression):
    def __init__(self, left: Any, op: str, right: Any, sql_type: SqlType | None = None):
        super().__init__(sql_type=sql_type)

        self.left = self._arg(left)
        self.op = op
        self.right = self._arg(right)

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


@register_converter(Template)
class Concat(Expression):
    def __init__(self, value: Any, *args: Any):
        super().__init__(sql_type=Types.TEXT)

        if isinstance(value, Template):
            self.args = [self._arg(a) for a in extract_template(value)]
        else:
            self.args = [self._arg(a) for a in (value, *args)]

    def __sql__(self, context: QueryContext) -> str:
        return f"({' || '.join(self._value(a, context) for a in self.args)})"


class Raw(Expression):
    def __init__(self, value: Any):
        super().__init__()

        if isinstance(value, Template):
            self.args = [self._arg(a) for a in extract_template(value)]
        else:
            self.args = [self.from_python(value)]

    def from_python(self, value: Any):
        if isinstance(value, int):
            self.sql_type = Types.BIGINT
        elif isinstance(value, (float, Decimal)):
            self.sql_type = Types.NUMERIC
        elif isinstance(value, (datetime, date)):
            self.sql_type = Types.TIMESTAMPTZ
        elif isinstance(value, timedelta):
            self.sql_type = Types.INTERVAL
        elif isinstance(value, UUID):
            self.sql_type = Types.UUID
        elif isinstance(value, (list, dict)):
            self.sql_type = Types.JSONB
        else:
            raise TypeError(f"Тип {type(value)} не поддерживается в Raw")
        return value

    def __sql__(self, context: QueryContext) -> str:
        if type_ := self.sql_type or "":
            type_ = f"::{type_}"
        return f"({
            ''.join(
                self._value(a, context) if isinstance(a, Node) else str(a)
                for a in self.args
            )
        }){type_}"


class Ref(Expression):
    def __init__(self, reference: str):
        super().__init__()

        self.reference = reference

    def __sql__(self, context: QueryContext) -> str:
        return quote_ident(self.reference)


class Cast(WrappedNodeMixin, Expression):
    def __init__(self, wrapped: Expression, to: str | Expression):
        super().__init__(wrapped=wrapped, is_aggregate=wrapped.is_aggregate)

        if isinstance(to, Expression):
            self.sql_type = to.sql_type
        else:
            self.sql_type = to

        self.to = to

    def __sql__(self, context: QueryContext):
        return f"({super().__sql__(context)})::{self.sql_type}"


class AtTimeZone(WrappedNodeMixin, Expression):
    def __init__(self, wrapped: Expression, zone: str | Expression):
        super().__init__(wrapped=wrapped, is_aggregate=wrapped.is_aggregate)

        self.zone = self._arg(zone)

    def __sql__(self, context: QueryContext) -> str:
        return (
            f"({super().__sql__(context)} "
            f"AT TIME ZONE {self._value(self.zone, context)})"
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
        self.is_aggregate = False
        return self

    def _render_args(self, context: QueryContext) -> str:
        return ", ".join(self._value(arg, context) for arg in self.args)

    def __sql__(self, context: QueryContext) -> str:
        sql = f"{self.name}({self._render_args(context)})"
        if self.window:
            sql = f"{sql} OVER ({self.window.__sql__(context)})"
        return sql


def Coalesce(*args: Any, sql_type: SqlType | None = None):
    return Func("COALESCE", *args, sql_type=sql_type)
