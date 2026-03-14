from enum import StrEnum
from string.templatelib import Template
from typing import TYPE_CHECKING, Any

from sql.enums import SqlType, Types

if TYPE_CHECKING:
    from .fields import Field
    from .model import Model


def _extract_template(value: Template):
    for i, s in enumerate(value.strings):
        if s:
            yield s
        if i < len(value.interpolations):
            yield value.interpolations[i].value


class S:
    relations: set[Model]

    def __init__(self):
        self.relations = set()

    def _value(self, value: Any, params: list[Any]) -> str:
        if isinstance(value, Template):
            value = Concat(*_extract_template(value))

        if isinstance(value, S):
            # self.relations |= value.relations
            return value.__sql__(params)

        if value is None:
            return "NULL"

        params.append(value)
        return f"${len(params)}"

    def __sql__(self, params: list[Any]) -> str:
        raise NotImplementedError()

    def prepare(self):
        params = []
        yield self.__sql__(params)
        yield from params

    def __str__(self):
        return self.__sql__([])


class E(S):
    sql_type: SqlType | None

    def __init__(
        self,
        sql_type: SqlType | None = None,
        is_aggregate: bool = False,
    ):
        super().__init__()

        self.sql_type = sql_type
        self.is_aggregate = is_aggregate

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
        if isinstance(other, (list, tuple)):
            return Q(self, "IN", other)
        if isinstance(other, Query):
            return Q(self, "= ANY", other)
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

    # --- Разное ---

    def cast(self, to: str | E):
        return Cast(self, to)

    def __rshift__(self, to: str | SqlType | type[Field] | Field):
        return self.cast(to)

    def at_timezone(self, zone: str | E):
        return AtTimeZone(self, zone)


class Q(E):
    def __init__(self, left: Any, op: str, right: Any, sql_type: SqlType | None = None):
        super().__init__(sql_type=sql_type)
        if isinstance(left, E):
            self.relations |= left.relations
        if isinstance(right, E):
            self.relations |= right.relations

        self.left = left
        self.op = op
        self.right = right

    def __sql__(self, params: list[Any]):
        l_sql = self._value(self.left, params)

        if "ANY" in self.op:
            r_sql = f"({self._value(self.right, params)})"
        elif self.op == "IN" and isinstance(self.right, (list, tuple)):
            r_sql = f"({', '.join(self._value(x, params) for x in self.right)})"
        else:
            r_sql = self._value(self.right, params)

        return f"({l_sql} {self.op} {r_sql})"


class W(S):
    def __init__(
        self,
        partition_by: list[E] | E | None = None,
        order_by: list[OrderBy | E] | OrderBy | None = None,
    ):
        super().__init__()
        self._partition_by = (
            partition_by
            if isinstance(partition_by, list)
            else ([partition_by] if partition_by else [])
        )
        self._order_by = (
            order_by if isinstance(order_by, list) else ([order_by] if order_by else [])
        )

        for part in self._partition_by:
            if isinstance(part, E):
                self.relations |= part.relations
        for order in self._order_by:
            if isinstance(order, E):
                self.relations |= order.relations

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


class Func(E):
    def __init__(
        self, name: str, *args, sql_type: SqlType | None = None, is_aggregate=False
    ):
        super().__init__(sql_type=sql_type, is_aggregate=is_aggregate)
        self.name = name.upper()
        self.args = args
        self.window: W | None = None
        for arg in args:
            if isinstance(arg, S):
                self.relations |= arg.relations

    def over(self, partition_by=None, order_by=None):
        self.window = W(partition_by, order_by)
        self.relations |= self.window.relations
        return self

    def _render_args(self, params: list[Any]) -> str:
        if self.name == "EXTRACT" and len(self.args) == 2:
            return f"{self.args[0]} FROM {self._value(self.args[1], params)}"
        return ", ".join(self._value(arg, params) for arg in self.args)

    def __sql__(self, params: list[Any]) -> str:
        sql = f"{self.name}({self._render_args(params)})"
        if self.window:
            sql = f"{sql} OVER ({self.window.__sql__(params)})"
        return sql


class OrderDirections(StrEnum):
    DESC = "DESC"
    ASC = "ASC"


class OrderBy(S):
    def __init__(self, wrapped: S, direction: OrderDirections):
        super().__init__()

        self.relations |= wrapped.relations
        self.wrapped, self.direction = wrapped, direction

    def __sql__(self, params: list[Any]):
        return f"{self.wrapped.__sql__(params)} {self.direction.value}"


class AtTimeZone(E):
    def __init__(self, wrapped: E, zone: str | E):
        super().__init__(is_aggregate=wrapped.is_aggregate)

        self.relations |= wrapped.relations
        if isinstance(zone, E):
            self.relations |= zone.relations

        self.wrapped, self.zone = wrapped, zone

    def __sql__(self, params: list[Any]) -> str:
        return (
            f"({self.wrapped.__sql__(params)} "
            f"AT TIME ZONE {self._value(self.zone, params)})"
        )


class Cast(E):
    def __init__(self, wrapped: E, to: str | E):
        super().__init__(is_aggregate=wrapped.is_aggregate)

        self.relations |= wrapped.relations
        self.to = to
        self.wrapped = wrapped

    def __sql__(self, params: list) -> str:
        to = self.to
        if isinstance(to, E):
            to = to.sql_type
        return f"({self.wrapped.__sql__(params)})::{to}"


class Concat(E):
    def __init__(self, *args: Any):
        super().__init__(sql_type=Types.TEXT)

        self.args = list(self._extract(args))

    def _extract(self, args: list[Any]):
        for arg in args:
            if isinstance(arg, E):
                self.relations |= arg.relations
                yield arg

            elif isinstance(arg, Template):
                yield from self._extract(_extract_template(arg))

            else:
                yield arg

    def __sql__(self, params: list[Any]):
        parts = [self._value(arg, params) for arg in self.args]
        return f"({' || '.join(parts)})"


class Query(S):
    def __init__(self):
        super().__init__()

        self._values: dict[str, Any] = {}
