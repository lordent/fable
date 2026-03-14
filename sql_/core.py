from __future__ import annotations

import inspect
import re
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Self, cast

if TYPE_CHECKING:
    from .field import Field
    from .model import Model


class SqlType(StrEnum):
    TEXT = "TEXT"
    INT = "INTEGER"
    BIGINT = "BIGINT"
    JSONB = "JSONB"
    UUID = "UUID"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMPTZ = "TIMESTAMP WITH TIME ZONE"
    TIME = "TIME"
    DATE = "DATE"
    BOOL = "BOOLEAN"
    NUMERIC = "NUMERIC"


class Expr:
    __slots__ = ("relations", "is_aggregate", "_window")

    relations: set[type[Model]]

    def __init__(
        self, relations: set[type[Model]] | None = None, is_aggregate: bool = False
    ):
        self.relations = relations or set()
        self.is_aggregate = is_aggregate
        self._window: Window | None = None

    def compile(self, params: list[Any]) -> str:
        raise NotImplementedError

    def prepare(self) -> tuple[str, list[Any]]:
        params: list[Any] = []
        sql = self.compile(params)
        return sql, params

    def over(self, partition_by: Any = None, order_by: Any = None) -> Self:
        self._window = Window(partition_by, order_by)
        self.is_aggregate = False
        return self

    def at_timezone(self, zone: str | Expr) -> AtTimeZone:
        return AtTimeZone(self, zone)

    def cast(self, to: str | SqlType | type[Field] | Field) -> Cast:
        field_instance: Field | None = None
        if hasattr(to, "sql_type"):
            f = cast("Field", to)
            target_type = f.sql_type
            field_instance = f
        elif isinstance(to, type) and hasattr(to, "sql_type"):
            field_instance = cast("Field", to())
            target_type = field_instance.sql_type
        else:
            target_type = to.value if isinstance(to, SqlType) else str(to)
        return Cast(self, target_type, field_instance=field_instance)

    def asc(self) -> OrderBy:
        return OrderBy(self, "ASC")

    def desc(self) -> OrderBy:
        return OrderBy(self, "DESC")

    def _compile_val(self, v: Any, params: list[Any]) -> str:
        if hasattr(v, "compile"):
            return cast(Expr, v).compile(params)
        if v is None:
            return "NULL"
        params.append(v)
        return f"${len(params)}"

    def __eq__(self, other: Any) -> Q:
        if other is None:
            return Q("IS NULL", self, None)
        if isinstance(other, (list, tuple)):
            return Q("IN", self, other)

        from .select import Select

        if isinstance(other, Select):
            return Q("= ANY", self, other)
        return Q("=", self, other)

    def __ne__(self, other: Any) -> Q:
        if other is None:
            return Q("IS NOT NULL", self, None)
        return Q("!=", self, other)

    def __lt__(self, other: Any) -> Q:
        return Q("<", self, other)

    def __le__(self, other: Any) -> Q:
        return Q("<=", self, other)

    def __gt__(self, other: Any) -> Q:
        return Q(">", self, other)

    def __ge__(self, other: Any) -> Q:
        return Q(">=", self, other)

    def __and__(self, other: Any) -> Q:
        return Q("AND", self, other)

    def __or__(self, other: Any) -> Q:
        return Q("OR", self, other)

    def __add__(self, other: Any) -> Q:
        return Q("+", self, other)

    def __sub__(self, other: Any) -> Q:
        return Q("-", self, other)

    def __mul__(self, other: Any) -> Q:
        return Q("*", self, other)

    def __truediv__(self, other: Any) -> Q:
        return Q("/", self, other)

    def __mod__(self, other: Any) -> Q:
        return Q("<->", self, other)

    def __rshift__(self, to: str | SqlType | type[Field] | Field):
        return self.cast(to)

    def __str__(self) -> str:
        return self.compile([])


class Q(Expr):
    __slots__ = ("op", "left", "right")

    def __init__(self, op: str, left: Any, right: Any = None):
        is_aggregate = any(getattr(x, "is_aggregate", False) for x in (left, right))
        rels: set[type[Model]] = set()
        for x in (left, right):
            if hasattr(x, "relations"):
                rels |= cast(Expr, x).relations
        super().__init__(relations=rels, is_aggregate=is_aggregate)
        self.op, self.left, self.right = op, left, right

    def compile(self, params: list[Any]) -> str:
        l_sql = self._compile_val(self.left, params) if self.left is not None else ""

        if self.op == "[]":
            return f"{l_sql}[{self._compile_val(self.right, params)}]"

        if self.op == "IN":
            if not self.right:
                return "(1=0)"
            placeholders = ", ".join([self._compile_val(v, params) for v in self.right])
            return f"({l_sql} IN ({placeholders}))"

        if self.op == "= ANY":
            return f"({l_sql} = ANY({self._compile_val(self.right, params)}))"

        r_sql = self._compile_val(self.right, params) if self.right is not None else ""
        if not l_sql:
            return f"({self.op} {r_sql})"
        if not r_sql:
            return f"({l_sql} {self.op})"
        return f"({l_sql} {self.op} {r_sql})"


class Func(Expr):
    __slots__ = ("name", "args", "is_distinct")

    def __init__(
        self,
        name: str,
        *args: Any,
        is_aggregate: bool = False,
        is_distinct: bool = False,
    ):
        rels: set[type[Model]] = set()
        for a in args:
            if hasattr(a, "relations"):
                rels |= cast(Expr, a).relations
        super().__init__(relations=rels, is_aggregate=is_aggregate)
        self.name, self.args, self.is_distinct = name, args, is_distinct

    def distinct(self) -> Self:
        self.is_distinct = True
        return self

    def compile(self, params: list[Any]) -> str:
        d = "DISTINCT " if self.is_distinct else ""
        args_sql = ", ".join(self._compile_val(a, params) for a in self.args)
        base = f"{self.name}({d}{args_sql})"
        return f"{base} OVER ({self._window.compile(params)})" if self._window else base


class Window:
    __slots__ = ("partition_by", "order_by")

    def __init__(self, partition_by: Any = None, order_by: Any = None):
        self.partition_by = (
            partition_by
            if isinstance(partition_by, (list, tuple))
            else ([partition_by] if partition_by else [])
        )
        self.order_by = (
            order_by
            if isinstance(order_by, (list, tuple))
            else ([order_by] if order_by else [])
        )

    def compile(self, params: list[Any]) -> str:
        parts = []
        if self.partition_by:
            sql = ", ".join(
                cast(Expr, p).compile(params) if hasattr(p, "compile") else str(p)
                for p in self.partition_by
            )
            parts.append(f"PARTITION BY {sql}")
        if self.order_by:
            sql = ", ".join(
                cast(Expr, o).compile(params) if hasattr(o, "compile") else str(o)
                for o in self.order_by
            )
            parts.append(f"ORDER BY {sql}")
        return " ".join(parts)


class OrderBy(Expr):
    __slots__ = ("wrapped", "direction")

    def __init__(self, wrapped: Expr, direction: str):
        super().__init__(relations=wrapped.relations)
        self.wrapped, self.direction = wrapped, direction

    def compile(self, params: list) -> str:
        return f"{self.wrapped.compile(params)} {self.direction}"


class Cast(Expr):
    __slots__ = ("wrapped", "to", "field_instance")

    def __init__(self, wrapped: Expr, to: str, field_instance: Field | None = None):
        super().__init__(relations=wrapped.relations, is_aggregate=wrapped.is_aggregate)
        self.wrapped, self.to, self.field_instance = wrapped, to, field_instance

    def compile(self, params: list) -> str:
        return f"({self.wrapped.compile(params)})::{self.to}"


class AtTimeZone(Expr):
    __slots__ = ("wrapped", "zone")

    def __init__(self, wrapped: Expr, zone: str | Expr):
        rels = wrapped.relations.copy()
        if hasattr(zone, "relations"):
            rels |= cast(Expr, zone).relations
        super().__init__(relations=rels, is_aggregate=wrapped.is_aggregate)
        self.wrapped, self.zone = wrapped, zone

    def compile(self, params: list[Any]) -> str:
        return (
            f"({self.wrapped.compile(params)} "
            f"AT TIME ZONE {self._compile_val(self.zone, params)})"
        )


class SQLInterpolation(Expr):
    __slots__ = ("_raw_tpl", "_params_map")

    def __init__(self, tpl: str, params_map: dict[str, Any]):
        rels = set()
        for v in params_map.values():
            r = getattr(v, "relations", None)
            if isinstance(r, set):
                rels |= r
            elif isinstance(v, type) and hasattr(v, "_table"):
                rels.add(v)

        super().__init__(relations=rels)
        self._raw_tpl = tpl
        self._params_map = params_map

    def _resolve_value(self, path: str, params: list[Any]) -> str:
        parts = path.split(".")
        root_name = parts[0]

        obj = self._params_map.get(root_name)
        if obj is None:
            return f"'{{{path}}}'"

        current = obj
        try:
            for part in parts[1:]:
                current = getattr(current, part)
        except AttributeError:
            return f"'{{{path}}}'"

        return self._compile_val(current, params)

    def compile(self, params: list[Any]) -> str:
        pattern = re.compile(r"\{([\w\.]+)\}")
        parts = []
        last_pos = 0

        for match in pattern.finditer(self._raw_tpl):
            static = self._raw_tpl[last_pos : match.start()]
            if static:
                safe_static = static.replace("'", "''")
                parts.append(f"'{safe_static}'")

            path = match.group(1)
            parts.append(self._resolve_value(path, params))

            last_pos = match.end()

        tail = self._raw_tpl[last_pos:]
        if tail:
            parts.append(f"'{tail.replace("'", "''")}'")

        if len(parts) == 1 and parts[0].startswith("'"):
            return parts[0]

        return f"({' || '.join(parts)})"


def concat(text: str) -> SQLInterpolation:
    frame = inspect.currentframe().f_back
    return SQLInterpolation(text, frame.f_locals | frame.f_globals)


def Count(arg: Any = "*", distinct: bool = False) -> Func:
    return Func("COUNT", arg, is_aggregate=True, is_distinct=distinct)


def Sum(arg):
    return Func("SUM", arg, is_aggregate=True)


def Avg(arg: Any) -> Func:
    return Func("AVG", arg, is_aggregate=True)


def Min(arg: Any) -> Func:
    return Func("MIN", arg, is_aggregate=True)


def Max(arg: Any) -> Func:
    return Func("MAX", arg, is_aggregate=True)


def Rank() -> Func:
    return Func("RANK")


def DenseRank() -> Func:
    return Func("DENSE_RANK")


def RowNumber() -> Func:
    return Func("ROW_NUMBER")


def Now() -> Func:
    return Func("NOW")


def Extract(arg: Any) -> Func:
    return Func("EXTRACT", arg)
