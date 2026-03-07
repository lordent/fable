from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .field import Field
    from .model import Model


class Expr:
    relations: set[type[Model]]

    def __init__(self, relations: set[type[Model]] | None = None):
        self.relations = relations or set()

    # Сравнение
    def __eq__(self, other: Any) -> Q:
        return Q("=", self, other)

    def __ne__(self, other: Any) -> Q:
        return Q("!=", self, other)

    def __lt__(self, other: Any) -> Q:
        return Q("<", self, other)

    def __le__(self, other: Any) -> Q:
        return Q("<=", self, other)

    def __gt__(self, other: Any) -> Q:
        return Q(">", self, other)

    def __ge__(self, other: Any) -> Q:
        return Q(">=", self, other)

    # Логика
    def __and__(self, other: Any) -> Q:
        return Q("AND", self, other)

    def __or__(self, other: Any) -> Q:
        return Q("OR", self, other)

    # Арифметика и спец-операторы
    def __add__(self, other: Any) -> Q:
        return Q("+", self, other)

    def __radd__(self, other: Any) -> Q:
        return Q("+", other, self)

    def __mod__(self, other: Any) -> Q:
        return Q("~", self, other)

    def at_timezone(self, tz: Any) -> AtTimeZoneExpr:
        return AtTimeZoneExpr(self, tz)

    def cast(self, to: str | type[Field]) -> Cast:
        return Cast(self, to)

    def _compile_param(self, v: Any, params: list[Any]) -> str:
        if isinstance(v, Expr):
            return v.compile(params)
        params.append(v)
        return f"${len(params)}"

    def compile(self, params: list[Any]) -> str:
        raise NotImplementedError


class Cast(Expr):
    def __init__(self, wrapped: Expr, to: str | type[Field]):
        self.target_type = to.sql_type if hasattr(to, "sql_type") else str(to)
        super().__init__(relations=wrapped.relations)

        self.wrapped = wrapped

    def compile(self, params: list) -> str:
        return f"({self.wrapped.compile(params)})::{self.target_type}"


class AtTimeZoneExpr(Expr):
    def __init__(self, wrapped: Expr, tz: Any):
        rels = wrapped.relations | (tz.relations if isinstance(tz, Expr) else set())
        super().__init__(relations=rels)

        self.wrapped = wrapped
        self.tz = tz

    def compile(self, params: list) -> str:
        return (
            f"({self.wrapped.compile(params)} "
            f"AT TIME ZONE {self._compile_param(self.tz, params)})"
        )


class Q(Expr):
    def __init__(self, op: str, left: Any, right: Any):
        rels = set()
        if isinstance(left, Expr):
            rels |= left.relations
        if isinstance(right, Expr):
            rels |= right.relations
        super().__init__(relations=rels)
        self.op, self.left, self.right = op, left, right

    def compile(self, params: list[Any]) -> str:
        return (
            f"({self._compile_param(self.left, params)} "
            f"{self.op} "
            f"{self._compile_param(self.right, params)})"
        )

    def __repr__(self) -> str:
        return self.compile([])


class Now(Expr):
    def compile(self, params: list) -> str:
        return "CURRENT_TIMESTAMP"


class Func(Expr):
    def __init__(self, name: str, arg: Any, is_distinct: bool = False):
        rels = arg.relations if isinstance(arg, Expr) else set()
        super().__init__(relations=rels)

        self.name = name
        self.arg = arg
        self.is_distinct = is_distinct

    def distinct(self) -> Func:
        return Func(self.name, self.arg, is_distinct=True)

    def compile(self, params: list[Any]) -> str:
        distinct_str = "DISTINCT " if self.is_distinct else ""
        return f"{self.name}({distinct_str}{self._compile_param(self.arg, params)})"


def Count(arg: Any = "*") -> Func:
    return Func("COUNT", arg)


def Sum(arg: Any) -> Func:
    return Func("SUM", arg)


def Min(arg: Any) -> Func:
    return Func("MIN", arg)


def Max(arg: Any) -> Func:
    return Func("MAX", arg)


def Avg(arg: Any) -> Func:
    return Func("AVG", arg)
