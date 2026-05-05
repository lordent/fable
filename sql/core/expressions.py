from typing import TYPE_CHECKING, Any, overload

from sql.core.datatypes import SqlType, Types
from sql.core.enums import DatePart
from sql.core.node import COLLECTION_TYPES, Node
from sql.core.order import OrderBy
from sql.core.types import T_SqlType

if TYPE_CHECKING:
    from sql.core.aggregates import AggregateExpression
    from sql.core.functions import ABS, AtTimeZone, Cast, Coalesce, Extract
    from sql.core.query import Q
    from sql.functions import Round


class Expression(Node):
    Q: type[Q]

    is_aggregation = False
    sql_type: T_SqlType = None

    @staticmethod
    def _get_sql_type(*args: Expression | T_SqlType):
        for arg in args:
            if isinstance(arg, SqlType):
                return arg

            if isinstance(arg, Expression) and (sql_type := arg.sql_type):
                return sql_type

    @staticmethod
    def _get_aggregation(*args: Any):
        for arg in args:
            if isinstance(arg, Expression) and arg.is_aggregation:
                return True

    def __init__(self, sql_type: T_SqlType = None, is_aggregation=None):
        super().__init__()

        self.sql_type = sql_type or self.sql_type
        self.is_aggregation = (
            self.is_aggregation if is_aggregation is None else is_aggregation
        )

    # --- OVERLOADS ---

    @overload
    def __and__(self: AggregateExpression, other: Any) -> AggregateExpression: ...
    @overload
    def __and__(self: Any, other: AggregateExpression) -> AggregateExpression: ...
    @overload
    def __and__(self: Any, other: Any) -> Expression: ...
    @overload
    def __or__(self: AggregateExpression, other: Any) -> AggregateExpression: ...
    @overload
    def __or__(self: Any, other: AggregateExpression) -> AggregateExpression: ...
    @overload
    def __or__(self: Any, other: Any) -> Expression: ...
    @overload
    def __invert__(self: AggregateExpression) -> AggregateExpression: ...
    @overload
    def __invert__(self: Any) -> Expression: ...
    @overload
    def __eq__(self: AggregateExpression, other: Any) -> AggregateExpression: ...
    @overload
    def __eq__(self: Any, other: AggregateExpression) -> AggregateExpression: ...
    @overload
    def __eq__(self: Any, other: Any) -> Expression: ...
    @overload
    def __ne__(self: AggregateExpression, other: Any) -> AggregateExpression: ...
    @overload
    def __ne__(self: Any, other: AggregateExpression) -> AggregateExpression: ...
    @overload
    def __ne__(self: Any, other: Any) -> Expression: ...
    @overload
    def __lt__(self: AggregateExpression, other: Any) -> AggregateExpression: ...
    @overload
    def __lt__(self: Any, other: AggregateExpression) -> AggregateExpression: ...
    @overload
    def __lt__(self: Any, other: Any) -> Expression: ...
    @overload
    def __le__(self: AggregateExpression, other: Any) -> AggregateExpression: ...
    @overload
    def __le__(self: Any, other: AggregateExpression) -> AggregateExpression: ...
    @overload
    def __le__(self: Any, other: Any) -> Expression: ...
    @overload
    def __gt__(self: AggregateExpression, other: Any) -> AggregateExpression: ...
    @overload
    def __gt__(self: Any, other: AggregateExpression) -> AggregateExpression: ...
    @overload
    def __gt__(self: Any, other: Any) -> Expression: ...
    @overload
    def __ge__(self: AggregateExpression, other: Any) -> AggregateExpression: ...
    @overload
    def __ge__(self: Any, other: AggregateExpression) -> AggregateExpression: ...
    @overload
    def __ge__(self: Any, other: Any) -> Expression: ...
    @overload
    def __add__(self: AggregateExpression, other: Any) -> AggregateExpression: ...
    @overload
    def __add__(self: Any, other: AggregateExpression) -> AggregateExpression: ...
    @overload
    def __add__(self: Any, other: Any) -> Expression: ...
    @overload
    def __sub__(self: AggregateExpression, other: Any) -> AggregateExpression: ...
    @overload
    def __sub__(self: Any, other: AggregateExpression) -> AggregateExpression: ...
    @overload
    def __sub__(self: Any, other: Any) -> Expression: ...
    @overload
    def __mul__(self: AggregateExpression, other: Any) -> AggregateExpression: ...
    @overload
    def __mul__(self: Any, other: AggregateExpression) -> AggregateExpression: ...
    @overload
    def __mul__(self: Any, other: Any) -> Expression: ...
    @overload
    def __truediv__(self: AggregateExpression, other: Any) -> AggregateExpression: ...
    @overload
    def __truediv__(self: Any, other: AggregateExpression) -> AggregateExpression: ...
    @overload
    def __truediv__(self: Any, other: Any) -> Expression: ...
    @overload
    def __mod__(self: AggregateExpression, other: Any) -> AggregateExpression: ...
    @overload
    def __mod__(self: Any, other: AggregateExpression) -> AggregateExpression: ...
    @overload
    def __mod__(self: Any, other: Any) -> Expression: ...
    @overload
    def __abs__(self: AggregateExpression) -> AggregateExpression: ...
    @overload
    def __abs__(self: Any) -> Expression: ...
    @overload
    def cast(self: AggregateExpression, to: T_SqlType) -> AggregateExpression: ...
    @overload
    def cast(self: Any, to: T_SqlType) -> Expression: ...
    @overload
    def __rshift__(self: AggregateExpression, to: T_SqlType) -> AggregateExpression: ...
    @overload
    def __rshift__(self: Any, to: T_SqlType) -> Expression: ...
    @overload
    def round(self: AggregateExpression, precision: int) -> AggregateExpression: ...
    @overload
    def round(self: Any, precision: int) -> Expression: ...
    @overload
    def extract(self: AggregateExpression, part: DatePart) -> AggregateExpression: ...
    @overload
    def extract(self: Any, part: DatePart) -> Expression: ...
    @overload
    def at_timezone(
        self: AggregateExpression, zone: str | Expression
    ) -> AggregateExpression: ...
    @overload
    def at_timezone(self: Any, zone: str | Expression) -> Expression: ...
    @overload
    def default(self: AggregateExpression, default: Any) -> AggregateExpression: ...
    @overload
    def default(self: Any, default: AggregateExpression) -> AggregateExpression: ...
    @overload
    def default(self: Any, default: Any) -> Expression: ...
    @overload
    def contains(self: AggregateExpression, other: Any) -> AggregateExpression: ...
    @overload
    def contains(self: Any, other: AggregateExpression) -> AggregateExpression: ...
    @overload
    def contains(self: Any, other: Any) -> Expression: ...
    @overload
    def overlap(self: AggregateExpression, other: Any) -> AggregateExpression: ...
    @overload
    def overlap(self: Any, other: AggregateExpression) -> AggregateExpression: ...
    @overload
    def overlap(self: Any, other: Any) -> Expression: ...
    @overload
    def __getitem__(
        self: AggregateExpression, key: str | int
    ) -> AggregateExpression: ...
    @overload
    def __getitem__(self: Any, key: str | int) -> Expression: ...
    @overload
    def text(self: AggregateExpression, key: str | int) -> AggregateExpression: ...
    @overload
    def text(self: Any, key: str | int) -> Expression: ...
    @overload
    def dist(self: AggregateExpression, other: Any) -> AggregateExpression: ...
    @overload
    def dist(self: Any, other: AggregateExpression) -> AggregateExpression: ...
    @overload
    def dist(self: Any, other: Any) -> Expression: ...

    # --- OVERLOADS END ---

    # --- Логические операции ---

    def __and__(self, other: Any) -> Q:
        return self.Q(self, "AND", other, sql_type=Types.BOOLEAN)

    def __or__(self, other: Any) -> Q:
        return self.Q(self, "OR", other, sql_type=Types.BOOLEAN)

    def __invert__(self) -> Q:
        return self.Q(None, "NOT", self, sql_type=Types.BOOLEAN)

    # --- Сравнение ---

    def __eq__(self, other: Any) -> Q:
        if other is None:
            return self.Q(self, "IS NULL", None, sql_type=Types.BOOLEAN)
        if isinstance(other, COLLECTION_TYPES):
            return self.Q(self, "IN", other, sql_type=Types.BOOLEAN)
        return self.Q(self, "=", other, sql_type=Types.BOOLEAN)

    def __ne__(self, other: Any) -> Q:
        if other is None:
            return self.Q(self, "IS NOT NULL", None)
        return self.Q(self, "!=", other, sql_type=Types.BOOLEAN)

    def __lt__(self, other: Any) -> Q:
        return self.Q(self, "<", other, sql_type=Types.BOOLEAN)

    def __le__(self, other: Any) -> Q:
        return self.Q(self, "<=", other, sql_type=Types.BOOLEAN)

    def __gt__(self, other: Any) -> Q:
        return self.Q(self, ">", other, sql_type=Types.BOOLEAN)

    def __ge__(self, other: Any) -> Q:
        return self.Q(self, ">=", other, sql_type=Types.BOOLEAN)

    # --- Арифметика ---

    def __add__(self, other: Any) -> Q:
        return self.Q(self, "+", other)

    def __sub__(self, other: Any) -> Q:
        return self.Q(self, "-", other)

    def __mul__(self, other: Any) -> Q:
        return self.Q(self, "*", other)

    def __truediv__(self, other: Any) -> Q:
        return self.Q(self, "/", other)

    def __mod__(self, other: Any) -> Q:
        return self.Q(self, "%", other)

    def __abs__(self) -> ABS: ...

    # --- Функции ---

    def cast(self, to: T_SqlType) -> Cast: ...

    def __rshift__(self, to: T_SqlType) -> Cast:
        return self.cast(to)

    def round(self, precision: int = 0) -> Round: ...

    def extract(self, part: DatePart) -> Extract: ...

    @property
    def year(self):
        return self.extract(DatePart.YEAR)

    @property
    def month(self):
        return self.extract(DatePart.MONTH)

    @property
    def days(self):
        return (self.extract(DatePart.EPOCH) / 86400).round()

    def at_timezone(self, zone: str | Expression) -> AtTimeZone: ...

    def default(self, default: Any) -> Coalesce: ...

    # --- Сортировка ---

    def asc(self, nulls_first=None) -> OrderBy:
        return OrderBy(self, OrderBy.Direction.ASC, nulls_first)

    def desc(self, nulls_first=None) -> OrderBy:
        return OrderBy(self, OrderBy.Direction.DESC, nulls_first)

    # --- Массивы и JSON ---

    def contains(self, other: Any) -> Q:
        return self.Q(
            self,
            "@>",
            [other] if not isinstance(other, COLLECTION_TYPES) else other,
            sql_type=Types.BOOLEAN,
        )

    def overlap(self, other: Any) -> Q:
        return self.Q(self, "&&", other, sql_type=Types.BOOLEAN)

    def __getitem__(self, key: str | int) -> Q:
        return self.Q(self, "->", str(key), sql_type=Types.JSONB)

    def text(self, key: str | int) -> Q:
        return self.Q(self, "->>", str(key), sql_type=Types.TEXT)

    # --- Разное ---

    def dist(self, other: Any) -> Q:
        return self.Q(self, "<->", other, sql_type=Types.DOUBLE_PRECISION)
