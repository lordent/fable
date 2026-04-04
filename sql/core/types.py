from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sql.core.aggregates import (
        AggregateAtTimeZone,
        AggregateCast,
        AggregateCoalesce,
        AggregateQ,
    )
    from sql.core.case import AggregateCase, ScalarCase
    from sql.core.datatypes import SqlType
    from sql.core.raw import AggregateRaw, AggregateRef, ScalarRaw, ScalarRef
    from sql.core.scalars import AtTimeZone, Cast, Coalesce, Q
    from sql.fields.base import Field
    from sql.models import Model, ProxyModel


class ScalarType:
    Case: type[ScalarCase]
    Q: type[Q]
    Cast: type[Cast]
    Coalesce: type[Coalesce]
    AtTimeZone: type[AtTimeZone]
    Ref: type[ScalarRef]
    Raw: type[ScalarRaw]


class AggregateType:
    Case: type[AggregateCase]
    Q: type[AggregateQ]
    Cast: type[AggregateCast]
    Coalesce: type[AggregateCoalesce]
    AtTimeZone: type[AggregateAtTimeZone]
    Ref: type[AggregateRef]
    Raw: type[AggregateRaw]


class QueryType:
    pass


type T_SqlType = SqlType | Field | None
type T_Model = type[Model] | ProxyModel
