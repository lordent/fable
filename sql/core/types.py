from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sql.core.datatypes import SqlType
    from sql.fields.base import Field
    from sql.models import Model, ProxyModel


class QueryType:
    pass


type T_SqlType = SqlType | Field | None
type T_Model = type[Model] | ProxyModel
