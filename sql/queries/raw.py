import inspect
from string.templatelib import Template
from typing import Any

from sql.core.node import Node, QueryContext
from sql.core.raw import Raw
from sql.core.types import T_Model
from sql.models import Model, ProxyModel
from sql.queries.base import Query


class ModelValue:
    __slots__ = ("model",)

    def __init__(self, model: T_Model):
        self.model = model


class _QueryRaw(Raw):
    def escape(self, value: Any):
        if isinstance(value, ProxyModel) or (
            inspect.isclass(value) and issubclass(value, Model)
        ):
            self.relations.add(value)
            return ModelValue(value)
        return super().escape(value)

    def __sql_argument__(self, argument: Any, context: QueryContext):
        if isinstance(argument, ModelValue):
            return argument.model.__sql__(context)
        return str(argument)


class RawQuery(Query):
    def __init__(self, raw: Template):
        super().__init__()

        self._query: Node = self._arg(_QueryRaw(raw))

    def __sql__(self, context: QueryContext):
        return self._query.__sql__(context)
