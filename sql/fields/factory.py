from copy import deepcopy
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sql.fields.base import Field
    from sql.models import Model


class FieldFactory:
    def __init__(self, field_cls: type[Field], args: tuple, kwargs: dict):
        self.field_cls, self.args, self.kwargs = field_cls, args, kwargs

    def factory(self, owner: type[Model] = None, name: str = None):
        instance: Field = type.__call__(
            self.field_cls, *deepcopy(self.args), **deepcopy(self.kwargs)
        )
        instance.bind = self.factory
        if owner and name:
            setattr(owner, name, instance)
            instance.__set_name__(owner, name)
        return instance

    def __set_name__(self, owner: type[Model], name: str):
        self.factory(owner, name)
