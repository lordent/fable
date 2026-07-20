from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from sql.fields.base import Field


F = TypeVar("F", bound="Field")


class FieldBlueprint:
    __slots__ = "field_class", "args", "kwargs"

    def __init__(self, field_class: type[Field], args, kwargs):
        self.field_class = field_class
        self.args = args
        self.kwargs = kwargs

    def factory(self):
        field: Field = type.__call__(self.field_class, *self.args, **self.kwargs)
        field.__blueprint__ = self
        return field


class FieldMeta(type):
    def __call__(cls: type[F], *args, **kwargs) -> F:
        return FieldBlueprint(cls, args, kwargs).factory()
