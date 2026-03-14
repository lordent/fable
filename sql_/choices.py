from enum import Enum


class Choices(Enum):
    def __new__(cls, *args):
        obj = object.__new__(cls)
        obj._value_ = args[0]

        fields = [f for f in cls.__annotations__ if f != "value"]
        for i, field in enumerate(fields):
            setattr(obj, field, args[i + 1])
        return obj
