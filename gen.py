OPERATORS = [
    "__add__",
    "__sub__",
    "__mul__",
    "__truediv__",
    "__mod__",
    "__eq__",
    "__ne__",
    "__gt__",
    "__ge__",
    "__lt__",
    "__le__",
    "__and__",
    "__or__",
    "__invert__",
]

TEMPLATE = """
    @overload
    def {name}(self: AggregateType, other: Any) -> AggregateQ: ...
    @overload
    def {name}(self: Any, other: AggregateType) -> AggregateQ: ...
    @overload
    def {name}(self: ScalarExpression, other: Any) -> Q: ...
"""


def generate(output_file="sql/core/_expressions_overloads.py"):
    content = [
        """# Не редактируйте этот файл, он создан автоматически

from typing import TYPE_CHECKING, Any, overload

if TYPE_CHECKING:
    from .aggregates import AggregateQ, AggregateType
    from .expressions import Q, ScalarExpression

    
class ExpressionOverloads:"""
    ]

    for name in OPERATORS:
        content.append(TEMPLATE.format(name=name))

    with open(output_file, "w") as f:
        f.write("\n".join(content))


if __name__ == "__main__":
    generate()
