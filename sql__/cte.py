from typing import Any, Self

from .model import Model
from .query import Column, Q


class CTE(Column):
    def __init__(
        self, name: str, model: type[Model] = None, recursive: bool = False
    ) -> None:
        super().__init__(name=name)

        self.name = name
        self.recursive = recursive
        self.query: Q | None = None
        self.model = model

    def __getattr__(self, name: str) -> Any:
        if self.model and hasattr(self.model, name):
            return getattr(self.model, name)
        raise AttributeError(
            f"CTE '{self.name}' has no attribute '{name}' (model not bound)"
        )

    def __call__(self, query: Q) -> Self:
        self.query = query
        self.dependencies.update(query.dependencies)
        return self

    def compile_definition(self, args: list[object]) -> str:
        if not self.query:
            raise ValueError(f"CTE {self.name} has no query")
        return f'"{self.name}" AS ({self.query.compile(args)})'

    def __str__(self) -> str:
        return f'"{self.name}"'
