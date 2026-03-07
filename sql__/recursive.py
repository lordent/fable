from typing import Any

from .list import List


class Recursive(List):
    def __init__(self, depth: int = 10, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.depth = depth
