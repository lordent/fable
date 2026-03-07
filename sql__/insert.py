from collections.abc import Sequence
from typing import Self

from .model import Model
from .query import Q
from .typing import ColumnValue, SQLExpression


class Insert(Q):
    def __init__(self, model: type[Model], **values: SQLExpression):
        super().__init__("")
        self.model = model
        self._values = values
        self._returning: list[Q] = []
        self._conflict_target: Sequence[str] | None = None
        self._update_values: ColumnValue | None = None
        self._do_nothing: bool = False

    def returning(self, *fields: Q) -> Self:
        self._returning.extend(fields)
        return self

    def on_conflict_do_nothing(self) -> Self:
        self._do_nothing = True
        return self

    def on_conflict_update(
        self, index_fields: Sequence[str], **update_values: SQLExpression
    ) -> Self:
        self._conflict_target = index_fields
        self._update_values = update_values
        return self

    def compile(self, args: list[object]) -> str:
        table_name = self.model.Meta.table_name
        cols: list[str] = []
        placeholders: list[str] = []

        for name, value in self._values.items():
            cols.append(f'"{name}"')
            if isinstance(value, Q):
                placeholders.append(value.compile(args))
            else:
                args.append(value)
                placeholders.append(f"${len(args)}")

        sql = [
            f'INSERT INTO "{table_name}" ({", ".join(cols)}) VALUES ({", ".join(placeholders)})'
        ]

        if self._do_nothing:
            sql.append("ON CONFLICT DO NOTHING")
        elif self._conflict_target and self._update_values:
            targets = ", ".join(f'"{f}"' for f in self._conflict_target)
            updates: list[str] = []
            for k, v in self._update_values.items():
                if isinstance(v, Q):
                    updates.append(f'"{k}" = {v.compile(args)}')
                else:
                    args.append(v)
                    updates.append(f'"{k}" = ${len(args)}')
            sql.append(f"ON CONFLICT ({targets}) DO UPDATE SET {', '.join(updates)}")

        if self._returning:
            ret = [f.compile([]) for f in self._returning]
            sql.append(f"RETURNING {', '.join(ret)}")

        return " ".join(sql)

    def __iter__(self):
        args: list[object] = []
        yield self.compile(args)
        yield from args
