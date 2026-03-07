from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Self

from .cte import CTE
from .mixins import SelectValuesMixin
from .query import Q
from .typing import Direction, SQLExpression

if TYPE_CHECKING:
    from .model import Model


class Select(SelectValuesMixin, Q):
    def __init__(
        self, *args: Q, model: type[Model] | None = None, **kwargs: SQLExpression
    ):
        super().__init__("")
        self.model = model
        self.dependencies: set[Any] = set()

        self._fields: dict[str, SQLExpression] = {}
        self._joins: dict[type[Model], tuple[str, Q]] = {}
        self._filters: list[Q] = []
        self._groups: list[Q] = []
        self._order_by: list[Q] = []
        self._limit: tuple[int | None, int | None] = (None, None)

        if args or kwargs:
            self.values(*args, **kwargs)

    def join(
        self, table: type[Model], condition: Q | None = None, mode: str = "LEFT"
    ) -> Self:
        if condition is None:
            condition = self._find_relation(table)

        self.dependencies.discard(table)
        self._joins[table] = (mode, condition)
        return self

    def _find_relation(self, target: type[Model]) -> Q:
        from .cte import CTE
        from .model import ModelManager

        # Все доступные источники (модели и уже добавленные CTE)
        sources = {
            dep for dep in self.dependencies if isinstance(dep, (ModelManager, CTE))
        }
        if self.model:
            sources.add(self.model)

        target_name = target.Meta.table_name
        target_fields = target.Meta.fields

        for src in sources:
            src_name = src.Meta.table_name
            src_fields = src.Meta.fields

            # 1. Если джойним CTE к Модели (или наоборот) — ищем одинаковые имена (id == id)
            if isinstance(target, CTE) or isinstance(src, CTE):
                if "id" in target_fields and "id" in src_fields:
                    return src_fields["id"] == target_fields["id"]

            # 2. Старая логика: поиск по именованию {table}_id
            fk_to_target = f"{target_name}_id"
            if fk_to_target in src_fields:
                return src_fields[fk_to_target] == target_fields["id"]

            fk_from_target = f"{src_name}_id"
            if fk_from_target in target_fields:
                return target_fields[fk_from_target] == src_fields["id"]

        raise ValueError(
            f"Could not auto-join {target_name}. No common fields or relations found."
        )

    def filter(self, *args: Q) -> Self:
        for condition in args:
            self._filters.append(condition)
            self.dependencies.update(condition.dependencies)
        return self

    def group(self, *args: Q) -> Self:
        self._groups = list(args)
        return self

    def order_by(self, *args: Q) -> Self:
        self._order_by = list(args)
        return self

    def _compile_with_clause(self, args: list[object]) -> str:
        from .cte import CTE

        found_ctes: list[CTE] = []
        for dep in self.dependencies:
            if isinstance(dep, CTE) and dep not in found_ctes:
                found_ctes.append(dep)

        if not found_ctes:
            return ""

        is_recursive = any(c.recursive for c in found_ctes)
        prefix = "WITH RECURSIVE " if is_recursive else "WITH "

        defs = [c.compile_definition(args) for c in found_ctes]
        return f"{prefix}{', '.join(defs)} "

    def _compile_values(self, args: list[object]) -> str:
        values: list[str] = []
        for name, value in self._fields.items():
            if isinstance(value, Q):
                values.append(f'{value.compile(args)} "{name}"')
            elif isinstance(value, dict):
                values.append(
                    f'{self._json_build_object_recursive(value, args)} "{name}"'
                )
            else:
                args.append(value)
                values.append(f'${len(args)} "{name}"')
        return ", ".join(values)

    def _compile_dependencies(self) -> str:
        from .cte import CTE

        deps = []
        for table in self.dependencies:
            if table in self._joins:
                continue
            if isinstance(table, CTE):
                continue

            deps.append(str(table))

        return ", ".join(deps)

    def compile(self, args: list[object]) -> str:
        with_sql = self._compile_with_clause(args)

        sql = [
            "SELECT",
            self._compile_values(args),
            "FROM",
            self._compile_dependencies(),
        ]

        for table, (mode, cond) in self._joins.items():
            sql.append(f"{mode} JOIN {table} ON {cond.compile(args)}")

        if self._filters:
            sql.append(f"WHERE {' AND '.join(f.compile(args) for f in self._filters)}")

        if self._groups:
            sql.append(f"GROUP BY {', '.join(g.compile(args) for g in self._groups)}")

        if self._order_by:
            sql.append(f"ORDER BY {', '.join(o.compile(args) for o in self._order_by)}")

        offset, limit = self._limit
        if offset is not None:
            args.append(offset)
            sql.append(f"OFFSET ${len(args)}")
        if limit is not None:
            args.append(limit)
            sql.append(f"LIMIT ${len(args)}")

        return with_sql + " ".join(sql)

    def __getitem__(self, val: int | slice) -> Self:
        if isinstance(val, slice):
            self._limit = (val.start, val.stop)
        else:
            self._limit = (None, val)
        return self

    def __iter__(self) -> Iterator[object]:
        args: list[object] = []
        sql = self.compile(args)
        yield sql
        yield from args

    def recursive(self, anchor: Q, direction: Direction = "down") -> CTE:
        from .cte import CTE
        from .model import ModelManager

        base_model: type[Model] = next(
            d for d in anchor.dependencies if isinstance(d, ModelManager)
        )

        pk = next(f for f in base_model if f.primary)
        fk = next(f for f in base_model if getattr(f, "to", None) == base_model)

        name = "tree_branch"

        tree = CTE(
            name,
            model=ModelManager.virtual(name, {"parent_id": fk, "id": pk}),
            recursive=True,
        )

        anchor_q = base_model.select(pk, fk).filter(anchor)

        if direction == "up":
            step_q = base_model.select(pk, fk).join(
                tree.model, pk == tree.parent_id, mode="INNER"
            )
        else:
            step_q = base_model.select(pk, fk).join(
                tree.model, fk == tree.id, mode="INNER"
            )

        return tree(anchor_q.union_all(step_q))
