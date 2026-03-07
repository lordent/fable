from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sql.model import Model


class Q:
    def __init__(self, query: str, *args: Any, _dependencies: set | None = None):
        self.query = query
        self.args = args
        self.dependencies = _dependencies or set()

    def get_sql(self, args_list: list[Any]) -> str:
        return self.query

    def compile(self, args_list: list[Any]) -> str:
        local_query = self.get_sql(args_list)

        if not self.args:
            return local_query

        for val in self.args:
            args_list.append(val)
            local_query = local_query.replace("{}", f"${len(args_list)}", 1)
        return local_query

    def _prepare_part(self, obj: Any, args: list[Any]) -> str:
        if isinstance(obj, Q):
            return obj.compile(args)
        return str(obj)

    def __operand__(self, operand: str, value: Any) -> Q:
        self_args: list[object] = []
        self_sql = self._prepare_part(self, self_args)

        new_deps = set(self.dependencies)
        value_args: tuple[Any, ...] = ()

        if isinstance(value, Q):
            v_args_list: list[object] = []
            value_sql = self._prepare_part(value, v_args_list)
            value_args = tuple(v_args_list)
            new_deps.update(value.dependencies)
        else:
            value_sql = "{}"
            value_args = (value,)

        return Q(
            f"({self_sql} {operand} {value_sql})",
            *(tuple(self_args) + value_args),
            _dependencies=new_deps,
        )

    def asc(self) -> Q:
        return Q(f"{self.compile([])} ASC", *self.args, _dependencies=self.dependencies)

    def desc(self) -> Q:
        return Q(
            f"{self.compile([])} DESC", *self.args, _dependencies=self.dependencies
        )

    def __eq__(self, val: Any) -> Q:
        if self is val:
            return True

        return self.__operand__("=", val)

    def __ne__(self, val: Any) -> Q:
        return self.__operand__("<>", val)

    def __gt__(self, val: Any) -> Q:
        return self.__operand__(">", val)

    def __lt__(self, val: Any) -> Q:
        return self.__operand__("<", val)

    def __and__(self, val: Any) -> Q:
        return self.__operand__("AND", val)

    def __or__(self, val: Any) -> Q:
        return self.__operand__("OR", val)

    def __hash__(self) -> int:
        return id(self)

    def union_all(self, other: Q) -> Q:
        return Q(
            f"({self.compile([])}) UNION ALL ({other.compile([])})",
            *(self.args + other.args),
            _dependencies=self.dependencies | other.dependencies,
        )

    def union(self, other: Q) -> Q:
        return Q(
            f"({self.compile([])}) UNION ({other.compile([])})",
            *(self.args + other.args),
            _dependencies=self.dependencies | other.dependencies,
        )

    def any(self, subquery: Q) -> Q:
        return Q(
            f"{self.compile([])} = ANY({subquery.compile([])})",
            *(self.args + subquery.args),
            _dependencies=self.dependencies | {subquery} | subquery.dependencies,
        )


class Column(Q):
    def __init__(self, name: str, table: type[Model] = None):
        super().__init__("", _dependencies={table} if table else set())
        self.name = name
        self.table = table

    def get_sql(self, args: list[object]) -> str:
        if self.table and hasattr(self.table, "Meta"):
            return f'"{self.table.Meta.alias}"."{self.name}"'
        return f'"{self.name}"'
