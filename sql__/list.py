from .mixins import SelectValuesMixin
from .query import Q


class List(SelectValuesMixin, Q):
    def __init__(self, *args, **kwargs):
        super().__init__("")
        self.dependencies = set()
        self._fields = {}
        self._distinct = False
        self._order_by = []

        if args or kwargs:
            self.values(*args, **kwargs)

    def distinct(self):
        self._distinct = True
        return self

    def order_by(self, *args):
        self._order_by = args
        return self

    def compile(self, args):
        obj_sql = self._json_build_object_recursive(self._fields, args)

        distinct_str = "DISTINCT " if self._distinct else ""

        order_str = ""
        if self._order_by:
            orders = [
                f.compile(args) if hasattr(f, "compile") else str(f)
                for f in self._order_by
            ]
            order_str = f" ORDER BY {', '.join(orders)}"

        return f"COALESCE(JSONB_AGG({distinct_str}{obj_sql}{order_str}), '[]'::jsonb)"
