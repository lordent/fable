from sql.core.expressions import Expression, Window

from sql.core.base import OrderBy, OrderDirections, QueryContext


class MockExpr(Expression):
    def __init__(self, name, sql_type=None):
        super().__init__(sql_type=sql_type)
        self.name = name

    def __sql__(self, context):
        return f'"{self.name}"'

    def asc(self):
        return OrderBy(self, OrderDirections.ASC)


def test_window_empty():
    w = Window()
    assert w.__sql__(QueryContext()) == ""


def test_window_partition_only():
    col1 = MockExpr("city")
    w = Window(partition_by=col1)
    assert w.__sql__(QueryContext()) == 'PARTITION BY "city"'

    col2 = MockExpr("category")
    w_multi = Window(partition_by=[col1, col2])
    assert w_multi.__sql__(QueryContext()) == 'PARTITION BY "city", "category"'


def test_window_order_auto_asc():
    col = MockExpr("salary")
    w = Window(order_by=col)
    assert w.__sql__(QueryContext()) == 'ORDER BY "salary" ASC'


def test_window_order_explicit():
    col = MockExpr("salary")
    order = OrderBy(col, OrderDirections.DESC)
    w = Window(order_by=order)
    assert w.__sql__(QueryContext()) == 'ORDER BY "salary" DESC'


def test_window_full_rendering():
    p = MockExpr("dept")
    o = MockExpr("id")
    w = Window(partition_by=p, order_by=o.asc())

    sql = w.__sql__(QueryContext())
    assert sql == 'PARTITION BY "dept" ORDER BY "id" ASC'


def test_window_relations_propagation():
    class MockModel:
        pass

    model = MockModel()

    expr = MockExpr("test")
    expr.relations.add(model)

    w = Window(partition_by=expr)
    assert model in w.relations
