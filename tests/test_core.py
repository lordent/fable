from sql.core.base import (
    Node,
    QueryContext,
)
from sql.core.converters import register_converter
from sql.core.enums import OrderDirections
from sql.core.order import OrderBy
from sql.core.raw import Raw
from tests.conftest import Users

# --- МОКИ ДЛЯ ТЕСТОВ ---


class MockModel:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f"Model({self.name})"


class CustomType:
    def __init__(self, val):
        self.val = val


@register_converter(CustomType)
class CustomNode(Node):
    def __init__(self, obj: CustomType):
        super().__init__()
        self.obj = obj

    def __sql__(self, context: QueryContext):
        return f"CAST({context.add_param(self.obj.val)} AS TEXT)"


# --- ТЕСТ-ПЛАН ---


def test_relations_propagation():
    m1, m2 = MockModel("Users"), MockModel("Sales")

    child = Node()
    child.relations.add(m1)

    parent = Node()
    parent.relations.add(m2)

    parent._arg(child)

    assert m1 in parent.relations
    assert m2 in parent.relations
    assert len(parent.relations) == 2


def test_custom_converter_logic():
    node = Node()
    obj = CustomType("hello")

    result = node._arg(obj)

    assert isinstance(result, CustomNode)
    context = QueryContext()
    assert result.__sql__(context) == "CAST($1 AS TEXT)"
    assert context.params == ["hello"]


def test_list_arg_handling():
    node = Node()

    assert node._list_arg(None) == []
    assert len(node._list_arg(1)) == 1
    assert len(node._list_arg([1, 2, 3])) == 3
    assert len(node._list_arg((1, 2))) == 2


def test_orderby_rendering():
    base = CustomNode(CustomType("col_name"))
    order = OrderBy(base, OrderDirections.DESC)

    context = QueryContext()
    sql = order.__sql__(context)
    assert "DESC" in sql
    assert "CAST($1 AS TEXT)" in sql
    assert context.params == ["col_name"]


def test_prepare_interface():
    node = CustomNode(CustomType("data"))
    res = list(node.prepare())

    assert len(res) == 2
    assert res[0] == "CAST($1 AS TEXT)"
    assert res[1] == "data"


def test_injection():
    danger_value = "(SELECT TRUNCATE 'users')"

    expression = Raw.Scalar(t"{Users.birth_date} + {danger_value}")
    assert ['("Users"."birth_date" + $1::TEXT)', danger_value] == list(
        expression.prepare()
    )
