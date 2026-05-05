import pytest

from sql.core.node import Node, QueryContext
from sql.queries.select import Select
from tests.test_models import User


def test_query_context_full():
    ctx = QueryContext()

    query = Select(*User)

    sub_ctx = ctx.sub()
    assert sub_ctx.level == 1
    assert sub_ctx.params is ctx.params
    assert ctx.value(None) == "NULL"
    assert ctx.value(query) == f"({query})"
    assert ctx.add_param("val") == "$1"
    assert ctx.params == ["val"]

    with pytest.raises(NotImplementedError):
        str(Node())
