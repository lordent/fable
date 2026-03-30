from datetime import timedelta
from uuid import uuid4

import pytest
from sql.core.expressions import Cast, Expression, Func, Raw

from sql.core.base import QueryContext
from sql.core.types import Types


class FakeExpr(Expression):
    def __init__(self, sql, sql_type=None, is_agg=False, is_win=False):
        super().__init__(sql_type=sql_type, is_aggregate=is_agg, is_windowed=is_win)
        self.sql = sql

    def __sql__(self, context: QueryContext):
        return self.sql


def test_expression_flags_propagation():
    agg = FakeExpr("sum(x)", is_agg=True)
    win = FakeExpr("rank()", is_win=True)
    plain = FakeExpr("col")

    expr1 = agg + plain
    assert expr1.is_aggregate is True
    assert expr1.is_windowed is False

    expr2 = win * plain
    assert expr2.is_windowed is True
    assert expr2.is_aggregate is False

    expr3 = (agg + win) / plain
    assert expr3.is_aggregate is True
    assert expr3.is_windowed is True


def test_sql_type_auto_resolve():
    num_expr = FakeExpr("price", sql_type=Types.NUMERIC)

    res = num_expr * 2
    assert res.sql_type == Types.NUMERIC

    f = Func("ABS", num_expr)
    assert f.sql_type == Types.NUMERIC


def test_q_any_all_magic():
    expr = FakeExpr("id", sql_type=Types.INTEGER)

    q_in = expr == [1, 2, 3]
    context = QueryContext()
    sql = q_in.__sql__(context)
    assert "= ANY" in sql
    assert "::INTEGER[]" in sql
    assert context.params == [[1, 2, 3]]

    q_not_in = expr != [1, 2]
    assert "!= ALL" in q_not_in.__sql__(QueryContext())


def test_jsonb_operators():
    meta = FakeExpr("metadata", sql_type=Types.JSONB)

    item = meta["points"]
    assert "->" in item.__sql__(QueryContext())
    assert item.sql_type == Types.JSONB

    text_val = meta.text("name")
    assert "->>" in text_val.__sql__(QueryContext())
    assert text_val.sql_type == Types.TEXT


def test_func_over_reset_aggregate():
    f = Func("SUM", FakeExpr("x"), is_aggregate=True)
    assert f.is_aggregate is True

    f.over(partition_by=FakeExpr("y"))
    assert f.is_aggregate is False
    assert f.is_windowed is True


def test_cast_explicit_type():
    expr = FakeExpr("1")
    casted = expr >> Types.BIGINT
    assert isinstance(casted, Cast)
    assert casted.sql_type == Types.BIGINT
    assert "::BIGINT" in casted.__sql__(QueryContext())


def test_raw_complex_types():
    assert "::INTERVAL" in Raw(timedelta(hours=1)).__sql__(QueryContext())
    assert "::UUID" in Raw(uuid4()).__sql__(QueryContext())
    assert "::JSONB" in Raw([1, 2, 3]).__sql__(QueryContext())
    assert "::JSONB" in Raw({"key": "val"}).__sql__(QueryContext())

    with pytest.raises(TypeError, match="Тип <class 'set'> не поддерживается в Raw"):
        Raw(set([1, 2]))


def test_q_is_null_isolation():
    q = FakeExpr("col") == None
    assert "(col IS NULL)" == q.__sql__(QueryContext())

    q_not = FakeExpr("col") != None
    assert "(col IS NOT NULL)" == q_not.__sql__(QueryContext())


def test_q_isolated_node_branch():
    subquery = FakeExpr("SELECT 1")
    subquery.isolated = True

    q = FakeExpr("val") == subquery
    assert "(val = (SELECT 1))" == q.__sql__(QueryContext())
