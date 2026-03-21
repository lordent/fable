from sql.core.aggregates import Avg, Count, Max, Sum
from sql.core.base import QueryContext
from sql.core.types import Types
from tests.conftest import FakeField


def test_aggregate_base_flags():
    c = Count(FakeField("id"))
    assert c.is_aggregate is True
    assert c.is_windowed is False


def test_count_star():
    c = Count()
    sql = c.__sql__(QueryContext())
    assert sql == "COUNT(*)"


def test_count_distinct():
    c = Count(FakeField("name"), distinct=True)
    sql = c.__sql__(QueryContext())
    assert 'COUNT(DISTINCT "name")' in sql


def test_sum_type_inheritance():
    f = FakeField("amount", sql_type=Types.NUMERIC)
    s = Sum(f)
    assert s.sql_type == Types.NUMERIC
    assert s.name == "SUM"


def test_avg_type_is_numeric():
    f = FakeField("age", sql_type=Types.INTEGER)
    a = Avg(f)
    assert a.sql_type == Types.NUMERIC


def test_max_min_type_inheritance():
    f = FakeField("created_at", sql_type=Types.DATE)
    m = Max(f)
    assert m.sql_type == Types.DATE
    assert m.__sql__(QueryContext()) == 'MAX("created_at")'


def test_aggregate_to_window_transformation():
    s = Sum(FakeField("amount"))
    assert s.is_aggregate is True

    sw = s.over()
    assert sw.is_aggregate is False
    assert sw.is_windowed is True
    assert "OVER" in sw.__sql__(QueryContext())
