from sql.core.base import QueryContext
from sql.core.expressions import Coalesce, Func
from sql.core.functions import Age, Extract, Now, Rank
from sql.core.types import Types
from tests.conftest import FakeField


def test_window_functions_basic():
    r = Rank()
    assert isinstance(r, Func)
    assert r.name == "RANK"
    assert r.__sql__(QueryContext()) == "RANK()"


def test_now_function():
    n = Now()
    assert n.__sql__(QueryContext()) == "NOW()"


def test_age_function():
    birth = FakeField("birth")

    a1 = Age(birth)
    assert a1.sql_type == Types.INTERVAL
    assert "AGE" in a1.__sql__(QueryContext())

    today = FakeField("today")
    a2 = Age(birth, today)
    assert 'AGE("birth", "today")' in a2.__sql__(QueryContext())


def test_extract_logic():
    col = FakeField("created_at")
    ext = Extract(col, part=Extract.YEAR)

    assert ext.sql_type == Types.INTEGER
    sql = ext.__sql__(QueryContext())
    assert "EXTRACT(YEAR FROM" in sql
    assert '"created_at"' in sql


def test_coalesce_type_inference():
    f1 = FakeField("f1", sql_type=Types.NUMERIC)

    c = Coalesce(f1, 0)
    assert c.sql_type == Types.NUMERIC
    assert 'COALESCE("f1", $1)' in c.__sql__(QueryContext())

    c_text = Coalesce(f1, sql_type=Types.TEXT)
    assert c_text.sql_type == Types.TEXT


def test_extract_enum_access():
    assert Extract.YEAR == "YEAR"
    assert Extract.MONTH == "MONTH"
