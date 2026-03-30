from sql.core.functions import Rank
from sql.fields.base import ForeignField
from sql.fields.fields import IntField, TextField
from sql.models import TableModel
from sql.queries.select import GroupMode, Select

from sql.core.aggregates import Count, Sum
from sql.core.base import QueryContext


class User(TableModel):
    _source = "users"
    name = TextField()


class Order(TableModel):
    _source = "orders"
    user_id = ForeignField(User)
    amount = IntField()


def test_select_basic_rendering():
    query = Select(User.name, uid=User.id)
    sql = query.__sql__(QueryContext())

    assert sql.startswith('SELECT "User"."name" AS "name", "User"."id" AS "uid"')
    assert 'FROM "users" AS "User"' in sql


def test_auto_group_by_logic():
    query = Select(User.name, total=Sum(Order.amount)).join(
        Order, on=(User.id == Order.user_id)
    )
    sql = query.__sql__(QueryContext())

    assert 'GROUP BY "User"."name"' in sql
    assert 'GROUP BY "User"."name", SUM(' not in sql


def test_window_function_exclusion_from_group_by():
    sale_rank = Rank().over(order_by=Order.amount.desc())
    query = Select(User.id, rank=sale_rank, total=Sum(Order.amount)).join(Order)

    sql = query.__sql__(QueryContext())
    assert 'GROUP BY "User"."id"' in sql
    assert "OVER" not in sql.split("GROUP BY")[1]


def test_filter_redirection_where_vs_having():
    query = (
        Select(User.name, total=Sum(Order.amount))
        .join(Order)
        .filter(User.id > 10)
        .filter(Sum(Order.amount) > 100)
    )
    sql = query.__sql__(QueryContext())

    assert 'WHERE ("User"."id" > $1)' in sql
    assert 'HAVING (SUM("Order"."amount") > $2)' in sql


def test_summary_rollup_rendering():
    query = (
        Select(User.name, total=Sum(Order.amount))
        .join(Order)
        .summary(User.name, mode=GroupMode.ROLLUP)
    )
    sql = query.__sql__(QueryContext())

    assert 'GROUP BY ROLLUP("User"."name")' in sql
    assert 'GROUP BY "User"."name", ROLLUP' not in sql


def test_order_by_with_aggregate_propagation():
    query = Select(sum=Sum(Order.amount)).order_by(User.name)
    sql = query.__sql__(QueryContext())

    assert 'GROUP BY "User"."name"' in sql
    assert 'ORDER BY "User"."name" ASC' in sql


def test_select_from_subquery_as_model():
    sub = Select(User.id, total=Sum(Order.amount)).group_by(User.id).as_model()

    query = Select(sub.id, sub.total).filter(sub.total > 1000)
    sql = query.__sql__(QueryContext())

    assert 'FROM (SELECT "User_s1"."id" AS "id"' in sql
    assert 'WHERE ("sub' in sql
    assert 'GROUP BY "User_s1"."id"' in sql


def test_complex_join_chains():
    class City(TableModel):
        _source = "cities"
        name = TextField()

    query = Select(User.name, City.name).join(Order).join(City, on=(Order.amount > 0))
    sql = query.__sql__(QueryContext())

    assert 'FROM "users" AS "User"' in sql
    assert 'JOIN "orders" AS "Order"' in sql
    assert 'JOIN "cities" AS "City"' in sql


def test_lock_rendering_with_of_clause():
    query = Select(User.name).join(Order).for_update(User, nowait=True)
    sql = query.__sql__(QueryContext())
    assert 'FOR UPDATE OF "User" NOWAIT' in sql


def test_aggregate_in_complex_expression_group_by():
    query = Select(double_amount=Order.amount * 2, cnt=Count(Order.id))
    sql = query.__sql__(QueryContext())
    assert 'GROUP BY ("Order"."amount" * $1)' in sql


def test_coalesce_with_aggregates_group_by():
    val = Sum(Order.amount).default(0)
    query = Select(User.name, revenue=val).join(Order)
    sql = query.__sql__(QueryContext())
    assert 'GROUP BY "User"."name"' in sql
    assert 'GROUP BY "User"."name", COALESCE' not in sql


def test_limit_offset_rendering():
    query = Select(User.name).limit(10).offset(5)
    sql = query.__sql__(QueryContext())
    assert sql.endswith("LIMIT 10 OFFSET 5")
