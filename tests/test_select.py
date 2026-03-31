from sql.core.base import QueryContext
from sql.fields.base import ForeignField
from sql.fields.fields import IntField, TextField
from sql.functions import Count, Rank, Sum
from sql.models import TableModel
from sql.queries.select import GroupMode, Select


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


def test_self_join_aliases_and_params():
    Manager = User["m"]
    query = (
        Select(User.name, manager_name=Manager.name)
        .join(Manager, on=(User.id == Manager.id))
        .filter(User.name == "Worker")
        .filter(Manager.name == "Boss")
    )
    sql, *params = query.prepare()
    assert '"users" AS "User"' in sql
    assert '"users" AS "User_m"' in sql
    assert params == ["Worker", "Boss"]


def test_jsonb_build_complex_nesting():
    order_item = Select.Item(id=Order.id, val=Order.amount)
    orders_list = Select.List(order_item=order_item)

    query = Select(User.name, user_orders=orders_list).join(Order)
    sql = query.__sql__(QueryContext())

    assert "JSONB_BUILD_OBJECT" in sql
    assert "JSONB_AGG" in sql
    assert "COALESCE" in sql
    assert 'GROUP BY "User"."name"' in sql


def test_empty_in_clause_safety():
    query = Select(User.name).filter(User.id == [])
    sql = query.__sql__(QueryContext())

    assert "(1=0)" in sql
    assert "IN ()" not in sql


def test_triple_nested_subquery_aliases():
    L3 = Select(User.id, User.name).as_model()
    L2 = Select(L3.id, L3.name).filter(L3.id > 10).as_model()
    query = Select(L2.name).filter(L2.id < 100)
    sql = query.__sql__(QueryContext())

    assert "User_s2" in sql
    assert L3._alias in sql
    assert L2._alias in sql


def test_template_to_concat_conversion():
    tpl = t"User: {User.name} (ID: {User.id})"
    query = Select(info=tpl, name=User.name, id=User.id)
    sql, *params = query.prepare()

    assert params == ["User: ", " (ID: ", ")"]
    assert '($1 || "User"."name" || $2 || "User"."id" || $3) AS "info"' in sql


def test_array_operator_typing():
    query = Select(User.id).filter(User.id == [1, 2, 3])
    sql, *params = query.prepare()

    assert "= ANY($1::BIGSERIAL[])" in sql
    assert params == [[1, 2, 3]]
