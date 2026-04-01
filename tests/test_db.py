import datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from sql.core.case import Case
from sql.core.raw import Raw, Ref
from sql.core.types import Types
from sql.fields.fields import DecimalField
from sql.functions import Avg, Count, RowNumber, Sum
from sql.queries.select import Select
from sql.queries.values import Item, List

from .conftest import Categories, Cities, Sales, Shops, Users


@pytest.mark.asyncio
async def test_select_users_integration():
    query = Select(
        Users.id,
        Users.name,
        Users.birth_date,
        birth_date_tomorrow=Raw.Scalar(t"{Users.birth_date} + INTERVAL '{1} day'")
        >> Types.DATE,
    ).order_by(Users.id)
    users = await query
    assert len(users) > 0
    assert users[0]["name"] == "Александр"
    assert (users[0]["birth_date_tomorrow"] - users[0]["birth_date"]).days == 1

    now = datetime.datetime.now(datetime.UTC)
    public_id = uuid4()

    query = Select(
        Sales.amount,
        increase_amount=Raw.Scalar(t"{Sales.amount} + {Decimal('100')} + {100.20}"),
        now=Raw.Scalar(t"{now} + INTERVAL '{1} day'"),
        public_id=Raw.Scalar(t"{public_id}"),
    ).order_by(Sales.id)
    for row in await query:
        assert (
            row["amount"] + Decimal("100") + Decimal("100.20") == row["increase_amount"]
        )
        assert (row["now"] - now).days == 1
        assert row["public_id"] == public_id


@pytest.mark.asyncio
async def test_trigram_search_integration():
    query = Select(Users.id, Users.name).filter(Users.name.dist("аликсандр") < 0.7)
    res = await query
    assert any(row["name"] == "Александр" for row in res)


@pytest.mark.asyncio
async def test_shops_open_now_integration():
    query = (
        Select(Shops.name, city=Cities.name).join(Cities).filter(Shops.is_open_now())
    )
    res = await query
    assert isinstance(res, list)


@pytest.mark.asyncio
async def test_analytics_rollup_integration():
    query = (
        Select(
            shop=Shops.name,
            total_sales=Sum(Sales.amount) >> DecimalField(12, 2),
        )
        .join(Sales)
        .summary(Shops.name)
        .order_by(Shops.name.asc())
    )
    res = await query

    summary_row = next(row for row in res if row["shop"] is None)
    assert summary_row["total_sales"] > 0


@pytest.mark.asyncio
async def test_frankenstein_integration():
    premium_cities_sub = (
        Select(Cities.id)
        .join(Users, on=(Users.id > 0))
        .filter(
            Users.tags.contains("premium"),
            Count(Users.id) >= 1,
        )
        .group_by(Cities.id)
        .as_model()
    )

    query = (
        Select(
            city_name=Cities.name,
            avg_check=Avg(Sales.amount) >> DecimalField(10, 2),
        )
        .join(Shops)
        .join(Sales)
        .filter(Cities.id == premium_cities_sub.id)
        .filter(Avg(Sales.amount) > 100)
        .group_by(Cities.name)
    )

    res = await query
    assert len(res) > 0
    assert res[0]["city_name"] in ["Москва", "Владивосток", "Калининград"]

    query = (
        Select(
            city_name=Cities.name,
            avg_check=Avg(Sales.amount) >> DecimalField(10, 2),
        )
        .join(Shops)
        .join(Sales)
        .join(premium_cities_sub, on=(Cities.id == premium_cities_sub.id))
        .filter(Avg(Sales.amount) > 100)
        .group_by(Cities.name)
    )

    assert await query == res


@pytest.mark.asyncio
async def test_json_structures_integration():
    query_list = (
        Select(
            city_name=Cities.name,
            shops_list=List(
                Shops.name,
                Shops.open_at,
                is_open_now=Shops.is_open_now(),
                meta=Item(
                    is_open_now=Shops.is_open_now(),
                ),
            ),
        )
        .join(Shops)
        .group_by(Cities.name)
        .order_by(Cities.name)
    )

    res_list = await query_list

    assert len(res_list) > 0
    vlad = next(row for row in res_list if row["city_name"] == "Владивосток")

    assert isinstance(vlad["shops_list"], list)
    assert len(vlad["shops_list"]) == 1
    assert vlad["shops_list"][0]["name"] == "Влд Утренний кофе"
    assert "open_at" in vlad["shops_list"][0]
    assert isinstance(vlad["shops_list"][0]["is_open_now"], bool)
    assert (
        vlad["shops_list"][0]["is_open_now"]
        == vlad["shops_list"][0]["meta"]["is_open_now"]
    )

    query_item = (
        Select(amount=Sales.amount, category_info=Item(Categories.id, Categories.name))
        .join(Categories)
        .filter(Sales.amount == 5000)
    )

    res_item = await query_item

    assert len(res_item) == 1
    row = res_item[0]

    assert row["amount"] == 5000
    assert isinstance(row["category_info"], dict)
    assert row["category_info"]["id"] == 3
    assert row["category_info"]["name"] == "Гаджеты"


@pytest.mark.asyncio
async def test_json_list_empty_coalesce():
    query = (
        Select(city=Cities.name, shops=List(Shops.name))
        .join(Shops, on=(Shops.id < 0))
        .group_by(Cities.name)
        .limit(1)
    )

    if res := await query:
        assert res[0]["shops"] == []


@pytest.mark.asyncio
async def test_join():
    res = Sum(Sales.amount) / Sum(Sales.amount)

    query = (
        Select(
            Categories.name,
            sales_count=Count(Categories.id),
            total_revenue=Sum(Sales.amount).default(0),
        )
        .join(Sales, strategy=Select.Join.RIGHT)
        .order_by(
            Ref.Aggregate("sales_count").desc(),
        )
    )

    res = [dict(record) for record in await query]
    assert res == [
        {"name": "Гаджеты", "sales_count": 3, "total_revenue": Decimal("10000.00")},
        {"name": "Бижутерия", "sales_count": 1, "total_revenue": Decimal("1500.00")},
    ]


@pytest.mark.asyncio
async def test_union():
    search_term = "Гаджет"
    min_amount_expensive = 4000.00
    min_amount_cheap = 1000.00

    q1 = (
        Select(Sales.id, Sales.amount, category_name=Categories.name)
        .join(Categories)
        .filter(
            Categories.name.similar(search_term),
            Sales.amount >= min_amount_expensive,
        )
    )

    q2 = (
        Select(Sales.id, Sales.amount, category_name=Categories.name)
        .join(Categories)
        .filter(
            Categories.name == "Бижутерия",
            Sales.amount >= min_amount_cheap,
        )
    )

    combined_query = q1 & q2

    final_model = combined_query.as_model()
    query = Select(final_model.category_name, final_model.amount).order_by(
        final_model.amount.desc()
    )

    assert [dict(record) for record in await query] == [
        {"category_name": "Бижутерия", "amount": Decimal("1500.00")}
    ]


@pytest.mark.asyncio
async def test_recursive():
    with (
        Select(Categories.id, Categories.name, level=Raw.Scalar(0))
        .filter(Categories.parent_id == None)
        .recursive() as Tree
    ):
        Tree &= Select(Categories.id, Categories.name, level=Tree.level + 1).join(Tree)

    query = Select(*Tree).order_by(Tree.id.asc())

    assert [dict(record) for record in await query] == [
        {"id": 1, "name": "Электроника", "level": 0},
        {"id": 2, "name": "Аксессуары", "level": 1},
        {"id": 3, "name": "Гаджеты", "level": 2},
        {"id": 4, "name": "Бижутерия", "level": 2},
        {"id": 5, "name": "Одежда", "level": 0},
    ]


@pytest.mark.asyncio
async def test_case():
    category_label = (
        Case(default="Дешево")
        .when(Sales.amount > 5000, "Премиум")
        .when(Sales.amount > 1000, "Средне")
    )

    query = Select(Sales.id, segment=category_label).order_by(Sales.id)

    assert [dict(record) for record in await query] == [
        {"id": 1, "segment": "Средне"},
        {"id": 2, "segment": "Средне"},
        {"id": 3, "segment": "Средне"},
        {"id": 4, "segment": "Средне"},
    ]

    points_calc = Sum(
        Case(default=0)
        .when(Users.tags.contains("premium"), Sales.amount * 2)
        .when(Users.tags.contains("vip"), Sales.amount * 3)
    )

    query = Select(Users.name, total_points=points_calc).order_by(Users.name)

    assert [dict(record) for record in await query] == [
        {"name": "Алекс", "total_points": Decimal("0")},
        {"name": "Александр", "total_points": Decimal("23000.00")},
        {"name": "Алексанр", "total_points": Decimal("0")},
        {"name": "Мария", "total_points": Decimal("23000.00")},
    ]


@pytest.mark.asyncio
async def test_having_with_expression_dependency():
    query = (
        Select(Users.name, total=Sum(Sales.amount))
        .filter(Sum(Sales.amount) > (Users.id * 1000))
        .group_by(Users.id)
        .order_by(Users.name)
    )

    res = await query
    assert len(res) > 0
    assert any(r["name"] == "Александр" for r in res)


@pytest.mark.asyncio
async def test_window_functions_integration():
    running_sum = Sum(Sales.amount).over(
        partition_by=Sales.shop_id,
        order_by=Sales.id,
    )[1:0]

    query = Select(Sales.id, Sales.amount, two_rows_sum=running_sum).order_by(
        Sales.shop_id, Sales.id
    )

    res = await query

    assert len(res) > 0

    row1 = next(r for r in res if r["id"] == 1)
    row2 = next(r for r in res if r["id"] == 2)

    assert row1["two_rows_sum"] == 5000
    assert row2["two_rows_sum"] == 7000


@pytest.mark.asyncio
async def test_row_number_simple():
    rn = RowNumber().over(order_by=Sales.amount.desc())
    query = Select(Sales.id, position=rn).limit(1)
    res = await query

    assert res[0]["position"] == 1


@pytest.mark.asyncio
async def test_window_following_slice():
    next_diff = Sum(Sales.amount).over(order_by=Sales.id)[0:-1]
    query = Select(Sales.id, Sales.amount, pair_sum=next_diff).order_by(Sales.id)
    assert [dict(record) for record in await query] == [
        {"id": 1, "amount": Decimal("5000.00"), "pair_sum": Decimal("7000.00")},
        {"id": 2, "amount": Decimal("2000.00"), "pair_sum": Decimal("3500.00")},
        {"id": 3, "amount": Decimal("1500.00"), "pair_sum": Decimal("4500.00")},
        {"id": 4, "amount": Decimal("3000.00"), "pair_sum": Decimal("3000.00")},
    ]


@pytest.mark.asyncio
async def test_window_range_mode():
    window = Sum(Sales.amount).over(order_by=Sales.amount).range[...:0]
    query = Select(Sales.id, Sales.amount, range_sum=window).order_by(Sales.amount)
    sql = str(query)

    assert "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" in sql

    res = await query
    assert len(res) > 0
