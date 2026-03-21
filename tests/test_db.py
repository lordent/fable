import pytest

from sql.core.aggregates import Avg, Count, Sum
from sql.fields.fields import DecimalField
from sql.queries.select import Select
from sql.queries.values import Item, List

from .conftest import Categories, Cities, Sales, Shops, Users


@pytest.mark.asyncio
async def test_select_users_integration():
    users = await Select(Users.id, Users.name).order_by(Users.id)
    assert len(users) > 0
    assert users[0]["name"] == "Александр"


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

    res = await query
    if res:
        assert res[0]["shops"] == []
