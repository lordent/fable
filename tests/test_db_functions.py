import pytest

from sql.fields.fields import DecimalField
from sql.functions import Avg
from sql.queries.select import Select

from .conftest import Cities, Shops, Users


@pytest.mark.asyncio
async def test_trigram_search_integration():
    query = Select(Users.id, Users.first_name).filter(
        Users.first_name.dist("аликсандр") < 0.7
    )
    res = await query
    assert any(row["first_name"] == "Александр" for row in res)


@pytest.mark.asyncio
async def test_shops_open_now_integration():
    query = (
        Select(Shops.name, city=Cities.name).join(Cities).filter(Shops.is_open_now())
    )
    res = await query
    assert isinstance(res, list)


@pytest.mark.asyncio
async def test_date_fields_extract_integration():
    query = Select(
        birth_year=Users.birth_date.year, birth_month=Users.birth_date.month
    ).filter(Users.first_name == "Александр")

    res = await query
    assert len(res) == 1
    row = res[0]

    assert row["birth_year"] == 1998
    assert row["birth_month"] == 5

    query = Select(name=Users.first_name, age=Users.birth_date.age).filter(
        Users.first_name == "Александр"
    )

    res = await query
    assert res[0]["age"] >= 25

    query = Select(Users.first_name).filter(Users.birth_date.month == 8)

    res = await query
    assert len(res) == 1
    assert res[0]["first_name"] == "Мария"

    query = (
        Select(tag=Users.tags, avg_age=Avg(Users.birth_date.age) >> DecimalField(10, 1))
        .filter(Users.tags.contains("tech"))
        .group_by(Users.tags)
    )

    res = await query
    assert any(20 < r["avg_age"] < 30 for r in res)
