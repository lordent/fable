from decimal import Decimal

import pytest

from sql.functions import Count, Sum
from sql.queries.select import Select

from .conftest import Categories, Sales, Users


@pytest.mark.asyncio
async def test_invert():
    query = (
        Select(Users.full_name)
        .filter(~(Users.first_name == "Александр"))
        .order_by(Users.first_name)
    )

    res = await query
    names = [r["full_name"] for r in res]

    assert "Александр Иванов" not in names
    assert "Алекс Петров" in names
    assert "Мария Петрова" in names

    condition = (Users.first_name == "Александр") & (Users.last_name == "Иванов")

    query = Select(Users.first_name).filter(~condition)

    res = await query
    for row in res:
        if row["first_name"] == "Александр":
            assert row["last_name"] != "Иванов"

    query = (
        Select(Categories.name, total=Sum(Sales.amount))
        .join(Sales)
        .group_by(Categories.name)
        .filter(~(Sum(Sales.amount) > 5000))
    )

    res = await query
    assert len(res) == 1
    assert res[0]["name"] == "Бижутерия"
    assert res[0]["total"] <= 5000

    query = Select(Users.first_name).filter(~~(Users.first_name == "Мария"))

    res = await query
    assert len(res) == 1
    assert res[0]["first_name"] == "Мария"


@pytest.mark.asyncio
async def test_ne_with_value_integration():
    query = (
        Select(Users.first_name)
        .filter(Users.first_name != "Александр")
        .order_by(Users.first_name)
    )

    res = await query
    names = [r["first_name"] for r in res]

    assert "Александр" not in names
    assert "Алексанр" in names
    assert "Мария" in names

    query = Select(Categories.name).filter(Categories.parent_id != None)

    res = await query
    names = [r["name"] for r in res]

    assert "Электроника" not in names
    assert "Гаджеты" in names
    assert "Аксессуары" in names

    query = (
        Select(Users.last_name).group_by(Users.last_name).filter(Count(Users.id) != 1)
    )

    res = await query

    assert len(res) == 1
    assert res[0]["last_name"] == "Иванов"

    query_2 = (
        Select(Users.last_name)
        .group_by(Users.last_name)
        .filter(Count(Users.id) != 2)
        .order_by(Users.last_name)
    )
    res_2 = await query_2
    assert len(res_2) == 2
    assert res_2[0]["last_name"] == "Петров"
    assert res_2[1]["last_name"] == "Петрова"


@pytest.mark.asyncio
async def test_array_integration():
    query = (
        Select(Users.first_name)
        .filter(Users.tags.overlap(["tech", "vip"]))
        .order_by(Users.first_name)
    )

    res = await query
    names = [r["first_name"] for r in res]

    assert "Александр" in names
    assert "Алекс" in names
    assert "Мария" in names
    assert "Алексанр" not in names

    query = Select(Users.first_name).filter(
        Users.metadata["last_login"] == "2023-10-01"
    )

    res = await query
    assert len(res) == 1
    assert res[0]["first_name"] == "Александр"

    query = Select(Users.first_name).filter(Users.metadata.text("points") == "100")

    res = await query
    assert len(res) == 1
    assert res[0]["first_name"] == "Алексанр"

    query = Select(Users.first_name).filter(Users.metadata.text("club") == "gold")
    assert (await query)[0]["first_name"] == "Мария"


@pytest.mark.asyncio
async def test_arithmetics():
    query = Select(new_amount=Sales.amount + 100).filter(Sales.amount == 5000)
    res = await query
    assert res[0]["new_amount"] == Decimal("5100.00")

    query = Select(young_age=Users.birth_date.age - 10).filter(
        Users.first_name == "Александр"
    )
    res = await query
    assert 15 <= res[0]["young_age"] <= 17  # Зависит от текущего месяца

    query = Select(double=Sales.amount * 2).filter(Sales.amount == 1500)
    res = await query
    assert res[0]["double"] == Decimal("3000.00")

    query = Select(half=Sales.amount / 2).filter(Sales.amount == 2000)
    res = await query
    assert res[0]["half"] == Decimal("1000.00")

    query = Select(is_even=Users.id % 2).filter(Users.id == 2)
    res = await query
    assert res[0]["is_even"] == 0


@pytest.mark.asyncio
async def test_order_by_nulls_logic_integration():
    query_first = (
        Select(Categories.name, Categories.parent_id)
        .order_by(Categories.parent_id.asc(nulls_first=True))
        .limit(1)
    )
    res_first = await query_first
    assert res_first[0]["parent_id"] is None

    query_last = Select(Categories.name, Categories.parent_id).order_by(
        Categories.parent_id.asc(nulls_first=False)
    )
    res_last = await query_last

    assert res_last[-1]["parent_id"] is None
    assert res_last[0]["parent_id"] is not None

    query_desc_first = Select(Categories.parent_id).order_by(
        Categories.parent_id.desc(nulls_first=True)
    )
    res_desc_first = await query_desc_first
    assert res_desc_first[0]["parent_id"] is None
