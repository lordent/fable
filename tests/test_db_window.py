from decimal import Decimal

import pytest

from sql.functions import Count, Lag, RowNumber, Sum
from sql.queries.select import Select
from tests.conftest import Sales, Shops


@pytest.mark.asyncio
async def test_window_functions_integration():
    running_sum = (
        Sum(Sales.amount)
        .over(
            partition_by=Sales.shop_id,
            order_by=Sales.id,
        )
        .rows[1:0]
    )

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


@pytest.mark.asyncio
async def test_filtered_aggregate_integration():
    query = (
        Select(
            Shops.name,
            total_count=Count(Sales.id),
            big_sales_count=Count(Sales.id).filter(Sales.amount > 3000),
        )
        .join(Sales)
        .group_by(Shops.name)
        .order_by(Shops.name)
    )

    res = await query

    msk = next(r for r in res if r["name"] == "Мск 24/7 Гаджеты")
    assert msk["total_count"] == 2
    assert msk["big_sales_count"] == 1


@pytest.mark.asyncio
async def test_lag_function_integration():
    lag_node = Lag(Sales.amount, offset=1, default=0)
    assert lag_node.sql_type == Sales.amount.sql_type
    query = Select(
        current_amount=Sales.amount,
        prev_amount=Lag(Sales.amount, 1, Decimal("0.00")).over(order_by=Sales.id),
    ).order_by(Sales.id)

    res = await query

    assert res[0]["current_amount"] == Decimal("5000.00")
    assert res[0]["prev_amount"] == Decimal("0.00")
    assert res[1]["current_amount"] == Decimal("2000.00")
    assert res[1]["prev_amount"] == Decimal("5000.00")

    query = Select(prev_prev=Lag(Sales.amount, 2).over(order_by=Sales.id)).order_by(
        Sales.id
    )

    res = await query
    assert res[2]["prev_prev"] == Decimal("5000.00")
