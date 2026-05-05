import datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from sql.core.datatypes import Types
from sql.core.raw import Raw, Value
from sql.queries.raw import RawQuery
from sql.queries.select import Select
from tests import TestApplication
from tests.conftest import Sales, Users


def test_impossible():
    with pytest.raises(TypeError):
        Raw(t"{Users.tags} || {[]}")

    with pytest.raises(TypeError):
        Raw(t"{Users.tags} || {...}")


def test_empty_list():
    assert (
        Raw(t"{Users.tags} || {Value([], Types.TEXT[:])}")
        == '("Users"."tags" || $1::TEXT[])'
    )


@pytest.mark.asyncio
async def test_select_users():
    days = 2
    tags = ["foo"]
    metadata = {"tags": tags}
    query = Select(
        Users.id,
        Users.birth_date,
        Users.tags,
        Users.metadata,
        first_name=Raw(t"{Users.first_name} || {' VIP'}"),
        append_tags=Raw(t"{Users.tags} || {tags}"),
        merge_metadata=Raw(t"{Users.metadata} || {metadata}"),
        birth_date_tomorrow=Raw(t"{Users.birth_date} + ({days} * INTERVAL '1 day')")
        >> Types.DATE,
    ).order_by(Users.id)
    users = await query
    assert len(users) > 0
    assert users[0]["merge_metadata"]["tags"] == tags
    assert users[0]["tags"] + tags == users[0]["append_tags"]
    assert users[0]["first_name"] == "Александр VIP"
    assert (users[0]["birth_date_tomorrow"] - users[0]["birth_date"]).days == days

    now = datetime.datetime.now(datetime.UTC)
    public_id = uuid4()

    days = 4
    query = Select(
        Sales.amount,
        increase_amount=Raw(t"{Sales.amount} + {Decimal('100')} + {100.20}")
        >> Types.NUMERIC,
        now=Raw(t"{now} + ({days} * INTERVAL '1 day')"),
        public_id=Raw(t"{public_id}"),
    ).order_by(Sales.id)

    for row in await query:
        assert row["amount"] + Decimal("100") + Decimal("100.20") == row[
            "increase_amount"
        ].quantize(Decimal("0.01"))
        assert (row["now"] - now).days == days
        assert row["public_id"] == public_id


@pytest.mark.asyncio
async def test_raw_query():
    query = RawQuery(
        t"""SELECT
            {Users.id}, {Users.first_name}, {Users.last_name},
            {Users.birth_date}
        FROM {Users}
        JOIN {Users["u2"]} ON {Users["u2"].last_name} = {Users.last_name}
        WHERE {Users["u2"].id} <> {Users.id}
        ORDER BY {Users.id}"""
    )

    assert isinstance(query.app, TestApplication)

    assert [dict(r) for r in await query] == [
        {
            "id": 1,
            "first_name": "Александр",
            "last_name": "Иванов",
            "birth_date": datetime.date(1998, 5, 15),
        },
        {
            "id": 2,
            "first_name": "Алексанр",
            "last_name": "Иванов",
            "birth_date": datetime.date(1993, 11, 10),
        },
    ]


def test_injection():
    danger_value = "(SELECT TRUNCATE 'users')"

    expression = Raw(t"{Users.birth_date} + {danger_value}")
    assert ['("Users"."birth_date" + $1::TEXT)', danger_value] == list(
        expression.prepare()
    )
