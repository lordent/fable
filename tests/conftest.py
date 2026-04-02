import asyncio

import asyncpg
import pytest
import pytest_asyncio

from sql.core.expressions import Expression
from sql.db import Config, Engine, _sessions_ctx
from sql.fields.base import ForeignField
from sql.fields.fields import (
    ArrayField,
    DateField,
    DecimalField,
    JsonbField,
    TextField,
    TimeField,
    TimestampField,
    TimeZoneField,
)
from sql.functions import Now
from sql.models import TableModel


class FakeField(Expression):
    def __init__(self, name, sql_type=None):
        super().__init__(sql_type=sql_type)
        self.name = name

    def __sql__(self, context):
        return f'"{self.name}"'


@pytest.fixture(scope="session", autouse=True)
async def setup_db():
    cfg = Config(
        dsn="postgresql://user:password@db:5432/sql_builder",
        apps=["test"],
        debug=True,
    )

    Engine(cfg)

    with open("./tests/dump.sql") as f:
        sql = f.read()

    conn = await asyncpg.connect(cfg.dsn)
    try:
        await conn.execute(sql)
        await conn.execute("ANALYZE")
    finally:
        await conn.close()

    await asyncio.sleep(0)


@pytest_asyncio.fixture(autouse=True)
async def db_cleanup():
    token = _sessions_ctx.set(None)
    yield
    _sessions_ctx.reset(token)


class Cities(TableModel):
    name = TextField()
    timezone = TimeZoneField()


class Categories(TableModel):
    parent_id = ForeignField("Self", on_delete=ForeignField.CASCADE)
    name = TextField()


class Shops(TableModel):
    city_id = ForeignField(Cities, on_delete=ForeignField.CASCADE)
    name = TextField()
    open_at = TimeField()
    close_at = TimeField()

    @classmethod
    def is_open_now(cls):
        local_now = Now().at_timezone(Cities.timezone) >> TimeField()

        # 2. Обычная смена (08:00 - 22:00)
        normal = (
            (cls.open_at < cls.close_at)
            & (local_now >= cls.open_at)
            & (local_now <= cls.close_at)
        )

        # 3. Ночная смена (22:00 - 04:00)
        night = (cls.open_at > cls.close_at) & (
            (local_now >= cls.open_at) | (local_now <= cls.close_at)
        )

        return normal | night


class Users(TableModel):
    first_name = TextField()
    last_name = TextField()
    birth_date = DateField()
    tags = ArrayField(TextField())
    metadata = JsonbField()


class Sales(TableModel):
    shop_id = ForeignField(Shops, on_delete=ForeignField.CASCADE)
    category_id = ForeignField(Categories, on_delete=ForeignField.CASCADE)
    amount = DecimalField(precision=12, scale=2)
    created_at = TimestampField()


class Stores(TableModel):
    name = TextField()
    city_id = ForeignField(Cities)
    open_at = TimeField()
    close_at = TimeField()
