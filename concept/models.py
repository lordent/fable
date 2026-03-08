from __future__ import annotations

from sql.core import SqlType, Now
from sql.field import (
    Field, TextField, IntField, SerialField, 
    ArrayField, JSONBField, TimestampField, 
    TimeField, ForeignKey, ReferentialAction, DecimalField, TimeZoneField, DateField,
)
from sql.model import Model


class Cities(Model):
    id = SerialField()
    name = TextField(unique=True)
    timezone = TimeZoneField()

class Categories(Model):
    id = SerialField()
    parent_id = ForeignKey("Self", nullable=True, on_delete=ReferentialAction.CASCADE)
    name = TextField()

class Shops(Model):
    id = SerialField()
    city_id = ForeignKey(Cities, on_delete=ReferentialAction.CASCADE)
    name = TextField()
    open_at = TimeField()
    close_at = TimeField()

    @classmethod
    def is_open_now(cls):
        local_now = Now().at_timezone(Cities.timezone).cast(TimeField)

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

class Users(Model):
    id = SerialField()
    name = TextField()
    birth_date = DateField()
    tags = ArrayField(TextField()) 
    metadata = JSONBField()


class Sales(Model):
    id = SerialField()
    shop_id = ForeignKey(Shops, on_delete=ReferentialAction.CASCADE)
    category_id = ForeignKey(Categories, on_delete=ReferentialAction.CASCADE)
    amount = DecimalField(precision=12, scale=2)
    created_at = TimestampField(auto_now=True)


class Stores(Model):
    name = TextField()
    city_id = ForeignKey(Cities)
    open_at = TimeField()
    close_at = TimeField()
