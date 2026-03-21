from __future__ import annotations

from sql.core.functions import Now
from sql.fields.base import ForeignField
from sql.fields.fields import (
    ArrayField,
    DateField,
    DecimalField,
    JsonbField,
    SerialField,
    TextField,
    TimeField,
    TimestampField,
    TimeZoneField,
)
from sql.models import Model


class Cities(Model):
    id = SerialField()
    name = TextField()
    timezone = TimeZoneField()


class Categories(Model):
    id = SerialField()
    parent_id = ForeignField("Self", on_delete=ForeignField.CASCADE)
    name = TextField()


class Shops(Model):
    id = SerialField()
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


class Users(Model):
    id = SerialField()
    name = TextField()
    birth_date = DateField()
    tags = ArrayField(TextField())
    metadata = JsonbField()


class Sales(Model):
    id = SerialField()
    shop_id = ForeignField(Shops, on_delete=ForeignField.CASCADE)
    category_id = ForeignField(Categories, on_delete=ForeignField.CASCADE)
    amount = DecimalField(precision=12, scale=2)
    created_at = TimestampField()


class Stores(Model):
    name = TextField()
    city_id = ForeignField(Cities)
    open_at = TimeField()
    close_at = TimeField()
