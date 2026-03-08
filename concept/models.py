from sql.core import Now
from sql.field import ArrayField, ForeignKey, TextField, TimeField, TimeZoneField
from sql.model import Model


class User(Model):
    last_name = TextField()


class Post(Model):
    user_id = ForeignKey(User)
    title = TextField()
    tags = ArrayField(TextField)


class City(Model):
    name = TextField()
    timezone = TimeZoneField()


class Store(Model):
    name = TextField()
    city_id = ForeignKey(City)
    open_at = TimeField()
    close_at = TimeField()

    @classmethod
    def is_open_now(cls):
        local_now = Now().at_timezone(City.timezone).cast(TimeField)

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
