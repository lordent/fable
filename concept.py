from sql.core import Now
from sql.field import ArrayField, ForeignKey, TextField, TimeField, TimeZoneField
from sql.model import Model
from sql.query import List, Q, Select
from sql.utils import describe


# --- Тестовая схема ---
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


def is_open_now(store: type[Store], city: type[City]) -> Q:
    local_now = Now().at_timezone(city.timezone).cast(TimeField)

    # 2. Обычная смена (08:00 - 22:00)
    normal = (
        (store.open_at < store.close_at)
        & (local_now >= store.open_at)
        & (local_now <= store.close_at)
    )

    # 3. Ночная смена (22:00 - 04:00)
    night = (store.open_at > store.close_at) & (
        (local_now >= store.open_at) | (local_now <= store.close_at)
    )

    return normal | night


q = Select(Store.name).join(City).filter(is_open_now(Store, City))
print(q.prepare()[0])

# --- Пример сложного сценария ---

print(User.id, User["u2"].id)
print(Post.id, Post["p2"].id)

u2 = User["u2"]
q = (
    Select(*User)
    .values(post_id=Post.id)
    .join(Post)
    .join(u2, on=(u2.last_name == User.last_name) & (u2.id != User.id))
    .filter(User.last_name % "Ivanov")
)

print(q.prepare())

stats_sub = Select(Post.user_id, count=Post.id).filter(Post.title == "Foo").as_table()
print(describe(stats_sub.count))
print(describe(stats_sub.user_id))

q = (
    Select(User.id, stats_sub.count)
    .join(stats_sub, on=stats_sub.user_id == User.id)
    .filter(User.last_name % "Bar")
)

print(q)
print(q.prepare())

q = Select(
    *User,
    posts=List(*Post),
).join(Post)

print(q)
print(q.prepare())


# 1. Найти посты, где есть тег 'sql'
q = Select(*Post).filter(Post.tags >= ["sql"])
# WHERE "Post"."tags" @> $1 (параметром уйдет ['sql'])
print(q)
print(q.prepare())

# 2. Найти посты, где теги пересекаются со списком
q = Select(*Post).filter(Post.tags * ["news", "tech"])
# WHERE "Post"."tags" && $1
print(q)
print(q.prepare())
# 3. Достать первый тег (Post.tags[1] в SQL)
q = Select(foo=Post.tags[0])
print(q)
print(q.prepare())
