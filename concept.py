from concept.models import City, Post, Store, User
from sql.query import List, Select
from sql.utils import describe

q = Select(Store.name).join(City).filter(Store.is_open_now())
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
