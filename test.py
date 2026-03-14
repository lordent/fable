from concept.models import Categories, Cities, Sales, Shops, Users
from sql.aggs import Count, Sum
from sql.fields import (
    ArrayField,
    CharField,
    ComputedField,
    DecimalField,
    ForeignField,
    IntField,
    TextField,
)
from sql.model import Model
from sql.queries.base import List
from sql.queries.select import Select


class User(Model):
    name = CharField(max_length=256)


class Author(Model):
    name = CharField(max_length=256)


class Book(Model):
    title = CharField(max_length=256)
    author = ForeignField(Author)
    tags = ArrayField(CharField(max_length=256))
    reviews = ComputedField(lambda: Count(Review.id))


class Review(Model):
    book = ForeignField(Book)
    user = ForeignField(User)
    text = TextField(max_length=700)
    likes = IntField()
    display = ComputedField(lambda: t"{Book.title} - {User.name}")


# print(Review.book, Review.book.model, Review.book.name)
# print(Book.id, Book.id.model, Book.id.name)

# q = Review.book == Book.id
# print(q.relations)
# print(Review.__sql__([]))


# class Sub(QueryModel):
#     text: Annotated[TextField, Review.text]
#     book: Annotated[SerialField, Review.id]


# sub: Sub = Select(*Review).filter(Review.user == 2).as_model()
# query = Select(Book.title, text=sub.text).join(sub, on=(Book.id == sub.book))
# print(query)

# all_users = Select(User.id)
# print(all_users)

# query = Select(*Review).filter(Review.user == all_users)
# print(query)

# query = Select(User.name).join(Review).filter(Review.likes > 100)

# print(list(query.prepare()))

query = Select(Shops.name, city=List(*Cities)).join(Cities).filter(Shops.is_open_now())
print(list(query.prepare()))

query = Select(
    Users.name,
    age=Users.birth_date.age,
    birth_year=Users.birth_date.year,
    birth_month=Users.birth_date.month,
).order_by(Users.birth_date.desc())

print(list(query.prepare()))

query = (
    Select(
        shop=Shops.name,
        category=Categories.name,
        total_sales=Sum(Sales.amount) >> DecimalField(12, 2),
    )
    .join(Sales)
    .join(Categories)
    .summary(Shops.name, Categories.name)
    .order_by(Shops.name.asc(), Categories.name.asc())
)

print(list(query.prepare()))

query = Select(*Review)
print(list(query.prepare()))
