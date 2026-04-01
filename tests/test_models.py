from sql.core.base import QueryContext
from sql.fields.base import ComputedField, Field, ForeignField
from sql.fields.fields import BoolField, IntField, TextField
from sql.functions import Count
from sql.models import TableModel
from sql.queries.base import ValuesQuery

# --- ТЕСТОВЫЕ МОДЕЛИ ---


class User(TableModel):
    _source = "users"
    name = TextField()
    posts_count = ComputedField(expression=lambda: Count(Post.id))


class Post(TableModel):
    _source = "posts"
    user_id = ForeignField(User)
    title = TextField()


class Profile(TableModel):
    user = IntField()


class Base(TableModel):
    is_active = BoolField()


class Admin(User, Base):
    role = TextField()


def test_model_initialization():
    assert User._source == "users"
    assert User._alias == "User"
    assert "id" in User._fields
    assert isinstance(User.id, Field)
    assert User.id.name == "id"


def test_inheritance_fields():
    assert "id" in Profile._fields
    assert "user" in Profile._fields
    assert User.id is not Profile.id
    assert User.id.model == User
    assert Profile.id.model == Profile


def test_model_as_alias():
    U1 = User["u1"]
    assert U1._virtual is True
    assert U1._alias == "User_u1"
    assert U1._source == User

    sql = U1.__sql__(QueryContext())
    assert sql == '"users" AS "User_u1"'


def test_model_iteration():
    fields = list(User)
    field_names = [f.name for f in fields]
    assert "id" in field_names
    assert "name" in field_names


def test_model_equality():
    U1 = User["alias"]
    U2 = User["alias"]
    U3 = User["other"]
    P1 = Profile["alias"]

    assert U1 == U2
    assert U1 != U3
    assert U1 != P1
    assert User != Profile


def test_query_model_factory():
    QM = ValuesQuery(total=100).as_model()

    assert QM._virtual is True
    assert QM._fields["total"] == 100
    assert "sub" in QM._alias
    assert QM._fields["total"].model == QM


def test_deep_inheritance_and_isolation():
    assert "role" in Admin._fields
    assert "name" in Admin._fields
    assert "is_active" in Admin._fields
    assert "name" in User._fields
    assert "role" not in User._fields
    assert Admin.name.model == Admin
    assert User.name.model == User
    assert id(Admin.id) != id(User.id)
    assert id(Admin.id) != id(Base.id)
    assert id(Admin.id) == id(Admin.id)


def test_computed_field():
    field = User._fields["posts_count"]
    context = QueryContext()
    sql = User.posts_count.__sql__(context)
    expr = User.posts_count.expression

    assert callable(field._expression)
    assert expr is not None
    assert isinstance(expr, Count)
    assert Post in field.relations
    assert User in field.relations
    assert 'COUNT("Post"."id")' in sql


def test_computed_field_isolation():
    class AnotherModel(TableModel):
        val = IntField()
        calc = ComputedField(expression=lambda: AnotherModel.val * 2)

    assert AnotherModel.calc.expression is not None
    assert AnotherModel.calc.__sql__(QueryContext()) == '("AnotherModel"."val" * $1)'
