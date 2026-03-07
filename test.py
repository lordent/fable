class User(Model):
    first_name = TextField()
    last_name = TextField()
    age = IntegerField()


class Topic(Model):
    name = TextField()
    path = LTreeField()
    parent = ForeignKey("Self", nullable=True)


class Post(Model):
    user = ForeignKey(User)
    Topic = ForeignKey(Topic)
    title = TextField()
    content = TextField()


# --- Сценарии ---


async def test_scenarios():
    # Получить всех однофамильцев
    Select(*User).join(
        User["u2"], User["u2"].last_name == User.last_name & User["u2"].id != User.id
    )

    # Получить всех пользователей и их посты, построить бредкрамбы для постов
    Select(
        *User,
        posts=List(*Post, breadcrumbs=PathList(Topic.id, Topic.name)),
    ).join(Post).join(Topic)

    # Поиск тем
    Select(Topic.id, Topic.name, breadcrumbs=PathList(Topic.id, Topic.name)).filter(
        Topic.name % ("вела прогулки")
    )


async def main():
    url = os.getenv("DATABASE_URL", "postgresql://user:password@db:5432/sql_builder")
    engine = Engine(url)
    await engine.connect()

    # await setup_data(engine)
    await test_scenarios(engine)

    await engine.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
