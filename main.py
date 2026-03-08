import asyncio

from concept.models import User
from sql.db import Config, Engine
from sql.query import Select

Engine(
    Config(
        dsn="postgresql://user:password@db:5432/sql_builder",
        apps=["concept"],
        debug=True,
    )
)


async def run_test():
    print("--- Выполняем простой запрос ---")
    users = await Select(User.id, User.last_name).limit(5)
    for row in users:
        print(f"User: {row['id']} - {row['last_name']}")

    # 3. Транзакция с блокировкой (FOR UPDATE)
    print("\n--- Транзакция + FOR UPDATE ---")
    async with User.atomic():
        target_user = await Select(User.id).filter(User.id == 1).for_update(nowait=True)
        print(f"Заблокирован юзер: {target_user}")

    # # 4. EXPLAIN ANALYZE
    # print("\n--- План запроса ---")
    # plan = await Explain(
    #     Select(User.id).filter(User.last_name % "Ivanov"), analyze=True
    # )
    # for row in plan:
    #     print(row[0])


if __name__ == "__main__":
    asyncio.run(run_test())
