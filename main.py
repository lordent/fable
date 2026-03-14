import asyncio

from sql.db import Config, Engine

# from sql.queries.dml import Delete, Update
from concept.models import Categories, Cities, Sales, Shops, Users
from sql.aggs import Count, Sum, concat
from sql.fields import DecimalField
from sql.func import Rank
from sql.queries.base import Item, List
from sql.queries.select import JoinStrategy, Select

Engine(
    Config(
        dsn="postgresql://user:password@db:5432/sql_builder",
        apps=["concept"],
        debug=True,
    )
)


async def _load_data():
    print("--- Загрузка данных ---")

    with open("./dump.sql") as f:
        sql = f.read()

    async with Users.connection() as conn:
        await conn.execute(sql)
        await conn.execute("ANALYZE")


async def run_test():
    await _load_data()

    print("--- Тест 1. Получение пользователей ---")
    users = await Select(Users.id, Users.name)
    for row in users:
        print(f"User: {row['id']} - {row['name']}")

    print("\n--- Тест 2. Поиск по триграммам ---")
    query = Select(Users.id, Users.name).filter(Users.name % "аликсандр" < 0.7)
    print(f"SQL запроса:\n{query}")

    pro_users = await query
    print(f"Найдено похожих: {pro_users}")

    print("\n--- Тест 3. Магазины (Таймзоны и Ночные смены) ---")
    query = (
        Select(Shops.name, city=List(*Cities)).join(Cities).filter(Shops.is_open_now())
    )

    print(f"SQL запроса:\n{query.compile([])}")

    results = await query
    print(f"Сейчас открыто магазинов: {len(results)}")
    for row in results:
        print(f"Магазин '{row['name']}' в городе {row['city']}")

    print("\n--- Тест 4. Рекурсивное дерево (as_table) ---")

    sub = (
        Select(Categories.id, Categories.name)
        .filter(Categories.parent_id == 2)
        .as_model()
    )
    query = Select(Categories.name, sub_name=sub.name).join(
        sub, on=(Categories.parent_id == sub.id)
    )

    print(f"SQL:\n{query.compile([])}")
    res = await query
    for row in res:
        print(f"Найдена вложенность: {row['sub_name']} -> {row['name']}")

    print("\n--- Тест 5. Кто чей папа (Self-Join) ---")

    Parent = Categories["parent"]
    query = (
        Select(Categories.name, parent=Item(*Parent))
        .join(Parent, on=(Categories.parent_id == Parent.id))
        .order_by(Parent.name)
    )

    print(f"SQL:\n{query.compile([])}")
    res = await query
    for row in res:
        print(f"Категория: {row['name']} (Родитель: {row['parent']})")

    print("\n--- Тест 6. Свойства даты (Age, Year, Month) ---")
    query = Select(
        Users.name,
        age=Users.birth_date.age,
        birth_year=Users.birth_date.year,
        birth_month=Users.birth_date.month,
    ).order_by(Users.birth_date.desc())

    print(f"SQL:\n{query.compile([])}")
    res = await query
    for row in res:
        print(
            f"{row['name']}: {int(row['age'])} лет (Родился: {int(row['birth_month'])}/{int(row['birth_year'])})"
        )

    print("\n--- Тест 7. Оконные функции (Sales Analytics) ---")

    # 1. Ранг продажи внутри категории по сумме (от большей к меньшей)
    sale_rank = Rank().over(partition_by=Categories.id, order_by=Sales.amount.desc())

    # 2. Сумма всех продаж в ЭТОЙ категории (окно)
    category_total = Sum(Sales.amount).over(partition_by=Categories.id)

    # 3. Процент конкретной продажи от всей категории
    share = (Sales.amount / category_total * 100) >> DecimalField(5, 2)

    query = (
        Select(Sales.amount, category_name=Categories.name, rank=sale_rank, share=share)
        .join(Categories)
        .order_by(Categories.name, sale_rank)
    )

    print(f"SQL:\n{query.compile([])}")
    res = await query

    print(f"{'Категория':<15} | {'Сумма':<8} | {'Ранг':<5} | {'Доля %'}")
    print("-" * 50)
    for row in res:
        print(
            f"{row['category_name']:<15} | {row['amount']:<8} | {row['rank']:<5} | {row['share']}%"
        )

    print("\n--- Тест 8. Аналитика с SUMMARY (ROLLUP) ---")

    query = (
        Select(
            shop=Shops.name,
            category=Categories.name,
            total_sales=Sum(Sales.amount).cast(DecimalField(12, 2)),
        )
        .join(Shops)
        .join(Categories)
        .summary(Shops.name, Categories.name)
        .order_by(Shops.name.asc(), Categories.name.asc())
    )

    print(f"SQL:\n{query.compile([])}")

    res = await query

    print(f"{'Магазин':<20} | {'Категория':<15} | {'Сумма'}")
    print("-" * 50)

    for row in res:
        # Postgres возвращает NULL в полях группировки для строк-итогов
        shop = row["shop"] if row["shop"] else "ИТОГО (ВСЕ)"
        cat = row["category"] if row["category"] else "ВСЕ КАТЕГОРИИ"

        # Визуально выделяем строки итогов
        prefix = ">> " if row["shop"] is None or row["category"] is None else "   "
        print(f"{prefix}{shop:<17} | {cat:<15} | {row['total_sales']:>10}")

    print("\n--- Тест 9. Босс-уровень: RIGHT JOIN + Алиасы + Агрегация ---")

    Parent = Categories["parent"]

    query = (
        Select(parent_name=Parent.name, children_count=Count(Categories.id))
        .join(
            Parent, on=(Categories.parent_id == Parent.id), strategy=JoinStrategy.RIGHT
        )
        .group_by(Parent.name)
        .order_by(Count(Categories.id).desc())
    )

    print(f"SQL:\n{query.compile([])}")

    res = await query

    print(f"{'Родительская категория':<25} | {'Кол-во подкатегорий'}")
    print("-" * 50)
    for row in res:
        name = row["parent_name"] or "--- БЕЗРОДНЫЕ ДЕТИ ---"
        print(f"{name:<25} | {row['children_count']}")

    print("\n--- Тест 10. Хардкорный UPDATE с авто-FROM ---")

    query = (
        Update(Users)
        .set(name=concat("Vip {Users.name}"))
        .filter(
            (Users.id == Sales.id)
            & (Sales.shop_id == Shops.id)
            & (Shops.city_id == Cities.id)
            & (Cities.name == "Москва")
            & (Sales.amount > 4000)
        )
        .returning(Users.id, Users.name)
    )

    print(f"Генерируемый SQL:\n{query.compile([])}\n")

    results = await query

    if results:
        print(f"🔥 Обновлено VIP-пользователей: {len(results)}")
        for row in results:
            print(f" - ID: {row['id']}, New Name: {row['name']}")
    else:
        print("Никто не подошел под критерии VIP.")

    print("\n--- Тест 11. DELETE с автоматическим USING ---")

    # Условие затрагивает Cities, но удаляем из Sales.
    # Между ними стоит Shops, который не упомянут в filter напрямую.
    # Но так как Cities.name связан с Shops, а Shops с Sales через FK,
    # твой механизм relations должен вытащить всю цепочку.

    query = (
        Delete(Sales)
        .filter(
            (Sales.shop_id == Shops.id)
            & (Shops.city_id == Cities.id)
            & (Cities.name == "Владивосток")
        )
        .returning(Sales.id, Sales.amount)
    )

    print(f"SQL (The Destroyer):\n{query.compile([])}\n")

    # Выполняем. Advisor проверит, не забыли ли мы индексы для такого маневра.
    results = await query

    if results:
        print(f"🧨 Удалено записей: {len(results)}")
        for row in results:
            print(f" - ID: {row['id']}, Сумма: {row['amount']}")
    else:
        print("Ничего не найдено для удаления.")


if __name__ == "__main__":
    asyncio.run(run_test())
