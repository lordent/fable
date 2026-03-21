"""
sql/
├── __init__.py
├── app.py             # Application, настройки контекста
├── db.py              # Engine, TransactionContext, ContextVar (сессии)
├── core/              # Ядро (выражения и типы)
│   ├── __init__.py
│   ├── base.py        # Базовый класс S, логика _arg, _value, registry
│   ├── types.py       # DSL для типов (SqlType, Types)
│   ├── expressions.py # E, Q, Func, Cast, Concat
│   └── order.py       # OrderBy, OrderDirections
├── fields/            # Поля моделей
│   ├── __init__.py
│   ├── base.py        # Base Field, FieldFactory, FieldMeta
│   ├── logic.py       # BoolField, ForeignField, ComputedField
│   └── temporal.py    # DateField, TimestampField + логика AGE/EXTRACT
├── model/             # Модели и метаклассы
│   ├── __init__.py
│   ├── base.py        # ModelMeta, Model
│   └── query.py       # QueryModel (для подзапросов)
└── queries/           # Конструкторы запросов (Builder)
    ├── __init__.py
    ├── base.py        # QueryBuilder (__await__, execute, Advisor)
    ├── select.py      # SelectValuesQuery, Select, Item, List
    ├── update.py      # Update
    ├── insert.py      # Insert
    └── delete.py      # Delete

"""
