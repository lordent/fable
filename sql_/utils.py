from typing import Any

from .core import Cast, Expr
from .field import Field


def describe(expr: Any) -> dict[str, Any]:
    """
    Универсальный инспектор для любого узла конструктора.
    Безопасно извлекает метаданные даже из сложных выражений.
    """

    # Базовая информация, которая есть у всех
    info = {
        "type": type(expr),
        "is_aggregate": getattr(expr, "is_aggregate", False),
        "sql_type": "UNKNOWN",
        "name": getattr(expr, "name", None),
    }

    # 1. Если это Field — забираем максимум метаданных
    if isinstance(expr, Field):
        info.update(
            {
                "sql_type": expr.sql_type.value
                if hasattr(expr.sql_type, "value")
                else str(expr.sql_type),
                "pk": expr.pk,
                "nullable": expr.nullable,
                "unique": expr.unique,
                "index": expr.index,
                "default": str(expr.default)
                if hasattr(expr.default, "compile")
                else expr.default,
            }
        )
        # Специфика для FK
        if hasattr(expr, "to"):
            info["related_to"] = expr.to._table

    # 2. Если это Cast — мы знаем целевой тип и можем вытащить field_instance
    elif isinstance(expr, Cast):
        info["sql_type"] = expr.to
        if expr.field_instance:
            # Рекурсивно дополняем данными из вложенного поля, если оно там есть
            inner_info = describe(expr.field_instance)
            inner_info.update(info)  # Cast приоритетнее по типу
            return inner_info

    # 3. Если это просто Expr (выражение типа User.id + 1)
    elif isinstance(expr, Expr):
        # Попробуем угадать имя, если оно было присвоено в Select kwargs
        info["sql_type"] = "EXPRESSION"

    # 4. Если это сырое значение (int, str)
    else:
        info["sql_type"] = type(expr).__name__.upper()
        info["value"] = expr

    return info
