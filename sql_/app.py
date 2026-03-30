from __future__ import annotations

from typing import TypeVar

T = TypeVar("T", bound="Application")


class AppMeta(type):
    def __new__(mcs, name, bases, attrs):
        cls = super().__new__(mcs, name, bases, attrs)
        if not attrs.get("_abstract"):
            instance = cls()
            _app_registry[cls.__module__] = instance
        return cls


class Application(metaclass=AppMeta):
    _abstract = True
    name: str = ""

    def on_ready(self):
        pass


_app_registry: dict[str, Application] = {}


def get_app_for_module(module_name: str) -> Application:
    parts = module_name.split(".")
    for i in range(len(parts), 0, -1):
        path = ".".join(parts[:i])
        if path in _app_registry:
            return _app_registry[path]

    raise RuntimeError(
        f"Application не найден для {module_name}. "
        f"Опишите class MyApp(Application) в {module_name.split('.')[0]}/__init__.py"
    )
