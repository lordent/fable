from collections.abc import Callable
from itertools import zip_longest
from string.templatelib import Template
from typing import Any


def extract_template(value: Template, validator: Callable[[Any], Any] = None):
    for s, interp in zip_longest(value.strings, value.interpolations):
        if s:
            yield s
        if interp:
            yield validator(interp.value) if validator else interp.value


def quote_ident(name: str):
    return f'"{name.replace('"', '""')}"'


def quote_literal(value: Any):
    return f"'{str(value).replace("'", "''")}'"
