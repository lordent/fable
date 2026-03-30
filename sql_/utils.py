from itertools import zip_longest
from string.templatelib import Template
from typing import Any


def extract_template(value: Template):
    for s, interp in zip_longest(value.strings, value.interpolations):
        if s:
            yield s
        if interp:
            yield interp.value


def quote_ident(name: str):
    return f'"{name.replace('"', '""')}"'


def quote_literal(value: Any):
    if value is None:
        return "NULL"

    return f"'{str(value).replace("'", "''")}'"
