from itertools import zip_longest
from string.templatelib import Template


def extract_template(value: Template):
    for s, interp in zip_longest(value.strings, value.interpolations):
        if s:
            yield s
        if interp:
            yield interp.value
