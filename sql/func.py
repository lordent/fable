from typing import Any

from .core import Func


def Rank():
    return Func("RANK")


def DenseRank():
    return Func("DENSE_RANK")


def RowNumber():
    return Func("ROW_NUMBER")


def Now():
    return Func("NOW")


def Extract(arg: Any):
    return Func("EXTRACT", arg)
