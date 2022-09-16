import re
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Callable, Dict, List, Set, Tuple, Union


class ValueGetter(ABC):
    @abstractmethod
    def __call__(self) -> Union[str, int, float]:
        raise NotImplementedError


class LiteralGetter(ValueGetter):
    def __init__(self, value: Union[str, int, float]) -> None:
        self._literal = value

    def __call__(self) -> Union[str, int, float]:
        return self._literal


class Relation(Enum):
    match = 1
    fullmatch = 2
    equal = 3
    less = 4
    leq = 5
    greater = 6
    geq = 7
    neq = 8


Relation_Fn = Callable[[Any, Callable[[], Any]], bool]
Value_Fn = Callable[[], Union[str, int, float]]
Expression = Tuple[Relation_Fn, Value_Fn]

def _do_nothing() -> str:
    return ""


def relation(op: Relation, value: Union[str, int, float, ValueGetter]) -> Expression:
    if isinstance(value, (str, int, float)):
        arg = LiteralGetter(value)
    else:
        assert isinstance(value, ValueGetter)
        arg = value

    if op == Relation.match:
        if not isinstance(value, str):
            raise ValueError("match requires a string regular expression argument")
        return lambda x, y: re.compile(value).match(x), _do_nothing  # type: ignore
    if op == Relation.fullmatch:
        if not isinstance(value, str):
            raise ValueError("match requires a string regular expression argument")
        return lambda x, y: re.compile(value).fullmatch(x), _do_nothing  # type: ignore

    if op == Relation.equal:
        return lambda x, y: x == y(), arg  # type: ignore
    if op == Relation.less:
        return lambda x, y: x < y(), arg  # type: ignore
    if op == Relation.leq:
        return lambda x, y: x <= y(), arg  # type: ignore
    if op == Relation.greater:
        return lambda x, y: x > y(), arg  # type: ignore
    if op == Relation.geq:
        return lambda x, y: x >= y(), arg  # type: ignore
    if op == Relation.neq:
        return lambda x, y: x != y(), arg  # type: ignore

    raise ValueError("Unknown ConditionCountMetric predicate")


def and_relations(left: Expression, right: Expression) -> Expression:
    return lambda x, y: left[0](x, left[1]) and right[0](x, right[1]), _do_nothing


def or_relations(left: Expression, right: Expression) -> Expression:
    return lambda x, y: left[0](x, left[1]) or right[0](x, right[1]), _do_nothing


def not_relation(relation: Expression) -> Expression:
    return lambda x, y: not relation[0](x, relation[1]), _do_nothing


def udf_relation(relation: Relation_Fn, value: Value_Fn = _do_nothing) -> Expression:
    return (relation, value)
