import re
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Callable, Dict, List, Set, Tuple, Union

from whylogs.core.configs import SummaryConfig
from whylogs.core.dataset_profile import DatasetProfile, DatasetProfileView
from whylogs.core.metrics.metrics import Metric


class ValueGetter(ABC):
    @abstractmethod
    def __call__(self) -> Union[str, int, float]:
        raise NotImplementedError


class LiteralGetter(ValueGetter):
    def __init__(self, value: Union[str, int, float]) -> None:
        self._literal = value

    def __call__(self) -> Union[str, int, float]:
        return self._literal


class MetricGetter(ValueGetter):
    def __init__(self, metric: Metric, path: str) -> None:
        self._metric = metric
        self._path = path
        summary = self._metric.to_summary_dict(SummaryConfig())
        if path not in summary:
            raise ValueError(f"{path} is not available in {metric.namespace}")

    def __call__(self) -> Union[str, int, float]:
        summary = self._metric.to_summary_dict(SummaryConfig())
        return summary[self._path]


class ProfileGetter(ValueGetter):
    def __init__(self, profile: DatasetProfile, column_name: str, path: str) -> None:
        self._profile = profile
        self._column_name = column_name
        self._path = path

    def __call__(self) -> Union[str, int, float]:
        col_prof = self._profile.view().get_column(self._column_name)
        if not col_prof:
            raise ValueError(f"Column {self._column_name} not found in profile")
        summary = col_prof.to_summary_dict()
        if self._path not in summary:
            raise ValueError(f"{self._path} is not available in {self._column_name} profile")
        return summary[self._path]


class Relation(Enum):
    match = 1
    fullmatch = 2
    equal = 3
    less = 4
    leq = 5
    greater = 6
    geq = 7
    neq = 8


RELATION_FN = Callable[[Any, Callable[[], Any]], bool]
VALUE_FN = Callable[[], Union[str, int, float]]
EXPRESSION = Tuple[RELATION_FN, VALUE_FN]

def _do_nothing() -> str:
    return ""


def relation(op: Relation, value: Union[str, int, float, ValueGetter]) -> EXPRESSION:
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


def and_relations(left: EXPRESSION, right: EXPRESSION) -> EXPRESSION:
    return lambda x, y: left[0](x, left[1]) and right[0](x, right[1]), _do_nothing


def or_relations(left: EXPRESSION, right: EXPRESSION) -> EXPRESSION:
    return lambda x, y: left[0](x, left[1]) or right[0](x, right[1]), _do_nothing


def not_relation(relation: EXPRESSION) -> EXPRESSION:
    return lambda x, y: not relation[0](x, relation[1]), _do_nothing

