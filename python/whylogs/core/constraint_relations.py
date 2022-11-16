import re
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Callable, Optional, Union

from whylogs.core.configs import SummaryConfig
from whylogs.core.metrics.metrics import Metric
from whylogs.core.relations import LiteralGetter, Relation, _TOKEN, ValueGetter


class CnPredicate:
    def __init__(
        self,
        path: str,
        op: Relation = Relation.no_op,
        value: Union[str, int, float, ValueGetter] = 0,
        udf: Optional[Callable[[Any], bool]] = None,
        left: Optional["CnPredicate"] = None,
        right: Optional["CnPredicate"] = None,
    ):
        self._path = path
        self._op = op
        self._udf = udf
        if op in {Relation.match, Relation.fullmatch}:
            if not isinstance(value, str):
                raise ValueError("match requires a string regular expression argument")
            self._re = re.compile(value)

        if isinstance(value, (str, int, float)):
            self._value = LiteralGetter(value)
        else:
            self._value = value  # type: ignore

        if op in {Relation._and, Relation._or}:
            if not left and right:
                raise ValueError("boolean operators require two operands")
        self._left = left
        self._right = right

        if op == Relation._not:
            if not right:
                raise ValueError("negation operator requires a predicate to negate")
            self._right = right

    def __call__(self, m: Metric) -> bool:
        op = self._op
        if op == Relation._and:
            return self._left(m) and self._right(m)  # type: ignore
        if op == Relation._or:
            return self._left(m) or self._right(m)  # type: ignore
        if op == Relation._not:
            return not self._right(m)  # type: ignore
        if op == Relation._udf:
            return self._udf(m)  # type: ignore

        summary = m.to_summary_dict(SummaryConfig())
        if self._path not in summary:
            raise ValueError(f"{path} is not available in {m.namespace}")
        x = summary[self._path]
        if op == Relation.match:
            return isinstance(x, str) and bool(self._re.match(x))
        if op == Relation.fullmatch:
            return isinstance(x, str) and bool(self._re.fullmatch(x))

        value = self._value()
        if op == Relation.equal:
            return x == value
        if op == Relation.less:
            return x < value
        if op == Relation.leq:
            return x <= value
        if op == Relation.greater:
            return x > value
        if op == Relation.geq:
            return x >= value
        if op == Relation.neq:
            return x != value

        raise ValueError("Unknown predicate")

    def matches(self, value: Union[str, int, float, ValueGetter]) -> "CnPredicate":
        return CnPredicate(self._path, Relation.match, value)

    def fullmatch(self, value: Union[str, int, float, ValueGetter]) -> "CnPredicate":
        return CnPredicate(self._path, Relation.fullmatch, value)

    def equals(self, value: Union[str, int, float, ValueGetter]) -> "CnPredicate":
        return CnPredicate(self._path, Relation.equal, value)

    def less_than(self, value: Union[str, int, float, ValueGetter]) -> "CnPredicate":
        return CnPredicate(self._path, Relation.less, value)

    def less_or_equals(self, value: Union[str, int, float, ValueGetter]) -> "CnPredicate":
        return CnPredicate(self._path, Relation.leq, value)

    def greater_than(self, value: Union[str, int, float, ValueGetter]) -> "CnPredicate":
        return CnPredicate(self._path, Relation.greater, value)

    def greater_or_equals(self, value: Union[str, int, float, ValueGetter]) -> "CnPredicate":
        return CnPredicate(self._path, Relation.geq, value)

    def not_equal(self, value: Union[str, int, float, ValueGetter]) -> "CnPredicate":
        return CnPredicate(self._path, Relation.neq, value)

    def and_(self, right: "CnPredicate") -> "CnPredicate":
        return CnPredicate(self._path, Relation._and, left=self, right=right)

    def or_(self, right: "CnPredicate") -> "CnPredicate":
        return CnPredicate(self._path, Relation._or, left=self, right=right)

    def is_(self, udf: Callable[[Metric], bool]) -> "CnPredicate":
        return CnPredicate(self._path, Relation._udf, udf=udf)

    def serialize(self) -> str:
        if not (self._left or self._right):
            return f"{_TOKEN[self._op.value]} {self._path} {self._value.serialize()}"
        if self._op == Relation._not:
            return f"not {self._right.serialize()}"  # type: ignore
        return f"{_TOKEN[self._op.value]} {self._left.serialize()} {self._right.serialize()}"  # type: ignore

    def __str__(self) -> str:
        if not (self._left or self._right):
            return f"{self._path} {_TOKEN[self._op.value]} {self._value.serialize()}"
        if self._op == Relation._not:
            return f"not {str(self._right)}"
        return f"({str(self._left)}) {_TOKEN[self._op.value]} ({str(self._right)})"


def CnNot(p: CnPredicate) -> CnPredicate:
    return CnPredicate(p._path, Relation._not, right=p)


def Require(path: Optional[str] = None) -> CnPredicate:
    return CnPredicate(path=path or "")
