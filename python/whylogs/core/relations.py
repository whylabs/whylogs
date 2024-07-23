import re
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Callable, Optional, Union

from whylogs.core.configs import SummaryConfig
from whylogs.core.metrics.metrics import Metric


class ValueGetter(ABC):
    @abstractmethod
    def __call__(self) -> Union[str, int, float]:
        raise NotImplementedError

    @abstractmethod
    def serialize(self) -> str:
        raise NotImplementedError


def unescape_quote(input: str) -> str:
    return '"' + input[1:-1].replace(r"\"", '"').replace("\\\\", "\\") + '"' if input[0] == input[-1] == '"' else input


def unescape_colon(input: str) -> str:
    return input.replace(r"\:", ":") if input[0] == ":" else input


def escape(input: str, target: str) -> str:
    return input.replace(target, f"\\{target}")


class LiteralGetter(ValueGetter):
    def __init__(self, value: Union[str, int, float]) -> None:
        self._literal = value

    def __call__(self) -> Union[str, int, float]:
        return self._literal

    def serialize(self) -> str:
        if isinstance(self._literal, str):
            escaped = escape(self._literal, '"')
            return f'"{escaped}"'
        return str(self._literal)


class Relation(Enum):
    no_op = 0
    match = 1
    fullmatch = 2
    equal = 3
    less = 4
    leq = 5
    greater = 6
    geq = 7
    neq = 8
    _and = 9
    _or = 10
    _not = 11
    _udf = 12
    search = 13


_TOKEN = ["", "~", "~=", "==", "<", "<=", ">", ">=", "!=", "and", "or", "not", "udf", "%"]


class Predicate:
    def __init__(
        self,
        op: Relation = Relation.no_op,
        value: Union[str, int, float, ValueGetter] = 0,
        udf: Optional[Callable[[Any], bool]] = None,
        left: Optional["Predicate"] = None,
        right: Optional["Predicate"] = None,
        component: Optional[str] = None,
    ):
        self._component = component
        self._op = op
        self._udf = udf
        if op in {Relation.match, Relation.fullmatch, Relation.search}:
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

    def __call__(self, x: Any) -> bool:
        if isinstance(x, Metric):
            metric = x
            if self._op != Relation._udf:
                if not self._component:
                    raise ValueError("Metric constraints require a specified component")

                summary = metric.to_summary_dict(SummaryConfig())
                if self._component not in summary:
                    raise ValueError(f"{self._component} is not available in {metric.namespace} metric")
                x = summary[self._component]
        else:
            metric = None

        op = self._op
        if op == Relation.match:
            return isinstance(x, str) and bool(self._re.match(x))
        if op == Relation.fullmatch:
            return isinstance(x, str) and bool(self._re.fullmatch(x))
        if op == Relation.search:
            return isinstance(x, str) and bool(self._re.search(x))

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
        if op == Relation._udf:
            return self._udf(metric or x)  # type: ignore
        if op == Relation._and:
            return self._left(metric or x) and self._right(metric or x)  # type: ignore
        if op == Relation._or:
            return self._left(metric or x) or self._right(metric or x)  # type: ignore
        if op == Relation._not:
            if not self._right:
                raise ValueError("negation operator requires a predicate to negate")
            return not self._right(metric or x)  # type: ignore

        raise ValueError("Unknown predicate")

    def _maybe_not(self, op: Relation, value: Union[str, int, float, ValueGetter]) -> "Predicate":
        pred = Predicate(op, value, component=self._component)
        if self._op == Relation._not and self._right is None:
            return Predicate(Relation._not, right=pred, component=self._component)
        return pred

    def matches(self, value: Union[str, int, float, ValueGetter]) -> "Predicate":
        return self._maybe_not(Relation.match, value)

    def fullmatch(self, value: Union[str, int, float, ValueGetter]) -> "Predicate":
        return self._maybe_not(Relation.fullmatch, value)

    def search(self, value: Union[str, int, float, ValueGetter]) -> "Predicate":
        return self._maybe_not(Relation.search, value)

    def equals(self, value: Union[str, int, float, ValueGetter]) -> "Predicate":
        return self._maybe_not(Relation.equal, value)

    def less_than(self, value: Union[str, int, float, ValueGetter]) -> "Predicate":
        return self._maybe_not(Relation.less, value)

    def less_or_equals(self, value: Union[str, int, float, ValueGetter]) -> "Predicate":
        return self._maybe_not(Relation.leq, value)

    def greater_than(self, value: Union[str, int, float, ValueGetter]) -> "Predicate":
        return self._maybe_not(Relation.greater, value)

    def greater_or_equals(self, value: Union[str, int, float, ValueGetter]) -> "Predicate":
        return self._maybe_not(Relation.geq, value)

    def not_equal(self, value: Union[str, int, float, ValueGetter]) -> "Predicate":
        return self._maybe_not(Relation.neq, value)

    def and_(self, right: "Predicate") -> "Predicate":
        return Predicate(Relation._and, left=self, right=right, component=self._component)

    def or_(self, right: "Predicate") -> "Predicate":
        return Predicate(Relation._or, left=self, right=right, component=self._component)

    def is_(self, udf: Callable[[Any], bool]) -> "Predicate":
        pred = Predicate(Relation._udf, udf=udf)
        if self._op == Relation._not and self._right is None:
            return Predicate(Relation._not, right=pred)
        return pred

    @property
    def not_(self) -> "Predicate":
        return Predicate(Relation._not, component=self._component)

    def serialize(self) -> str:
        if not (self._left or self._right):
            return f"{_TOKEN[self._op.value]} {self._component or 'x'} {self._value.serialize()}"
        if self._op == Relation._not:
            return f"not {self._right.serialize()}"  # type: ignore
        return f"{_TOKEN[self._op.value]} {self._left.serialize()} {self._right.serialize()}"  # type: ignore

    def __str__(self) -> str:
        if not (self._left or self._right):
            return f"{self._component or 'x'} {_TOKEN[self._op.value]} {self._value.serialize()}"
        if self._op == Relation._not:
            return f"not {str(self._right)}"
        return f"({str(self._left)}) {_TOKEN[self._op.value]} ({str(self._right)})"


def Not(p: Predicate) -> Predicate:
    return Predicate(Relation._not, right=p, component=p._component)


def Require(component: Optional[str] = None) -> Predicate:
    return Predicate(component=component)
