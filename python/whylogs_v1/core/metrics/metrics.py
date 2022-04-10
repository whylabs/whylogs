from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, Type, TypeVar

from whylogs_v1.core.preprocessing import PreprocessColumn
from whylogs_v1.core.proto import TrackerMessage

INPUT = TypeVar("INPUT")
SUMMARY = TypeVar("SUMMARY")

SM = TypeVar("SM", bound="SerializableMetric")
UM = TypeVar("UM", bound="UpdatableMetric")
MM = TypeVar("MM", bound="MergeableMetric")
CM = TypeVar("CM", bound="CustomMetricMixin")


@dataclass(frozen=True)
class OperationResult:
    failures: int = 0
    successes: int = 0

    @classmethod
    def ok(cls, cnt: int = 1) -> "OperationResult":
        return OperationResult(successes=cnt, failures=0)

    @classmethod
    def failed(cls, cnt: int = 1) -> "OperationResult":
        return OperationResult(successes=0, failures=cnt)

    def __add__(self, other: "OperationResult"):
        return OperationResult(
            successes=self.successes + other.successes,
            failures=self.failures + other.failures,
        )


class SerializableMetric(ABC):
    @abstractmethod
    def serialize(self: SM) -> TrackerMessage:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def fromSerialized(cls: Type[SM], msg: TrackerMessage) -> SM:
        raise NotImplementedError


class UpdatableMetric(Generic[MM], SerializableMetric):
    @abstractmethod
    def columnar_update(self: UM, data: PreprocessColumn) -> OperationResult:
        raise NotImplementedError

    @abstractmethod
    def to_mergeable(self: UM) -> MM:
        raise NotImplementedError


class MergeableMetric(Generic[SUMMARY], SerializableMetric):
    def __add__(self: MM, other: MM) -> MM:
        return self.merge(other)

    @abstractmethod
    def merge(self: MM, other: MM) -> MM:
        raise NotImplementedError


class CustomMetricMixin(
    UpdatableMetric["CustomMetricMixin"],
    MergeableMetric[SUMMARY],
    Generic[SUMMARY],
    ABC,
):
    """Custom metrics that can be both mergeable and updatable.

    It is recommended that you extend each of the class separately to prevent accidentally messing up
    with the internal of a metrics. However, if users want to do their own thing this is an option.

    Example:
    >>> from whylogs_v1.core import CustomMetricMixin
    >>> from typing import Any

    >>> class MyMetric(CustomMetricMixin[Any]):
    >>>     def columnar_update(self, data: PreprocessColumn) -> OperationResult:
    >>>        pass
    >>>
    >>>    def merge(self: "MyMetric", other: "MyMetric") -> "MyMetric":
    >>>        pass
    >>>
    >>>    def serialize(self) -> TrackerMessage:
    >>>        pass
    >>>
    >>>    @classmethod
    >>>    def fromSerialized(cls, msg: TrackerMessage) -> "MyMetric":
    >>>        pass
    """

    def to_mergeable(self: CM) -> CM:
        """By default, this class returns itself for a mergeable view."""
        return self
