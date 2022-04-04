from abc import ABC, abstractmethod
from typing import Generic, Type, TypeVar

from whylogs_v1.core.extraction import ExtractedColumn

INPUT = TypeVar("INPUT")
SERIALIZED = TypeVar("SERIALIZED")
SUMMARY = TypeVar("SUMMARY")

SM = TypeVar("SM", bound="SerializableMetric")
UM = TypeVar("UM", bound="UpdatableMetric")
MM = TypeVar("MM", bound="MergeableMetric")
CM = TypeVar("CM", bound="CustomMetricMixin")


class SerializableMetric(ABC, Generic[SERIALIZED]):
    @abstractmethod
    def serialize(self: SM) -> SERIALIZED:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def fromSerialized(cls: Type[SM], msg: SERIALIZED) -> SM:
        raise NotImplementedError


class UpdatableMetric(SerializableMetric[SERIALIZED], Generic[INPUT, SERIALIZED, MM]):
    def columnar_update(self: UM, data: ExtractedColumn) -> None:
        for datum in data.raw_iterator():
            self.update(datum)

    @abstractmethod
    def update(self: UM, datum: INPUT) -> None:
        pass

    @abstractmethod
    def to_mergeable(self: UM) -> MM:
        raise NotImplementedError


class MergeableMetric(SerializableMetric[SERIALIZED], Generic[SERIALIZED, SUMMARY]):
    def __add__(self: MM, other: MM) -> MM:
        return self.merge(other)

    @abstractmethod
    def merge(self: MM, other: MM) -> MM:
        raise NotImplementedError


class CustomMetricMixin(  # TODO: this class is getting error
    UpdatableMetric[INPUT, SERIALIZED, "CustomMetricMixin"],
    MergeableMetric[SERIALIZED, SUMMARY],
    Generic[INPUT, SERIALIZED, SUMMARY],
):
    pass
