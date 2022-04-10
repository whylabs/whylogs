import logging
import math
from abc import ABC
from typing import Any, Generic, Optional, Type, TypeVar

import whylogs_datasketches as ds  # type: ignore

from whylogs_v1.core.metrics.metrics import (
    MergeableMetric,
    OperationResult,
    UpdatableMetric,
)
from whylogs_v1.core.preprocessing import PreprocessColumn
from whylogs_v1.core.proto import KllSketchMessage, TrackerMessage
from whylogs_v1.core.stubs import np

logger = logging.getLogger(__name__)

NUM = TypeVar("NUM", float, int)
MERGE_INT = TypeVar("MERGE_INT", bound="MergeableNumberMetric")


class MergeableNumberMetric(MergeableMetric[NUM], Generic[NUM], ABC):
    def __init__(self, value: NUM = 0):
        if value is None:
            value = 0
        self._value: NUM = value

    def serialize(self: MERGE_INT) -> TrackerMessage:
        value = self._value
        if isinstance(value, int):
            return TrackerMessage(n=value)
        if isinstance(value, float):
            return TrackerMessage(d=value)
        raise AssertionError("This should not happen")

    @classmethod
    def fromSerialized(cls: Type[MERGE_INT], msg: TrackerMessage) -> MERGE_INT:
        value = getattr(msg, msg.WhichOneof("value"))  # type: ignore
        return cls(value)


UP_INT = TypeVar("UP_INT", bound="UpdatableNumberMetric")


class UpdatableNumberMetric(UpdatableMetric[MERGE_INT], Generic[NUM, MERGE_INT], ABC):
    def __init__(self, value: NUM = None) -> None:
        self._value: Optional[NUM] = value

    def serialize(self) -> TrackerMessage:
        value = self._value
        if isinstance(value, int):
            return TrackerMessage(n=value)
        if isinstance(value, float):
            return TrackerMessage(d=value)
        if isinstance(value, np.number):
            if isinstance(value, np.floating):
                return TrackerMessage(d=value)
            else:
                return TrackerMessage(n=value)
        raise AssertionError("This should not happen")

    @classmethod
    def fromSerialized(cls: Type[UP_INT], msg: TrackerMessage) -> UP_INT:  # type: ignore
        value = getattr(msg, msg.WhichOneof("value"))  # type: ignore
        return cls(value)


class MergeableCountMetric(MergeableNumberMetric[int]):
    def merge(self, other: "MergeableCountMetric") -> "MergeableCountMetric":
        if self._value is None:
            self._value = 0
        if other._value is None:
            other._value = 0
        total = self._value + other._value
        return MergeableCountMetric(value=total)


class UpdatableCountMetric(UpdatableNumberMetric[int, MergeableCountMetric]):
    def __init__(self, value: int = 0):
        if value is None:
            value = 0
        super().__init__(value)

    def columnar_update(self, data: PreprocessColumn) -> OperationResult:
        if data.len > 0:
            if self._value is None:
                self._value = data.len
            else:
                self._value += data.len
            return OperationResult.ok(data.len)
        else:
            # TODO: warn about incompatible data type here
            logger.warning("Incompatible data type detected. Got: %s", type(data.original))
            return OperationResult.failed()

    def to_mergeable(self) -> "MergeableCountMetric":
        if self._value is not None:
            return MergeableCountMetric(value=self._value)
        else:
            return MergeableCountMetric()


class UpdatableNullCountMetric(UpdatableNumberMetric[int, MergeableCountMetric]):
    def columnar_update(self, data: PreprocessColumn) -> OperationResult:
        if data.len > 0:
            if self._value is None:
                self._value = data.null_count
            else:
                self._value += data.null_count
            return OperationResult.ok(data.null_count)
        else:
            # TODO: warn about incompatible data type here
            logger.warning("Incompatible data type detected. Got: %s", type(data.original))
            return OperationResult.failed()

    def to_mergeable(self) -> MergeableCountMetric:
        if self._value is None:
            self._value = 0
        return MergeableCountMetric(self._value)


class MergeableMaxMetric(MergeableNumberMetric[int]):
    def merge(self, other: "MergeableMaxMetric") -> "MergeableMaxMetric":
        if self._value is None and other._value is None:
            return MergeableMaxMetric()
        if self._value is None:
            return MergeableMaxMetric(other._value)
        if other._value is None:
            return MergeableMaxMetric(self._value)

        mx = max(self._value, other._value)
        return MergeableMaxMetric(value=mx)


class UpdatableMaxIntMetric(UpdatableNumberMetric[int, MergeableMaxMetric]):
    def columnar_update(self, data: PreprocessColumn) -> OperationResult:
        if data.len > 0:
            successes = 0
            candidates = []
            if data.numpy.ints is not None:
                candidates.append(data.numpy.ints.max())
                successes += len(data.numpy.ints)

            if data.list.ints is not None:
                candidates.append(max(data.list.ints))
                successes += len(data.list.ints)
            if len(candidates) > 0:
                value = max(candidates)
                if self._value is not None and value > self._value:
                    self._value = value
                else:
                    self._value = value

            return OperationResult(successes=successes, failures=(data.len - successes))
        else:
            # TODO: warn about incompatible data type here
            logger.warning("Incompatible data type detected. Got: %s", type(data.original))
            return OperationResult.failed()

    def to_mergeable(self) -> MergeableMaxMetric:
        if self._value:
            return MergeableMaxMetric(self._value)
        else:
            return MergeableMaxMetric()


class MergeableMinIntMetric(MergeableNumberMetric[int]):
    def merge(self, other: "MergeableMinIntMetric") -> "MergeableMinIntMetric":
        if self._value is None and other._value is None:
            return MergeableMinIntMetric()
        if self._value is None:
            return MergeableMinIntMetric(other._value)
        if other._value is None:
            return MergeableMinIntMetric(self._value)

        mx = max(self._value, other._value)
        return MergeableMinIntMetric(value=mx)


class UpdatableMinIntMetric(UpdatableNumberMetric[int, MergeableMaxMetric]):
    def columnar_update(self, data: PreprocessColumn) -> OperationResult:
        if data.len > 0:
            successes = 0
            candidates = []
            if data.numpy.ints is not None:
                candidates.append(data.numpy.ints.min())
                successes += len(data.numpy.ints)

            if data.list.ints is not None:
                candidates.append(min(data.list.ints))
                successes += len(data.list.ints)
            if len(candidates) > 0:
                value = min(candidates)
                if self._value is not None and value > self._value:
                    self._value = value
                else:
                    self._value = value

            return OperationResult(successes=successes, failures=(data.len - successes))
        else:
            # TODO: warn about incompatible data type here
            logger.warning("Incompatible data type detected. Got: %s", type(data.original))
            return OperationResult.failed()

    def to_mergeable(self) -> MergeableMaxMetric:
        if self._value:
            return MergeableMaxMetric(self._value)
        else:
            return MergeableMaxMetric()


_DEFAULT_HIST_K = 256


class MergeableDistribution(MergeableMetric[Any]):
    def __init__(
        self,
        k: int = 256,
        mean: float = 0,
        m2: float = 0,
        sketch: Optional[ds.kll_doubles_sketch] = None,
    ):
        if sketch is None:
            sketch = ds.kll_doubles_sketch(k)

        self._histogram = sketch
        self._mean = mean
        self._m2 = m2

    @property
    def n(self) -> int:
        return self._histogram.get_n()

    @property
    def variance(self) -> float:
        n = self.n
        if n < 2:
            raise ValueError("Variance not available")

        return (self._m2 - self._mean**2) * (n / (n - 1))

    @property
    def stddev(self) -> float:
        return math.sqrt(self.variance)

    def merge(self, other: "MergeableDistribution") -> "MergeableDistribution":  # type: ignore
        hist_copy = ds.kll_doubles_sketch(self._histogram)
        hist_copy.merge(other._histogram)

        mean = 0.0
        m2 = 0.0
        x_len = self._histogram.get_n()
        y_len = other._histogram.get_n()
        if x_len == 0:
            mean = other._mean
            m2 = other._m2
        elif y_len == 0:
            mean = self._mean
            m2 = self._m2
        elif x_len > 0 and y_len > 0:
            new_n = x_len + y_len
            mean = (x_len / new_n) * (self._mean / new_n) + (y_len / new_n) * (other._mean / new_n)
            m2 = (x_len / new_n) * (self._m2 / new_n) + (y_len / new_n) * (other._m2 / new_n)

        return MergeableDistribution(k=hist_copy.get_k(), sketch=hist_copy, mean=mean, m2=m2)

    def serialize(self) -> TrackerMessage:
        kll = KllSketchMessage(
            sketch=self._histogram.serialize(),
            m2=self._m2,
            mean=self._mean,
        )
        return TrackerMessage(histogram=kll)

    @classmethod
    def fromBytes(cls, msg: bytes, mean: float, m2: float) -> "MergeableDistribution":
        sk = ds.kll_doubles_sketch.deserialize(msg)

        return MergeableDistribution(sketch=sk, mean=mean, m2=m2)

    @classmethod
    def fromSerialized(cls, msg: TrackerMessage) -> "MergeableDistribution":
        if msg is None or msg.histogram is None:
            return MergeableDistribution()
        kll = msg.histogram
        return MergeableDistribution.fromBytes(
            msg=kll.sketch,
            mean=kll.mean,
            m2=kll.m2,
        )


class UpdatableDistribution(UpdatableMetric[MergeableDistribution]):
    def __init__(
        self,
        k: int = _DEFAULT_HIST_K,
        mean: float = 0,
        m2: float = 0,
        sketch: Optional[ds.kll_doubles_sketch] = None,
    ) -> None:
        if sketch is not None:
            self._histogram = sketch
        else:
            self._histogram = ds.kll_doubles_sketch(k)
        self._mean = mean
        self._m2 = m2

    def columnar_update(self, view: PreprocessColumn) -> OperationResult:
        # best scenario is numpy is available
        successes = 0

        y_sum: float = 0.0
        y_len: int = 0
        x_len = self._histogram.get_n()
        y_squared_sum: float = 0.0
        if view.numpy.len > 0:
            for arr in [view.numpy.floats, view.numpy.ints]:
                if arr is not None:
                    self._histogram.update(arr)
                    y_squared_sum += (arr**2).sum()
                    y_len += len(arr)

        # these should be empty if the numpy above are done
        for lst in [view.list.ints, view.list.floats]:
            if lst is not None:
                self._histogram.update_list(lst)
                successes += len(lst)
                y_len += len(lst)
                for value in lst:
                    y_sum += value
                    y_squared_sum += value**2

        if x_len > 0:
            sum_len = x_len + y_len
            self._mean = (x_len / sum_len) * (self._mean / sum_len) + y_sum / sum_len
            self._m2 = (x_len / sum_len) * (self._m2 / sum_len) + y_squared_sum / sum_len
        elif y_len > 0:
            self._mean = y_sum / y_len
            self._m2 = y_squared_sum / y_len

        failures = 0
        for failed_lists in [view.list.objs, view.list.strings]:
            if failed_lists is not None:
                failures += len(failed_lists)

        return OperationResult(failures=failures, successes=successes)

    def to_mergeable(self) -> "MergeableDistribution":
        return MergeableDistribution.fromBytes(
            msg=self._histogram.serialize(),
            mean=self._mean,
            m2=self._m2,
        )

    def serialize(self) -> TrackerMessage:
        kll = KllSketchMessage(
            sketch=self._histogram.serialize(),
            mean=self._mean,
            m2=self._m2,
        )
        return TrackerMessage(histogram=kll)

    @classmethod
    def fromSerialized(cls, msg: TrackerMessage) -> "UpdatableDistribution":
        kll = msg.histogram
        if kll is None:
            return UpdatableDistribution()
        sk = ds.kll_doubles_sketch.deserialize(kll.sketch)
        return UpdatableDistribution(
            sketch=sk,
            mean=kll.mean,
            m2=kll.m2,
        )

    @property
    def is_parallelizable(self) -> bool:
        return True
