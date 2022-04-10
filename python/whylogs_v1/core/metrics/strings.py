import logging
from typing import Any, Optional

import whylogs_datasketches as wd

from whylogs_v1.core.metrics import MergeableMetric, OperationResult, UpdatableMetric
from whylogs_v1.core.preprocessing import PreprocessColumn
from whylogs_v1.core.proto import FrequentItemsSketchMessage, TrackerMessage

logger = logging.getLogger(__name__)


class MergeableFrequentItemsMetric(MergeableMetric[Any]):
    def __init__(self, k: int = 12, sketch: Optional[wd.frequent_strings_sketch] = None):
        self._k = k
        if sketch is None:
            sketch = wd.frequent_strings_sketch(lg_max_k=k)
        self._sketch = sketch

    def merge(self, other: "MergeableFrequentItemsMetric") -> "MergeableFrequentItemsMetric":
        res = wd.frequent_strings_sketch(max([self._k, other._k]))
        res.merge(self._sketch)
        res.merge(other._sketch)

        return MergeableFrequentItemsMetric()

    def serialize(self) -> TrackerMessage:
        fi = FrequentItemsSketchMessage(lg_max_k=self._k, sketch=self._sketch.serialize())
        return TrackerMessage(frequent_items=fi)

    @classmethod
    def fromSerialized(cls, msg: TrackerMessage) -> "MergeableFrequentItemsMetric":
        if msg is None or msg.frequent_items is None:
            return MergeableFrequentItemsMetric()
        sk = wd.frequent_strings_sketch.deserialize(msg.frequent_items.sketch)
        return MergeableFrequentItemsMetric(k=msg.frequent_items.lg_max_k, sketch=sk)


class UpdatableFrequentItemsMetric(UpdatableMetric[MergeableFrequentItemsMetric]):
    def __init__(self, k: int = 12, sketch: Optional[wd.frequent_strings_sketch] = None):
        self._k = k
        if sketch is None:
            sketch = wd.frequent_strings_sketch(lg_max_k=k)
        self._sketch = sketch

    def columnar_update(self, col: PreprocessColumn) -> OperationResult:
        successes = 0
        for arr in [col.numpy.floats, col.numpy.ints]:
            if arr is not None:
                self._sketch.update_np(arr)
                successes += len(arr)
        if col.pandas.strings is not None:
            self._sketch.update_str_list(col.pandas.strings.to_list())
            successes += len(col.pandas.strings)

        if col.list.ints is not None:
            self._sketch.update_int_list(col.list.ints)
            successes += len(col.list.ints)

        if col.list.floats is not None:
            self._sketch.update_double_list(col.list.floats)
            successes += len(col.list.floats)

        if col.list.strings is not None:
            self._sketch.update_str_list(col.list.strings)
            successes += len(col.list.strings)

        failures = 0
        if col.list.objs is not None:
            successes += len(col.list.objs)
        if col.pandas.objs is not None:
            successes += len(col.pandas.objs)

        return OperationResult(successes=successes, failures=failures)

    def to_mergeable(self) -> "MergeableFrequentItemsMetric":
        return MergeableFrequentItemsMetric()

    def serialize(self) -> TrackerMessage:
        return TrackerMessage(
            frequent_items=FrequentItemsSketchMessage(lg_max_k=self._k, sketch=self._sketch.serialize())
        )

    @classmethod
    def fromSerialized(cls, msg: TrackerMessage) -> "UpdatableFrequentItemsMetric":
        if msg is None:
            return UpdatableFrequentItemsMetric()
        return UpdatableFrequentItemsMetric()
