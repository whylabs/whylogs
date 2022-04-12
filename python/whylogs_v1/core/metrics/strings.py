import logging
from dataclasses import dataclass
from typing import List, Optional

import whylogs_datasketches as wd  # type: ignore

from whylogs_v1.core.metrics import MergeableMetric, OperationResult, UpdatableMetric
from whylogs_v1.core.preprocessing import PreprocessColumn
from whylogs_v1.core.proto import FrequentItemsSketchMessage, TrackerMessage

logger = logging.getLogger(__name__)

_NO_FALSE_NEGATIVES = wd.frequent_items_error_type.NO_FALSE_NEGATIVES


@dataclass
class FrequentItemResult:
    value: str
    upper: int
    est: int
    lower: int


class MergeableFrequentItemsMetric(MergeableMetric):
    def __init__(self, k: int = 12, sketch: Optional[wd.frequent_strings_sketch] = None):
        self._k = k
        if sketch is None:
            sketch = wd.frequent_strings_sketch(lg_max_k=k)
        self._sketch = sketch

    def get_top_k(
        self, err_type: wd.frequent_items_error_type = _NO_FALSE_NEGATIVES, threshold: float = 0
    ) -> List[FrequentItemResult]:
        res = []
        for item in self._sketch.get_frequent_items(err_type, threshold):
            res.append(FrequentItemResult(value=item[0], upper=item[1], est=item[2], lower=item[3]))
        return res

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
