from typing import Optional

import whylogs_datasketches as ds  # type: ignore

from whylogs_v1.core.metrics import MergeableMetric, OperationResult, UpdatableMetric
from whylogs_v1.core.preprocessing import PreprocessColumn
from whylogs_v1.core.proto import HllSketchMessage, TrackerMessage


class MergeableUniquenessMetric(MergeableMetric):
    def __init__(self, k: int = 12, sketch: Optional[ds.hll_union] = None):
        if sketch is None:
            sketch = ds.hll_union(lg_max_k=k)
        self._sketch = sketch

    @property
    def estimate(self) -> int:
        return self._sketch.get_estimate()

    def upper(self, num_std_devs: int = 1) -> int:
        return self._sketch.get_upper_bound(num_std_devs)

    def lower(self, num_std_devs: int = 1) -> int:
        return self._sketch.get_lower_bound(num_std_devs)

    def merge(self, other: "MergeableUniquenessMetric") -> "MergeableUniquenessMetric":
        k = max([self._sketch.lg_config_k, other._sketch.lg_config_k])
        res = ds.hll_union(k)
        res.update(self._sketch.get_result())
        res.update(other._sketch.get_result())
        return MergeableUniquenessMetric(sketch=res)

    def serialize(self) -> TrackerMessage:
        return TrackerMessage()

    @classmethod
    def fromSerialized(cls, msg: TrackerMessage) -> "MergeableUniquenessMetric":
        return MergeableUniquenessMetric()


class UpdatableUniquenessMetric(UpdatableMetric[MergeableUniquenessMetric]):
    def __init__(self, k: int = 12, sketch: Optional[ds.hll_sketch] = None) -> None:
        if sketch is None:
            sketch = ds.hll_sketch(lg_k=k)
        self._sketch = sketch

    def columnar_update(self, view: PreprocessColumn) -> OperationResult:
        successes = 0
        if view.numpy.len > 0:
            if view.numpy.ints is not None:
                self._sketch.update_np(view.numpy.ints)
                successes += len(view.numpy.ints)
            if view.numpy.floats is not None:
                self._sketch.update_np(view.numpy.floats)
                successes += len(view.numpy.floats)
        if view.pandas.strings is not None:
            self._sketch.update_str_list(view.pandas.strings.to_list())
            successes += len(view.pandas.strings)

        # update everything in the remaining lists
        if view.list.ints:
            self._sketch.update_int_list(view.list.ints)
            successes += len(view.list.ints)
        if view.list.floats:
            self._sketch.update_double_list(view.list.floats)
            successes += len(view.list.floats)
        if view.list.strings:
            self._sketch.update_str_list(view.list.strings)
            successes += len(view.list.strings)

        failures = 0
        if view.list.objs:
            failures = len(view.list.objs)
        return OperationResult(successes=successes, failures=failures)

    def to_mergeable(self) -> "MergeableUniquenessMetric":
        union = ds.hll_union(self._sketch.lg_config_k)
        union.update(self._sketch)
        return MergeableUniquenessMetric(sketch=union)

    def serialize(self) -> TrackerMessage:
        hll = HllSketchMessage(lg_k=self._sketch.lg_config_k, sketch=self._sketch.serialize_updatable())
        return TrackerMessage(cardinality_tracker=hll)

    @classmethod
    def fromSerialized(cls, msg: TrackerMessage) -> "UpdatableUniquenessMetric":
        if msg is None or msg.cardinality_tracker is None:
            return UpdatableUniquenessMetric()
        hll = msg.cardinality_tracker
        sketch = ds.hll_sketch.deserialize(hll.sketch)
        return UpdatableUniquenessMetric(sketch=sketch)
