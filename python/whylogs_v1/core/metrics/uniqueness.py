from typing import Any, Optional, Union

import whylogs_datasketches as ds  # type: ignore

from whylogs_v1.core.extraction import ExtractedColumn
from whylogs_v1.core.metrics import MergeableMetric, UpdatableMetric
from whylogs_v1.core.proto import HllSketchMessage


class MergeableUniquenessMetric(MergeableMetric[HllSketchMessage, Any]):
    def __init__(self, k: int = 12, sketch: Optional[ds.hll_union] = None):
        if sketch is None:
            sketch = ds.hll_union(lg_max_k=k)
        self._sketch = sketch

    def merge(self, other: "MergeableUniquenessMetric") -> "MergeableUniquenessMetric":
        res = ds.hll_union(self._sketch.lg_config_k)
        res.update(self._sketch.get_result())
        res.update(other._sketch.get_result())
        return MergeableUniquenessMetric(sketch=res)

    def serialize(self) -> HllSketchMessage:
        return HllSketchMessage()

    @classmethod
    def fromSerialized(cls, msg: HllSketchMessage) -> "MergeableUniquenessMetric":
        return MergeableUniquenessMetric()


SCALAR_TYPES = Union[int, str, float]


class UpdatableUniquenessMetric(
    UpdatableMetric[SCALAR_TYPES, HllSketchMessage, MergeableUniquenessMetric]
):
    def __init__(self, k: int = 12, sketch: Optional[ds.hll_sketch] = None) -> None:
        if sketch is None:
            sketch = ds.hll_sketch(lg_k=k)
        self._sketch = sketch

    def update(self, value: SCALAR_TYPES) -> None:
        self._sketch.update(value)

    def columnar_update(self, view: ExtractedColumn) -> None:
        if view.numpy.len > 0:
            if view.numpy.ints is not None:
                self._sketch.batch_update_int(view.numpy.ints)
            if view.numpy.floats is not None:
                self._sketch.batch_update_double(view.numpy.floats)
        if view.pandas.strings is not None:
            view.pandas.strings.apply(self._sketch.update)

        # update everything in the remaining lists
        for it in view.list.iterables():
            for i in it:
                self._sketch.update(i)

    def to_mergeable(self) -> "MergeableUniquenessMetric":
        pass

    def serialize(self) -> HllSketchMessage:
        return HllSketchMessage(
            lg_k=self._sketch.lg_config_k, sketch=self._sketch.serialize_compact()
        )

    @classmethod
    def fromSerialized(cls, msg: HllSketchMessage) -> "UpdatableUniquenessMetric":
        if msg is None:
            return UpdatableUniquenessMetric()
        sketch = ds.hll_sketch(lg_max_k=msg.lg_k)
        sketch.deserialize(msg.sketch)
        return UpdatableUniquenessMetric(sketch=sketch)
