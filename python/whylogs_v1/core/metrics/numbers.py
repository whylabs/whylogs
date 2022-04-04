import logging
from typing import Any, Optional

import whylogs_datasketches as ds  # type: ignore

from whylogs_v1.core.extraction import ExtractedColumn
from whylogs_v1.core.metrics.metrics import MergeableMetric, UpdatableMetric
from whylogs_v1.core.proto import CountMessage, KllSketchMessage

logger = logging.getLogger("whylogs.core.views")


class MergeableCountMetric(MergeableMetric[CountMessage, Any]):
    def __init__(self, value: int = 1):
        self._count = value

    def merge(self, other: "MergeableCountMetric") -> "MergeableCountMetric":
        total = self._count + other._count
        return MergeableCountMetric(total)

    def serialize(self) -> CountMessage:
        return CountMessage(value=self._count)

    @classmethod
    def fromSerialized(cls, data: CountMessage) -> "MergeableCountMetric":
        return MergeableCountMetric(data.value)


class UpdatableCountMetric(UpdatableMetric[Any, CountMessage, MergeableCountMetric]):
    def __init__(self, value: int = 0) -> None:
        self._count = value

    def update(self, _ignored: Any) -> None:
        self._count += 1

    def columnar_update(self, data: ExtractedColumn) -> None:
        if data.len > 0:
            self._count += data.len
        else:
            # TODO: warn about incompatible data type here
            pass

    def to_mergeable(self) -> "MergeableCountMetric":
        pass

    def serialize(self) -> CountMessage:
        return CountMessage(value=self._count)

    @classmethod
    def fromSerialized(cls, serialized: CountMessage) -> "UpdatableCountMetric":
        if serialized is None:
            return UpdatableCountMetric()
        return UpdatableCountMetric(serialized.value)


_DEFAULT_HIST_K = 256


class MergeableHistogram(MergeableMetric[KllSketchMessage, Any]):
    def __init__(
        self, k: int = _DEFAULT_HIST_K, sketch: Optional[ds.kll_floats_sketch] = None
    ):
        if sketch is None:
            sketch = ds.kll_floats_sketch(k)

        self._histogram = sketch

    def merge(self, other: "MergeableHistogram") -> "MergeableHistogram":  # type: ignore
        hist_copy = ds.kll_floats_sketch(self._histogram)
        hist_copy.merge(other._histogram)
        return MergeableHistogram(hist_copy.serialize())

    def serialize(self) -> KllSketchMessage:
        return KllSketchMessage(
            k=self._histogram.get_k(), sketch=self._histogram.serialize()
        )

    @classmethod
    def fromBytes(cls, k: int, msg: bytes) -> "MergeableHistogram":
        sk = ds.kll_floats_sketch(k)
        sk.deserialize(msg)
        return MergeableHistogram(sketch=sk)

    @classmethod
    def fromSerialized(cls, msg: KllSketchMessage) -> "MergeableHistogram":
        return MergeableHistogram.fromBytes(msg.k, msg.sketch)


class UpdatableHistogram(UpdatableMetric[float, KllSketchMessage, MergeableHistogram]):
    def __init__(
        self, k: int = _DEFAULT_HIST_K, sketch: Optional[ds.kll_floats_sketch] = None
    ):
        if sketch is not None:
            self._histogram = sketch
        else:
            self._histogram = ds.kll_floats_sketch(k)

    def update(self, value: float) -> None:
        self._histogram.update(float(value))

    def columnar_update(self, view: ExtractedColumn) -> None:
        # best scenario is numpy is available
        if view.numpy.len > 0:
            if view.numpy.floats is not None:
                self._histogram.update(view.numpy.floats)
            if view.numpy.ints is not None:
                self._histogram.update(view.numpy.ints)

        # these should be empty if the numpy above are done
        if view.list.ints:
            assert view.numpy.len == 0
            for i in view.list.ints:
                self._histogram.update(i)
        if view.list.floats:
            assert view.numpy.len == 0
            for f in view.list.floats:
                self._histogram.update(f)

    def to_mergeable(self) -> "MergeableHistogram":
        return MergeableHistogram.fromBytes(
            self._histogram.get_k(), self._histogram.serialize()
        )

    def serialize(self) -> KllSketchMessage:
        return KllSketchMessage(
            k=self._histogram.get_k(), sketch=self._histogram.serialize()
        )

    @classmethod
    def fromSerialized(cls, msg: KllSketchMessage) -> "UpdatableHistogram":
        sk = ds.kll_floats_sketch(msg.k)
        sk.deserialize(msg.sketch)
        return UpdatableHistogram(sketch=sk.deserialize(msg.sketch))
