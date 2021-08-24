import datetime
from enum import Enum

import datasketches
import pandas as pd

from whylogs.proto import HllSketchMessage, UniqueCountSummary

DEFAULT_LG_K = 12


class HllSketch:
    def __init__(self, lg_k=None, sketch=None):
        if sketch is None:
            if lg_k is None:
                lg_k = DEFAULT_LG_K
            sketch = datasketches.hll_sketch(lg_k)
        assert isinstance(sketch, datasketches.hll_sketch)
        self.sketch = sketch
        self.lg_k = lg_k

    def update(self, value):
        try:
            self.sketch.update(value)
        except TypeError:
            value = self._serialize_item(value)
            self.sketch.update(value)

    def merge(self, other):
        lg_k = max(self.lg_k, other.lg_k)
        union = datasketches.hll_union(lg_k)
        union.update(self.sketch)
        union.update(other.sketch)
        return HllSketch(lg_k, union.get_result())

    def get_estimate(self):
        return self.sketch.get_estimate()

    def get_lower_bound(self, num_std_devs: int = 1):
        return self.sketch.get_lower_bound(num_std_devs)

    def get_upper_bound(self, num_std_devs: int = 1):
        return self.sketch.get_upper_bound(num_std_devs)

    def to_protobuf(self):
        return HllSketchMessage(sketch=self.sketch.serialize_compact(), lg_k=self.lg_k)

    def _serialize_item(self, x):
        if isinstance(x, datetime.datetime):
            return x.isoformat()
        elif isinstance(x, Enum):
            return x.value
        else:
            return pd.io.json.dumps(x)

    def is_empty(self):
        return self.sketch.is_empty()

    @staticmethod
    def from_protobuf(message: HllSketchMessage):
        if len(message.sketch) == 0:
            return HllSketch()
        sketch = datasketches.hll_sketch.deserialize(message.sketch)
        return HllSketch(message.lg_k, sketch)

    def to_summary(self, num_std_devs=1):
        if self.is_empty():
            return None
        return UniqueCountSummary(
            estimate=self.get_estimate(),
            upper=self.get_upper_bound(),
            lower=self.get_lower_bound(),
        )
