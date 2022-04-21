from enum import Enum

from whylogs.core.metrics.column_metrics import ColumnCountsMetric, TypeCountersMetric
from whylogs.core.metrics.metrics import (
    CardinalityMetric,
    DistributionMetric,
    FrequentItemsMetric,
    IntsMetric,
    Metric,
)


class StandardMetric(Enum):
    types = TypeCountersMetric
    dist = DistributionMetric
    cnt = ColumnCountsMetric
    ints = IntsMetric
    card = CardinalityMetric
    fi = FrequentItemsMetric

    def __init__(self, clz: Metric):
        self._clz = clz

    def zero(self, schema) -> Metric:  # type: ignore
        return self._clz.zero(schema)
