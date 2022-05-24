from enum import Enum

from whylogs.core.metrics.column_metrics import ColumnCountsMetric, TypeCountersMetric
from whylogs.core.metrics.metrics import (
    CardinalityMetric,
    DistributionMetric,
    FrequentItemsMetric,
    IntsMetric,
    Metric,
)
from whylogs.core.metrics.unicode_range import UnicodeRangeMetric


class StandardMetric(Enum):
    types = TypeCountersMetric
    distribution = DistributionMetric
    counts = ColumnCountsMetric
    ints = IntsMetric
    cardinality = CardinalityMetric
    frequent_items = FrequentItemsMetric
    unicode_range = UnicodeRangeMetric

    def __init__(self, clz: Metric):
        self._clz = clz

    def zero(self, schema) -> Metric:  # type: ignore
        return self._clz.zero(schema)
