from typing import TypeVar

from whylogs_v1.core.datatypes import DataType
from whylogs_v1.core.metrics import UpdatableMetric
from whylogs_v1.core.metrics.metrics import MergeableMetric, OperationResult
from whylogs_v1.core.preprocessing import PreprocessColumn
from whylogs_v1.core.proto import TrackerMessage


class Tracker(object):
    def __init__(self, data_type: DataType, metric: UpdatableMetric) -> None:
        self._data_type = data_type
        self._metric = metric

    def track_column(self, series: PreprocessColumn) -> OperationResult:
        """
        Track a series of values in 'ExtractedColumn'

        Args:
            series: 'ExtractedColumn' that might contain different series

        Returns:
            Number of successful operations
        """
        return self._metric.columnar_update(series)

    def serialize(self) -> TrackerMessage:
        return self._metric.serialize()

    def to_mergeable(self) -> MergeableMetric:
        return self._metric.to_mergeable()


N = TypeVar("N", bound=DataType)


class NumericTracker(Tracker):
    """
    A tracker for numbers.

    Note that you'll need to specify the generic type as well if mypy is enabled, but at the same
    time specify the expected type in the constructor.
    """

    def __init__(self, tpe: DataType, metric: UpdatableMetric):
        super().__init__(tpe, metric)
