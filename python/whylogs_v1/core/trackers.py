from typing import TypeVar

from whylogs_v1.core.datatypes import DataType
from whylogs_v1.core.metrics.metrics import Metric, OperationResult
from whylogs_v1.core.preprocessing import PreprocessColumn
from whylogs_v1.core.proto import MetricMessage


class Tracker(object):
    def __init__(self, data_type: DataType, metric: Metric) -> None:
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

    def serialize(self) -> MetricMessage:
        return self._metric.serialize()


N = TypeVar("N", bound=DataType)
