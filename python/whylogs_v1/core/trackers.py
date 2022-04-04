from typing import Any, Callable, Generic, Type, TypeVar

from whylogs_v1.core.datatypes import DataType, NumericColumnType
from whylogs_v1.core.extraction import ExtractedColumn
from whylogs_v1.core.metrics import UpdatableMetric


class Tracker(object):
    def __init__(self, data_type: DataType, metric: UpdatableMetric) -> None:
        self._data_type = data_type
        self._metric = metric

    def track(self, value: Any) -> int:
        if not self._data_type.is_compatible(value):
            return 1
        normalized_value = self._data_type.normalize(value)
        self._metric.update(normalized_value)
        return 0

    def track_column(self, series: ExtractedColumn) -> int:
        """
        Track a series of values in 'ExtractedColumn'

        Args:
            series: 'ExtractedColumn' that might contain different series

        Returns:
            Number of successful operations
        """
        self._metric.columnar_update(series)
        return 0


N = TypeVar("N", float, int)


class NumericTracker(Tracker, Generic[N]):
    """
    A tracker for numbers.

    Note that you'll need to specify the generic type as well if mypy is enabled, but at the same
    time specify the expected type in the constructor.
    """

    def __init__(self, tpe: Type[N], metric: UpdatableMetric):
        super().__init__(NumericColumnType(tpe), metric)


METRIC = TypeVar("METRIC")
_TrackerFactory = Callable[[], Tracker]
