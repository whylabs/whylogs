from typing import Dict, Optional

from whylogs_v1.core.datatypes import AnyType, DataType, NumericColumnType
from whylogs_v1.core.metrics import UpdatableCountMetric, UpdatableHistogram
from whylogs_v1.core.metrics.uniqueness import UpdatableUniquenessMetric
from whylogs_v1.core.trackers import NumericTracker, Tracker, _TrackerFactory


class ColumnSchema(object):
    def __init__(
        self,
        col_name: str,
        data_type: DataType,
        tracker_factories: Optional[Dict[str, _TrackerFactory]] = None,
    ) -> None:
        self._col_name = col_name
        self._data_type = data_type
        if tracker_factories is not None:
            self._tracker_factories = tracker_factories
        elif isinstance(self._data_type, NumericColumnType):
            self._tracker_factories = {
                "hist": lambda: NumericTracker[float](float, UpdatableHistogram()),
                "count": lambda: Tracker(AnyType(), UpdatableCountMetric()),
                "uniq": lambda: Tracker(AnyType(), UpdatableUniquenessMetric()),
            }
        else:
            self._tracker_factories = {
                "uniq": lambda: Tracker(AnyType(), UpdatableUniquenessMetric()),
            }

    @property
    def name(self) -> str:
        return self._col_name

    @property
    def tracker_factories(self) -> Dict[str, _TrackerFactory]:
        return self._tracker_factories

    def add_tracker_factory(self, name: str, tracker_factory: _TrackerFactory) -> None:
        if name in self._tracker_factories:
            raise ValueError(f"{name} tracker already exists")
        self._tracker_factories[name] = tracker_factory
