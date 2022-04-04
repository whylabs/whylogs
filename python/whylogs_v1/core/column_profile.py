from typing import Any, Dict, Optional

from .extraction import ExtractedColumn
from .projectors import SingleFieldProjector
from .schema import ColumnSchema


class ColumnProfile(object):
    def __init__(self, schema: ColumnSchema):
        self._schema = schema

        self._trackers = dict()
        for name, tracker_lambda in schema.tracker_factories.items():
            self._trackers[name] = tracker_lambda()
        self._projector = SingleFieldProjector(col_name=schema.name)
        self._failure_count = 0

    def track(self, row: Dict[str, Any]) -> None:
        value = self._projector.apply(row)
        self.track_value(value)

    def track_value(self, datum: Optional[Any]) -> None:
        failure_count = 0
        for tracker in self._trackers.values():
            failure_count += tracker.track(datum)

    def track_column(self, series: Any) -> None:
        ex_col = ExtractedColumn.apply(series)
        for tracker in self._trackers.values():
            tracker.track_column(ex_col)
