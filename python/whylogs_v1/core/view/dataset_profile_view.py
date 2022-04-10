import logging
from typing import Dict, Optional

from whylogs_v1.core.proto import DatasetProfileMessage
from whylogs_v1.core.view.column_profile_view import ColumnProfileView

logger = logging.getLogger(__name__)


class DatasetProfileView(object):
    _columns: Dict[str, ColumnProfileView]

    def __init__(self, columns: Dict[str, ColumnProfileView]):
        self._columns = columns.copy()

    def merge(self, other: "DatasetProfileView") -> "DatasetProfileView":
        all_names = set(self._columns.keys()).union(other._columns.keys())
        merged_columns: Dict[str, ColumnProfileView] = {}
        for n in all_names:
            lhs = self._columns.get(n)
            rhs = other._columns.get(n)

            res = lhs
            if lhs is None:
                res = rhs
            elif rhs is not None:
                res = lhs + rhs
            assert res is not None
            merged_columns[n] = res
        return DatasetProfileView(columns=merged_columns)

    def get_column(self, col_name: str) -> Optional[ColumnProfileView]:
        return self._columns.get(col_name)

    def serialize(self) -> DatasetProfileMessage:
        res = {}
        for col_name, col in self._columns.items():
            res[col_name] = col.serialize()
        return DatasetProfileMessage(columns=res)

    @classmethod
    def deserialize(cls, msg: DatasetProfileMessage) -> "DatasetProfileView":
        res = {}
        for col_name, col_msg in msg.columns.items():
            res[col_name] = ColumnProfileView.deserialize(col_msg)

        return DatasetProfileView(columns=res)
