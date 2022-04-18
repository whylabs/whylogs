import logging
from enum import Enum
from typing import Dict, Optional

from whylogs.core.configs import SummaryConfig
from whylogs.core.proto import DatasetProfileMessage
from whylogs.core.stubs import pd
from whylogs.core.view.column_profile_view import ColumnProfileView

logger = logging.getLogger(__name__)


class SummaryType(str, Enum):
    COLUMN = "COLUMN"
    DATASET = "DATASET"


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

    def to_pandas(self, column_metric: Optional[str] = None, cfg: Optional[SummaryConfig] = None) -> pd.DataFrame:
        all_dicts = []
        for col_name, col in self._columns.items():
            sum_dict = col.to_summary_dict(column_metric=column_metric, cfg=cfg)
            sum_dict["column"] = col_name
            sum_dict["type"] = SummaryType.COLUMN
            all_dicts.append(sum_dict)
        df = pd.DataFrame(all_dicts)
        return df.set_index("column")

    @classmethod
    def deserialize(cls, msg: DatasetProfileMessage) -> "DatasetProfileView":
        res = {}
        for col_name, col_msg in msg.columns.items():
            res[col_name] = ColumnProfileView.deserialize(col_msg)

        return DatasetProfileView(columns=res)
