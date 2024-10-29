from typing import Any, Dict, Mapping, Optional, Tuple

from whylogs.core.dataframe_wrapper import DataFrameWrapper
from whylogs.core.stubs import pd, pl


def _dataframe_or_dict(
    obj: Any,
    pandas: Optional[pd.DataFrame] = None,
    polars: Optional[pl.DataFrame] = None,
    row: Optional[Mapping[str, Any]] = None,
) -> Tuple[Optional[DataFrameWrapper], Optional[Mapping[str, Any]]]:
    if obj is not None:
        if pandas is not None:
            raise ValueError("Cannot pass both obj and pandas params")
        if polars is not None:
            raise ValueError("Cannot pass both obj and polars params")
        if row is not None:
            raise ValueError("Cannot pass both obj and row params")

        if isinstance(obj, (dict, Dict, Mapping)):
            row = obj
        elif pd.DataFrame is not None and isinstance(obj, pd.DataFrame):
            pandas = obj
        elif pl.DataFrame is not None and isinstance(obj, pl.DataFrame):
            polars = obj

    if pandas is not None and row is not None:
        raise ValueError("Cannot pass both pandas and row params")

    if polars is not None and row is not None:
        raise ValueError("Cannot pass both polars and row params")

    df = DataFrameWrapper(pandas, polars) if (pandas is not None or polars is not None) else None
    return (df, row)
