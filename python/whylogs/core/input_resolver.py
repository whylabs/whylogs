from typing import Any, Dict, Mapping, Optional, Tuple

from whylogs.core.dataframe_wrapper import DataFrame, DataFrameWrapper
from whylogs.core.stubs import pd, pl


def _dataframe_or_dict(
    obj: Any,
    dataframe: Optional[DataFrame] = None,
    row: Optional[Mapping[str, Any]] = None,
) -> Tuple[Optional[DataFrameWrapper], Optional[Mapping[str, Any]]]:
    if obj is not None:
        if dataframe is not None:
            raise ValueError("Cannot pass both obj and dataframe params")
        if row is not None:
            raise ValueError("Cannot pass both obj and row params")

        if isinstance(obj, (dict, Dict, Mapping)):
            return (None, obj)
        elif isinstance(obj, DataFrameWrapper):
            return (obj, None)
        elif isinstance(obj, (pd.DataFrame, pl.DataFrame)):
            return (DataFrameWrapper(obj), None)

    if dataframe is not None and row is not None:
        raise ValueError("Cannot pass both dataframe and row params")

    df = DataFrameWrapper(dataframe) if dataframe is not None else None
    return (df, row)
