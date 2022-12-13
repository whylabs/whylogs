from typing import Any, Dict, Mapping, Optional, Tuple

from whylogs.core.stubs import pd


def _pandas_or_dict(
    obj: Any, pandas: Optional[pd.DataFrame] = None, row: Optional[Mapping[str, Any]] = None
) -> Tuple[Optional[pd.DataFrame], Optional[Mapping[str, Any]]]:
    if obj is not None:
        if pandas is not None:
            raise ValueError("Cannot pass both obj and pandas params")
        if row is not None:
            raise ValueError("Cannot pass both obj and row params")

        if isinstance(obj, (dict, Dict, Mapping)):
            row = obj
        elif pd.DataFrame is not None and isinstance(obj, pd.DataFrame):
            pandas = obj

    if pandas is not None and row is not None:
        raise ValueError("Cannot pass both pandas and row params")

    return (pandas, row)
