from typing import Any, Dict, Mapping, Optional, Tuple, Union

from whylogs.core.stubs import is_not_stub, pd, pl


def _pandas_or_dict(
    obj: Any, pandas: Optional[Union[pd.DataFrame, pl.DataFrame]] = None, row: Optional[Mapping[str, Any]] = None
) -> Tuple[Optional[Union[pd.DataFrame, pl.DataFrame]], Optional[Mapping[str, Any]]]:
    if obj is not None:
        if pandas is not None:
            raise ValueError("Cannot pass both obj and pandas params")
        if row is not None:
            raise ValueError("Cannot pass both obj and row params")

        if isinstance(obj, (dict, Dict, Mapping)):
            row = obj
        elif is_not_stub(pd.DataFrame) and isinstance(obj, pd.DataFrame):
            pandas = obj
        elif is_not_stub(pl.DataFrame) and isinstance(obj, pl.DataFrame):
            pandas = obj

    if pandas is not None and row is not None:
        raise ValueError("Cannot pass both pandas and row params")

    return (pandas, row)
