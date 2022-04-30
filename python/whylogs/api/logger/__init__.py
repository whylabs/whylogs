from typing import Any, Dict, Optional

from typing_extensions import Literal

from whylogs.api.logger.logger import Logger
from whylogs.api.logger.result_set import ResultSet
from whylogs.api.logger.rolling import TimedRollingLogger
from whylogs.api.logger.transient import TransientLogger
from whylogs.core import DatasetProfile, DatasetSchema
from whylogs.core.stubs import pd


def log(
    obj: Any = None,
    *,
    pandas: Optional[pd.DataFrame] = None,
    row: Optional[Dict[str, Any]] = None,
    schema: Optional[DatasetSchema] = None,
) -> ResultSet:
    return TransientLogger(schema=schema).log(obj, pandas=pandas, row=row)


def read(path: str) -> ResultSet:
    return ResultSet.read(path)


def write(profile: DatasetProfile, base_dir: str) -> None:
    return profile.write(base_dir)


def logger(
    schema: Optional[DatasetSchema] = None,
    *,
    mode: Literal["transient", "rolling"] = "transient",
    **kwargs: Any,
) -> Logger:
    """

    Returns:
        Logger: a logger object
    """
    if mode == "transient":
        return TransientLogger(schema)
    elif mode == "rolling":
        return TimedRollingLogger(schema, **kwargs)
    else:
        raise ValueError("Unsupported logger mode")


__ALL__ = [
    "log",
    "read",
    "write",
    "logger",
    "Logger",
    "ResultSet",
    "RollingLogger",
]
