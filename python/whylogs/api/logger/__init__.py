import logging
from typing import Any, Dict, Optional

from typing_extensions import Literal

from whylogs.api.logger.logger import Logger
from whylogs.api.logger.result_set import ProfileResultSet, ResultSet, ResultSetReader
from whylogs.api.logger.rolling import TimedRollingLogger
from whylogs.api.logger.transient import TransientLogger
from whylogs.core import DatasetProfile, DatasetSchema
from whylogs.core.model_performance_metrics.model_performance_metrics import (
    ModelPerformanceMetrics,
)
from whylogs.core.stubs import pd

diagnostic_logger = logging.getLogger(__name__)


def log(
    obj: Any = None,
    *,
    pandas: Optional[pd.DataFrame] = None,
    row: Optional[Dict[str, Any]] = None,
    schema: Optional[DatasetSchema] = None,
) -> ResultSet:
    return TransientLogger(schema=schema).log(obj, pandas=pandas, row=row)


def _log_with_metrics(
    data: pd.DataFrame,
    metrics: ModelPerformanceMetrics,
    schema: Optional[DatasetSchema],
    include_data: bool,
) -> ResultSet:
    if include_data:
        results = log(pandas=data, schema=schema)
    else:
        results = ProfileResultSet(DatasetProfile(schema=schema))
    results.add_model_performance_metrics(metrics)
    return results


def log_classification_metrics(
    data: pd.DataFrame,
    target_column: str,
    prediction_column: str,
    score_column: Optional[str] = None,
    schema: Optional[DatasetSchema] = None,
    log_full_data: bool = False,
) -> ProfileResultSet:
    """
    Function to track metrics based on validation data.
    user may also pass the associated attribute names associated with
    target, prediction, and/or score.
    Parameters
    ----------
    targets : List[Union[str, bool, float, int]]
        actual validated values
    predictions : List[Union[str, bool, float, int]]
        inferred/predicted values
    scores : List[float], optional
        assocaited scores for each inferred, all values set to 1 if not
        passed
    """

    if schema and schema.segments:
        diagnostic_logger.warning(
            "Model performance metrics do not yet support segmentation, unsegmented metrics will be computed"
        )

    model_performance_metrics = ModelPerformanceMetrics()
    model_performance_metrics.compute_confusion_matrix(
        predictions=data[prediction_column].to_list(),
        targets=data[target_column].to_list(),
        scores=data[score_column].to_list() if score_column else None,
    )

    return _log_with_metrics(data=data, metrics=model_performance_metrics, schema=schema, include_data=log_full_data)


def log_regression_metrics(
    data: pd.DataFrame,
    target_column: str,
    prediction_column: str,
    schema: Optional[DatasetSchema] = None,
    log_full_data: bool = False,
) -> ProfileResultSet:
    """
    Function to track regression metrics based on validation data.
    user may also pass the associated attribute names associated with
    target, prediction, and/or score.
    Parameters
    ----------
    targets : List[Union[str, bool, float, int]]
        actual validated values
    predictions : List[Union[str, bool, float, int]]
        inferred/predicted values
    scores : List[float], optional
        assocaited scores for each inferred, all values set to 1 if not
        passed
    """

    if schema and schema.segments:
        diagnostic_logger.warning(
            "Model performance metrics do not yet support segmentation, unsegmented metrics will be computed"
        )

    model_performance_metrics = ModelPerformanceMetrics()
    model_performance_metrics.compute_regression_metrics(
        predictions=data[prediction_column].to_list(),
        targets=data[target_column].to_list(),
    )

    return _log_with_metrics(data=data, metrics=model_performance_metrics, schema=schema, include_data=log_full_data)


def read(path: str) -> ResultSet:
    return ResultSet.read(path)


def reader(name: str) -> ResultSetReader:
    return ResultSet.reader(name=name)


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
    "reader",
    "write",
    "logger",
    "Logger",
    "ResultSet",
    "RollingLogger",
    "SegmentedResultSet",
]
