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


def log(
    obj: Any = None,
    *,
    pandas: Optional[pd.DataFrame] = None,
    row: Optional[Dict[str, Any]] = None,
    schema: Optional[DatasetSchema] = None,
) -> ResultSet:
    return TransientLogger(schema=schema).log(obj, pandas=pandas, row=row)


def log_classification_metrics(
    targets,
    predictions,
    scores=None,
    target_field=None,
    prediction_field=None,
    score_field=None,
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
    target_field : str, optional
        Description
    prediction_field : str, optional
        Description
    score_field : str, optional
        Description
    target_field : str, optional
    prediction_field : str, optional
    score_field : str, optional
    """
    model_performance_metrics = ModelPerformanceMetrics()
    model_performance_metrics.compute_confusion_matrix(
        predictions=predictions,
        targets=targets,
        scores=scores,
        target_field=target_field,
        prediction_field=prediction_field,
        score_field=score_field,
    )
    profile = DatasetProfile()
    profile.add_model_performance_metric(model_performance_metrics)
    return ProfileResultSet(profile)


def log_regression_metrics(
    targets,
    predictions,
    target_field=None,
    prediction_field=None,
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
    target_field : str, optional
        Description
    prediction_field : str, optional
        Description
    score_field : str, optional
        Description
    target_field : str, optional
    prediction_field : str, optional
    score_field : str, optional
    """
    model_performance_metrics = ModelPerformanceMetrics()
    model_performance_metrics.compute_regression_metrics(
        predictions=predictions,
        targets=targets,
        target_field=target_field,
        prediction_field=prediction_field,
    )
    profile = DatasetProfile()
    profile.add_model_performance_metric(model_performance_metrics)
    return ProfileResultSet(profile)


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
