import logging
from typing import Any, Dict, Optional

from typing_extensions import Literal

from whylogs.api.logger.logger import Logger
from whylogs.api.logger.result_set import (
    ProfileResultSet,
    ResultSet,
    ResultSetReader,
    SegmentedResultSet,
)
from whylogs.api.logger.rolling import TimedRollingLogger
from whylogs.api.logger.segment_processing import (
    _get_segment_from_group_key,
    _grouped_dataframe,
    _log_segment,
)
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


def _performance_metric(pandas, perf_columns, metric_name):
    performance_values = {
        p: pandas[perf_columns[p]].to_list() if perf_columns[p] in pandas else None for p in perf_columns
    }
    model_performance_metrics = ModelPerformanceMetrics()
    metric_function = getattr(model_performance_metrics, metric_name)
    metric_function(**performance_values)
    return model_performance_metrics


def _segmented_performance_metrics(log_full_data, schema, data, performance_column_mapping, performance_metric):
    segmented_profiles = dict()
    segment_partitions = list()
    if log_full_data:
        for partition_name in schema.segments:
            partition = schema.segments.get(partition_name)
            diagnostic_logger.info(f"Processing partition {partition_name}")
            partition_segments = _log_segment(partition, schema, pandas=data)
            diagnostic_logger.info(f"Partition {partition_name} had {len(partition_segments)} segments.")
            segmented_profiles[partition.id] = partition_segments

    for partition_name in schema.segments:
        partition = schema.segments.get(partition_name)
        grouped_data = _grouped_dataframe(partition, pandas=data)
        partition_segments = segmented_profiles.get(partition.id) or dict()
        for group_key in grouped_data.groups.keys():
            pandas_segment = grouped_data.get_group(group_key)
            segment_key = _get_segment_from_group_key(group_key, partition.id)
            diagnostic_logger.info(f"Computing {performance_metric} for segment {partition_name}->{segment_key}")

            profile = partition_segments.get(segment_key) or DatasetProfile(schema)
            model_performance_metrics = _performance_metric(
                pandas_segment, performance_column_mapping, performance_metric
            )
            profile.add_model_performance_metrics(model_performance_metrics)
            partition_segments[segment_key] = profile
        segmented_profiles[partition.id] = partition_segments
        segment_partitions.append(partition)

    return SegmentedResultSet(segments=segmented_profiles, partitions=segment_partitions)


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

    perf_column_mapping = {"predictions": prediction_column, "targets": target_column, "scores": score_column}

    if schema and schema.segments:
        return _segmented_performance_metrics(
            log_full_data,
            schema=schema,
            data=data,
            performance_column_mapping=perf_column_mapping,
            performance_metric="compute_confusion_matrix",
        )

    model_performance_metrics = _performance_metric(
        pandas=data, perf_columns=perf_column_mapping, metric_name="compute_confusion_matrix"
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
    perf_column_mapping = {"predictions": prediction_column, "targets": target_column}

    if schema and schema.segments:
        return _segmented_performance_metrics(
            log_full_data,
            schema=schema,
            data=data,
            performance_column_mapping=perf_column_mapping,
            performance_metric="compute_regression_metrics",
        )

    model_performance_metrics = _performance_metric(
        pandas=data, perf_columns=perf_column_mapping, metric_name="compute_regression_metrics"
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
