import logging
from datetime import datetime
from functools import reduce
from typing import Any, Dict, List, Optional, Union

from typing_extensions import Literal

from whylogs.api.logger.events import log_debug_event
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
from whylogs.api.usage_stats import emit_usage
from whylogs.api.whylabs.session.notebook_logger import (
    notebook_session_log,
    notebook_session_log_comparison,
)
from whylogs.core import DatasetProfile, DatasetSchema
from whylogs.core.metadata import WHYLABS_TRACE_ID_KEY
from whylogs.core.model_performance_metrics.model_performance_metrics import (
    ModelPerformanceMetrics,
)
from whylogs.core.stubs import pd

diagnostic_logger = logging.getLogger(__name__)

Loggable = Union["pd.DataFrame", List[Dict[str, Any]]]


def log(
    obj: Any = None,
    *,
    pandas: Optional[pd.DataFrame] = None,
    row: Optional[Dict[str, Any]] = None,
    schema: Optional[DatasetSchema] = None,
    name: Optional[str] = None,
    multiple: Optional[Dict[str, Loggable]] = None,
    dataset_timestamp: Optional[datetime] = None,
    trace_id: Optional[str] = None,
    tags: Optional[List[str]] = None,
    segment_key_values: Optional[Dict[str, str]] = None,
    debug_event: Optional[Dict[str, Any]] = None,
) -> ResultSet:
    if multiple is not None:
        result_sets: Dict[str, ResultSet] = {}
        emit_usage("multiple")
        for alias, data in multiple.items():
            result_set = TransientLogger(schema=schema).log(data, trace_id=trace_id)
            if dataset_timestamp is not None:
                result_set.set_dataset_timestamp(dataset_timestamp)
            result_sets[alias] = result_set

        # Return one result set with everything in it since we have to return result_sets
        result_set = reduce(lambda r1, r2: r1.merge(r2), result_sets.values())
        notebook_session_log_comparison(multiple, result_sets)
        return result_set
    else:
        result_set = TransientLogger(schema=schema).log(
            obj, pandas=pandas, row=row, name=name, trace_id=trace_id, tags=tags, segment_key_values=segment_key_values
        )
        if dataset_timestamp is not None:
            result_set.set_dataset_timestamp(dataset_timestamp)
        notebook_session_log(result_set, obj, pandas=pandas, row=row, name=name)

        if debug_event is not None:
            if trace_id is None and WHYLABS_TRACE_ID_KEY in result_set.metadata:
                trace_id = result_set.metadata.get(WHYLABS_TRACE_ID_KEY)
            debug_event_status = log_debug_event(
                debug_event=debug_event,
                trace_id=trace_id,
                tags=tags,
                segment_key_values=segment_key_values,
                dataset_timestamp=dataset_timestamp,
            )
            diagnostic_logger.info(f"Done log_debug_event: {debug_event_status}")
        return result_set


def _log_with_metrics(
    data: pd.DataFrame,
    metrics: ModelPerformanceMetrics,
    schema: Optional[DatasetSchema],
    include_data: bool,
    dataset_timestamp: Optional[datetime] = None,
) -> ResultSet:
    if include_data:
        results = log(pandas=data, schema=schema, dataset_timestamp=dataset_timestamp)
    else:
        results = ProfileResultSet(DatasetProfile(schema=schema))

    results.add_model_performance_metrics(metrics)
    return results


def _performance_metric(
    pandas: pd.DataFrame, perf_columns: Dict[str, Optional[str]], metric_name: str
) -> ModelPerformanceMetrics:
    performance_values = {
        p: pandas[perf_columns[p]].to_list() if perf_columns[p] in pandas else None for p in perf_columns
    }
    model_performance_metrics = ModelPerformanceMetrics()
    metric_function = getattr(model_performance_metrics, metric_name)
    metric_function(**performance_values)
    return model_performance_metrics


def _segmented_performance_metrics(
    log_full_data: bool,
    schema: DatasetSchema,
    data: pd.DataFrame,
    performance_column_mapping: Dict[str, Optional[str]],
    performance_metric: str,
    dataset_timestamp: Optional[datetime] = None,
) -> SegmentedResultSet:
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

    result_set = SegmentedResultSet(segments=segmented_profiles, partitions=segment_partitions)
    if dataset_timestamp is not None:
        result_set.set_dataset_timestamp(dataset_timestamp)

    return result_set


def log_classification_metrics(
    data: pd.DataFrame,
    target_column: str,
    prediction_column: str,
    score_column: Optional[str] = None,
    schema: Optional[DatasetSchema] = None,
    log_full_data: bool = False,
    dataset_timestamp: Optional[datetime] = None,
) -> ResultSet:
    """
    Function to track metrics based on validation data.
    user may also pass the associated attribute names associated with
    target, prediction, and/or score.

    Parameters
    ----------
    data : pd.DataFrame
        Dataframe with the data to log.
    target_column : str
        Column name for the actual validated values.
    prediction_column : str
        Column name for the predicted values.
    score_column : Optional[str], optional
        Associated scores for each inferred, all values set to 1 if None, by default None
    schema : Optional[DatasetSchema], optional
        Defines the schema for tracking metrics in whylogs, by default None
    log_full_data : bool, optional
        Whether to log the complete dataframe or not.
        If True, the complete DF will be logged in addition to the regression metrics.
        If False, only the calculated regression metrics will be logged.
        In a typical production use case, the ground truth might not be available
        at the time the remaining data is generated. In order to prevent double profiling the
        input features, consider leaving this as False. by default False.
    dataset_timestamp : Optional[datetime], optional
        dataset's timestamp, by default None

    Examples
    --------
    ::

        data = {
            "product": ["milk", "carrot", "cheese", "broccoli"],
            "category": ["dairies", "vegetables", "dairies", "vegetables"],
            "output_discount": [0, 0, 1, 1],
            "output_prediction": [0, 0, 0, 1],
        }
        df = pd.DataFrame(data)

        results = why.log_classification_metrics(
                df,
                target_column="output_discount",
                prediction_column="output_prediction",
                log_full_data=True,
            )


    """

    perf_column_mapping = {"predictions": prediction_column, "targets": target_column, "scores": score_column}

    if schema and schema.segments:
        return _segmented_performance_metrics(
            log_full_data,
            schema=schema,
            data=data,
            performance_column_mapping=perf_column_mapping,
            performance_metric="compute_confusion_matrix",
            dataset_timestamp=dataset_timestamp,
        )

    model_performance_metrics = _performance_metric(
        pandas=data, perf_columns=perf_column_mapping, metric_name="compute_confusion_matrix"
    )

    return _log_with_metrics(
        data=data,
        metrics=model_performance_metrics,
        schema=schema,
        include_data=log_full_data,
        dataset_timestamp=dataset_timestamp,
    )


def log_regression_metrics(
    data: pd.DataFrame,
    target_column: str,
    prediction_column: str,
    schema: Optional[DatasetSchema] = None,
    log_full_data: bool = False,
    dataset_timestamp: Optional[datetime] = None,
) -> ResultSet:
    """Function to track regression metrics based on validation data.
    User may also pass the associated attribute names associated with target, prediction, and/or score.

    Parameters
    ----------
    data : pd.DataFrame
        Dataframe with the data to log.
    target_column : str
        Column name for the target values.
    prediction_column : str
        Column name for the predicted values.
    schema : Optional[DatasetSchema], optional
        Defines the schema for tracking metrics in whylogs, by default None
    log_full_data : bool, optional
        Whether to log the complete dataframe or not.
        If True, the complete DF will be logged in addition to the regression metrics.
        If False, only the calculated regression metrics will be logged.
        In a typical production use case, the ground truth might not be available
        at the time the remaining data is generated. In order to prevent double profiling the
        input features, consider leaving this as False. by default False.
    dataset_timestamp : Optional[datetime], optional
        dataset's timestamp, by default None

    Returns
    -------
    ResultSet

    Examples
    --------
    ::

        import pandas as pd
        import whylogs as why

        df = pd.DataFrame({"target_temperature": [[10.5, 24.3, 15.6]], "predicted_temperature": [[9.12,26.42,13.12]]})
        results = why.log_regression_metrics(df, target_column = "temperature", prediction_column = "prediction_temperature")


    """
    perf_column_mapping: Dict[str, Optional[str]] = {"predictions": prediction_column, "targets": target_column}

    if schema and schema.segments:
        return _segmented_performance_metrics(
            log_full_data,
            schema=schema,
            data=data,
            performance_column_mapping=perf_column_mapping,
            performance_metric="compute_regression_metrics",
            dataset_timestamp=dataset_timestamp,
        )

    model_performance_metrics = _performance_metric(
        pandas=data, perf_columns=perf_column_mapping, metric_name="compute_regression_metrics"
    )

    return _log_with_metrics(
        data=data,
        metrics=model_performance_metrics,
        schema=schema,
        include_data=log_full_data,
        dataset_timestamp=dataset_timestamp,
    )


def read(path: str) -> ResultSet:
    return ResultSet.read(path)


def reader(name: str) -> ResultSetReader:
    return ResultSet.reader(name=name)


def write(profile: DatasetProfile, base_dir: Optional[str] = None, filename: Optional[str] = None) -> None:
    if base_dir and filename:
        return profile._write(base_dir, filename)
    else:
        return profile._write("", base_dir)  # backward compatibility: use base_dir as full filename


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
