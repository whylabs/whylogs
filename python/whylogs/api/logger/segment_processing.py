import logging
import math
from functools import reduce
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Tuple

from whylogs.api.logger.result_set import SegmentedResultSet
from whylogs.api.logger.segment_cache import SegmentCache
from whylogs.core import DatasetSchema
from whylogs.core.dataset_profile import DatasetProfile
from whylogs.core.input_resolver import _pandas_or_dict
from whylogs.core.segment import Segment
from whylogs.core.segmentation_partition import SegmentationPartition, SegmentFilter
from whylogs.core.stubs import pd

logger = logging.getLogger(__name__)

_MAX_SEGMENT_PARTITIONS = 10


def _process_segment(
    segmented_data: Any,
    segment_key: Segment,
    segments: Dict[Segment, Any],
    schema: DatasetSchema,
    segment_cache: Optional[SegmentCache] = None,
):
    profile = None
    if segment_cache:
        profile = segment_cache.get_or_create_matching_profile(segment_key)

    if profile is None:
        profile = DatasetProfile(schema)

    profile.track(segmented_data, execute_udfs=False)
    segments[segment_key] = profile


def _get_segment_from_group_key(group_key, partition_id) -> Tuple[str, ...]:
    if isinstance(group_key, str):
        segment_tuple_key: Tuple[str, ...] = (group_key,)
    elif isinstance(group_key, (List, Iterable, Iterator)):
        segment_tuple_key = tuple(str(k) for k in group_key)
    else:
        segment_tuple_key = (str(group_key),)

    return Segment(segment_tuple_key, partition_id)


def _is_nan(x):
    try:
        return math.isnan(x)
    except TypeError:
        return False


def _process_simple_partition(
    partition_id: str,
    schema: DatasetSchema,
    segments: Dict[Segment, Any],
    columns: List[str],
    pandas: Optional[pd.DataFrame] = None,
    row: Optional[Mapping[str, Any]] = None,
    segment_cache: Optional[SegmentCache] = None,
):
    if pandas is not None:
        # simple means we can segment on column values
        grouped_data = pandas.groupby(columns)
        for group in grouped_data.groups.keys():
            if isinstance(group, tuple) and any([_is_nan(x) for x in group]):
                evaluations = []
                for val, col in zip(group, columns):
                    if _is_nan(val):
                        evaluations.append((pandas[col].isna()))
                    else:
                        evaluations.append((pandas[col] == val))
                mask = reduce(lambda x, y: x & y, evaluations)
                pandas_segment = pandas[mask]
            else:
                pandas_segment = grouped_data.get_group(group)
            segment_key = _get_segment_from_group_key(group, partition_id)
            _process_segment(pandas_segment, segment_key, segments, schema, segment_cache)
    elif row:
        # TODO: consider if we need to combine with the column names
        segment_key = Segment(tuple(str(row[element]) for element in columns), partition_id)
        _process_segment(row, segment_key, segments, schema, segment_cache)


def _filter_inputs(
    filter: SegmentFilter, pandas: Optional[pd.DataFrame] = None, row: Optional[Mapping[str, Any]] = None
) -> Tuple[Optional[pd.DataFrame], Optional[Dict[str, Any]]]:
    assert (
        filter.filter_function or filter.query_string
    ), f"must define at least a filter function or query string when specifying a segment filter: {filter}"
    filtered_pandas = None
    filtered_row = None
    if pandas is not None:
        if filter.filter_function:
            filtered_pandas = pandas[filter.filter_function]
        elif filter.query_string:
            filtered_pandas = pandas.query(filter.query_string)
    elif row is not None:
        if filter.filter_function:
            filtered_row = filter.filter_function(row)
        elif filter.query_string:
            raise ValueError(
                "SegmentFilter query string not supported when logging rows, either don't specify a filter or implement the filter.filter_function"
            )
    return (filtered_pandas, filtered_row)


def _grouped_dataframe(partition: SegmentationPartition, pandas: pd.DataFrame):
    columns = partition.mapper.col_names if partition.mapper else None
    if not columns:
        raise ValueError(
            "Please use column segmentation, there are no columns defined and ColumnMapperFunction not yet supported."
        )
    grouped_data = pandas.groupby(columns)
    return grouped_data


def _log_segment(
    partition: SegmentationPartition,
    schema: DatasetSchema,
    obj: Any = None,
    pandas: Optional[pd.DataFrame] = None,
    row: Optional[Mapping[str, Any]] = None,
    segment_cache: Optional[SegmentCache] = None,
) -> Dict[Segment, Any]:
    segments: Dict[Segment, Any] = {}
    pandas, row = _pandas_or_dict(obj, pandas, row)
    if partition.filter:
        pandas, row = _filter_inputs(partition.filter, pandas, row)
    if partition.simple:
        columns = partition.mapper.col_names if partition.mapper else None
        if columns:
            _process_simple_partition(partition.id, schema, segments, columns, pandas, row, segment_cache)
    else:
        raise NotImplementedError("custom mapped segments not yet implemented")
    return segments


def segment_processing(
    schema: DatasetSchema,
    obj: Any = None,
    pandas: Optional[pd.DataFrame] = None,
    row: Optional[Dict[str, Any]] = None,
    segment_cache: Optional[SegmentCache] = None,
) -> SegmentedResultSet:
    number_of_partitions = len(schema.segments)
    logger.info(f"The specified schema defines segments with {number_of_partitions} partitions.")
    assert not (
        number_of_partitions > _MAX_SEGMENT_PARTITIONS
    ), f"Attempt to process {number_of_partitions} partitions is larger than the max of {_MAX_SEGMENT_PARTITIONS}, use a lower number of partitions"
    segmented_profiles = dict()
    segment_partitions = list()

    for partition_name in schema.segments:
        segment_partition = schema.segments[partition_name]
        logger.info(f"Processing partition with name({partition_name})")
        logger.debug(f"{partition_name}: is simple ({segment_partition.simple}), id ({segment_partition.id})")
        if segment_partition.filter:
            # TODO segments filter
            logger.debug(f"{partition_name}: defines filter ({segment_partition.filter})")
        if segment_partition.mapper:
            logger.debug(
                f"{partition_name}: defines mapper on colums ({segment_partition.mapper.col_names}) and id ({segment_partition.mapper.id})"
            )
        partition_segments = _log_segment(
            partition=segment_partition,
            schema=schema,
            obj=obj,
            pandas=pandas,
            row=row,
            segment_cache=segment_cache,
        )
        segmented_profiles[segment_partition.id] = partition_segments
        segment_partitions.append(segment_partition)
        logger.debug(f"Done profiling for partition with name({partition_name})")
    return SegmentedResultSet(segments=segmented_profiles, partitions=segment_partitions)
