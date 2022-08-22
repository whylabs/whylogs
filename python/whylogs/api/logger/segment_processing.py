import logging
from typing import Any, Dict, List, Optional

from whylogs.api.logger.result_set import SegmentedResultSet
from whylogs.api.store.profile_store import ProfileStore
from whylogs.core import DatasetSchema
from whylogs.core.dataset_profile import DatasetProfile
from whylogs.core.errors import LoggingError
from whylogs.core.input_resolver import _pandas_or_dict
from whylogs.core.segmentation_partition import Segment, SegmentationPartition
from whylogs.core.stubs import pd

logger = logging.getLogger(__name__)

_MAX_SEGMENT_PARTITIONS = 10


def _split_pandas_dataframe(pandas: pd.DataFrame, partition: SegmentationPartition) -> Dict[Segment, pd.DataFrame]:
    mapper = partition.mapper
    if mapper.map:
        # do stuff
        print("hi")
    pass


def _log_pandas_segment(partition: SegmentationPartition, pandas: pd.DataFrame) -> Dict[Segment, Any]:
    if partition.filter:
        filtered_df = pandas.query(partition.filter)
    pass
    # pandas.groupby(partition.mapper)


def _process_segment(
    segmented_data: Any,
    segment_key: Segment,
    segments: Dict[Segment, Any],
    schema: DatasetSchema,
    store: Optional[ProfileStore] = None,
):

    profile = store.get_matching_profiles(segmented_data, segment=segment_key) if store else DatasetProfile(schema)
    profile.track(segmented_data)
    segments[segment_key] = profile


def _process_simple_partition(
    partition_id: str,
    schema: DatasetSchema,
    segments: Dict[Segment, Any],
    columns: List[str],
    pandas: Optional[pd.DataFrame] = None,
    row: Optional[Dict[str, Any]] = None,
    profile_cache: Optional[ProfileStore] = None,
):
    if pandas is not None:
        # simple means we can segment on column values
        grouped_data = pandas.groupby(columns)
        for group in grouped_data.groups.keys():
            pandas_segment = grouped_data.get_group(group)
            segment_key = Segment(tuple(str(k) for k in group), partition_id)
            _process_segment(pandas_segment, segment_key, segments, schema, profile_cache)
    elif row:
        # TODO: consider if we need to combine with the column names
        segment_key = Segment(tuple(str(row[element]) for element in columns), partition_id)
        _process_segment(row, segment_key, segments, schema, profile_cache)


def _log_segment(
    partition: SegmentationPartition,
    schema: DatasetSchema,
    obj: Any = None,
    pandas: Optional[pd.DataFrame] = None,
    row: Optional[Dict[str, Any]] = None,
    store: Optional[ProfileStore] = None,
) -> Dict[Segment, Any]:
    segments = {}
    if partition.simple:
        pandas, row = _pandas_or_dict(obj, pandas, row)
        columns = partition.mapper.col_names if partition.mapper else None
        if columns:
            _process_simple_partition(partition.id, schema, segments, columns, pandas, row, store)

    else:
        raise NotImplementedError("Implement non-simple path of logging segments!")
        # if partition.filter and partition.filter.filter_function:
        # filtered_pandas, filtered_row = partition.filter.filter_function(obj, pandas, row)
    return segments


def _log_partitions(
    schema: DatasetSchema,
    obj: Any = None,
    pandas: Optional[pd.DataFrame] = None,
    row: Optional[Dict[str, Any]] = None,
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
            logger.debug(f"{partition_name}: defines filter ({segment_partition.filter.__name__})")
        if segment_partition.mapper:
            logger.debug(
                f"{partition_name}: defines mapper on colums ({segment_partition.mapper.col_names}) and id ({segment_partition.mapper.id})"
            )
        partition_segments = _log_segment(segment_partition, schema, obj, pandas, row)
        segmented_profiles[segment_partition.id] = partition_segments
        segment_partitions.append(segment_partition)
        logger.debug(f"Done profiling for partition with name({partition_name})")
    return SegmentedResultSet(segments=segmented_profiles, partitions=segment_partitions)
