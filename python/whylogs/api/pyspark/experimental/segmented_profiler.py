import dataclasses
import json
from datetime import datetime, timezone
from functools import partial, reduce
from logging import getLogger
from typing import Dict, Iterable, List, Optional, Tuple

import whylogs as why
from whylogs.api.logger.result_set import ResultSet, SegmentedResultSet
from whylogs.api.pyspark.experimental.profiler import COL_NAME_FIELD, COL_PROFILE_FIELD
from whylogs.api.usage_stats import emit_usage
from whylogs.core import DatasetSchema
from whylogs.core.segment import Segment
from whylogs.core.segmentation_partition import SegmentationPartition
from whylogs.core.stubs import pd
from whylogs.core.view.column_profile_view import ColumnProfileView
from whylogs.core.view.dataset_profile_view import DatasetProfileView

logger = getLogger(__name__)
emit_usage("pyspark")

try:  # type: ignore
    from pyspark.ml.functions import vector_to_array
    from pyspark.ml.linalg import VectorUDT
    from pyspark.sql import DataFrame as SparkDataFrame  # types: ignore
except ImportError:  # noqa
    logger.error("No pyspark available")
    SparkDataFrame = None  # type: ignore

SEGMENT_KEY_FIELD = "segment_key"


def _segment_to_string(segment: Segment) -> str:
    return json.dumps(dataclasses.asdict(segment))


def _string_to_segment(segment_key: str) -> Optional[Segment]:
    if len(segment_key) == 0:
        return None
    segment_params = json.loads(segment_key)
    if "key" not in segment_params or "parent_id" not in segment_params:
        raise ValueError("Segment key is missing required key values or parent_id: {segment_params}")
    tuple_key = tuple(segment_params["key"])
    parent_id = segment_params["parent_id"]
    return Segment(key=tuple_key, parent_id=parent_id)


def whylogs_pandas_segmented_profiler(
    pdf_iterator: Iterable[pd.DataFrame], schema: Optional[DatasetSchema] = None
) -> Iterable[pd.DataFrame]:
    if schema is None or not schema.segments:
        raise ValueError(
            "Cannot profile segments without segmentation defined in the specified DatasetSchema: no segments found."
        )
    for input_df in pdf_iterator:
        # TODO: optimize this so we split the dataframe by segment first rather than split in the pandas dataframe
        segmented_results = why.log(input_df, schema=schema)
        res_df = pd.DataFrame(columns=[SEGMENT_KEY_FIELD, COL_NAME_FIELD, COL_PROFILE_FIELD])
        for segmented_key in segmented_results.segments():
            spark_segment_key = _segment_to_string(segmented_key)
            for col_name, col_profile in segmented_results.view(segmented_key).get_columns().items():
                d = {
                    SEGMENT_KEY_FIELD: [spark_segment_key],
                    COL_NAME_FIELD: [col_name],
                    COL_PROFILE_FIELD: [col_profile.to_protobuf().SerializeToString()],
                }
                df_temp = pd.DataFrame(data=d)
                res_df = pd.concat([res_df, df_temp])
        yield res_df


def column_profile_bytes_aggregator(group_by_cols: Tuple[str], profiles_df: pd.DataFrame) -> pd.DataFrame:
    if len(profiles_df) == 0:
        return pd.DataFrame([group_by_cols + (bytearray(),)])

    merged_profile: ColumnProfileView = reduce(
        lambda acc, x: acc.merge(x), profiles_df[COL_PROFILE_FIELD].apply(ColumnProfileView.from_bytes)
    )
    return pd.DataFrame([group_by_cols + (merged_profile.serialize(),)])


def collect_segmented_column_profile_views(
    input_df: SparkDataFrame, schema: Optional[DatasetSchema] = None
) -> Dict[Segment, Dict[str, ColumnProfileView]]:
    if SparkDataFrame is None:
        logger.warning("Unable to load pyspark; install pyspark to get whylogs profiling support in spark environment.")
    vector_columns = [
        col_name for col_name in input_df.schema.names if isinstance(input_df.schema[col_name].dataType, VectorUDT)
    ]
    input_df_arrays = input_df
    for col_name in vector_columns:
        input_df_arrays = input_df_arrays.withColumn(col_name, vector_to_array(input_df_arrays[col_name]))

    cp = f"{SEGMENT_KEY_FIELD} string, {COL_NAME_FIELD} string, {COL_PROFILE_FIELD} binary"
    whylogs_pandas_map_profiler_with_schema = partial(whylogs_pandas_segmented_profiler, schema=schema)
    segmented_profile_bytes_df = input_df_arrays.mapInPandas(whylogs_pandas_map_profiler_with_schema, schema=cp)  # type: ignore
    # aggregate by segment key first, then by column
    segment_column_profiles = segmented_profile_bytes_df.groupby(
        SEGMENT_KEY_FIELD, COL_NAME_FIELD
    ).applyInPandas(  # linebreak
        column_profile_bytes_aggregator, schema=cp
    )

    collected_segment_column_profile_views: Dict[Tuple[Segment, str], ColumnProfileView] = {
        (_string_to_segment(row.segment_key), row.col_name): ColumnProfileView.from_bytes(row.col_profile)
        for row in segment_column_profiles.collect()
    }

    segmented_column_profile_views: Dict[Segment, Dict[str, ColumnProfileView]] = dict()

    for key_tuple in collected_segment_column_profile_views:
        segment_key = key_tuple[0]
        column_name = key_tuple[1]
        if segment_key not in segmented_column_profile_views:
            segmented_column_profile_views[segment_key] = dict()
        segmented_column_profile_views[segment_key][column_name] = collected_segment_column_profile_views[key_tuple]

    return segmented_column_profile_views


def _lookup_segment_partition_by_id(schema: DatasetSchema, partition_id: str) -> Optional[SegmentationPartition]:
    for partition_name in schema.segments:
        partition = schema.segments[partition_name]
        if partition_id == partition.id:
            return partition
    logger.warning(f"Skipping segment: could not find partition with id {partition_id} in: {schema.segments}")
    return None


def collect_segmented_results(
    input_df: SparkDataFrame,
    schema: DatasetSchema,
    dataset_timestamp: Optional[datetime] = None,
    creation_timestamp: Optional[datetime] = None,
) -> ResultSet:
    now = datetime.now(timezone.utc)

    _dataset_timestamp = dataset_timestamp or now
    _creation_timestamp = creation_timestamp or now
    if not schema.segments:
        raise ValueError("Cannot collect segmented results without segments defined in the passed in DatasetSchema")

    logger.info(f"Processing segmented profiling in spark with {schema.segments}")
    segment_column_views_dict = collect_segmented_column_profile_views(input_df=input_df, schema=schema)
    segments: Dict[str, Dict[Segment, DatasetProfileView]] = dict()
    partitions: List[SegmentationPartition] = list(schema.segments.values())

    for segment in segment_column_views_dict:
        partition_id = segment.parent_id
        # check that the segment's parent id has a matching partition
        partition = _lookup_segment_partition_by_id(schema, partition_id)
        if partition is None:
            logger.error(
                f"Skipping segment: could not collect profiles for segment {segment} because the schema has no matching partition ids: {schema.segments.keys()}"
            )
            continue
        if partition_id not in segments:
            segments[partition_id] = dict()
        if segment not in segments[partition_id]:
            column_views_dict = segment_column_views_dict[segment]
            segments[partition_id][segment] = DatasetProfileView(
                columns=column_views_dict, dataset_timestamp=_dataset_timestamp, creation_timestamp=_creation_timestamp
            )
        else:
            raise ValueError(f"Collision when collecting profiles for segment {segment}!")

    return SegmentedResultSet(segments=segments, partitions=partitions)
