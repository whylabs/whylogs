from datetime import datetime, timezone
from functools import partial, reduce
from logging import getLogger
from typing import Dict, Iterable, Optional, Tuple

import whylogs as why
from whylogs.api.usage_stats import emit_usage
from whylogs.core import DatasetSchema
from whylogs.core.metrics.metrics import conf
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

COL_NAME_FIELD = "col_name"
COL_PROFILE_FIELD = "col_profile"


def whylogs_pandas_map_profiler(
    pdf_iterator: Iterable[pd.DataFrame], schema: Optional[DatasetSchema] = None
) -> Iterable[pd.DataFrame]:
    for input_df in pdf_iterator:
        res = why.log(input_df, schema=schema)
        res_df = pd.DataFrame(columns=[COL_NAME_FIELD, COL_PROFILE_FIELD])
        for col_name, col_profile in res.view().get_columns().items():
            d = {
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


def collect_column_profile_views(
    input_df: SparkDataFrame, schema: Optional[DatasetSchema] = None
) -> Dict[str, ColumnProfileView]:
    if SparkDataFrame is None:
        logger.warning("Unable to load pyspark; install pyspark to get whylogs profiling support in spark environment.")
    # check for vector type columns which break mapInPandas
    vector_columns = [
        col_name for col_name in input_df.schema.names if isinstance(input_df.schema[col_name].dataType, VectorUDT)
    ]
    input_df_arrays = input_df
    for col_name in vector_columns:
        input_df_arrays = input_df_arrays.withColumn(col_name, vector_to_array(input_df_arrays[col_name]))
    cp = f"{COL_NAME_FIELD} string, {COL_PROFILE_FIELD} binary"
    whylogs_pandas_map_profiler_with_schema = partial(whylogs_pandas_map_profiler, schema=schema)

    profile_bytes_df = input_df_arrays.mapInPandas(whylogs_pandas_map_profiler_with_schema, schema=cp)  # type: ignore
    column_profiles = profile_bytes_df.groupby(COL_NAME_FIELD).applyInPandas(  # linebreak
        column_profile_bytes_aggregator, schema=cp
    )
    collected_profile_views = {
        row.col_name: ColumnProfileView.from_bytes(row.col_profile) for row in column_profiles.collect()
    }
    return collected_profile_views


def _collect_column_profile_views_batched(
    batch_size: int, input_df: SparkDataFrame, schema: Optional[DatasetSchema] = None
) -> Dict[str, ColumnProfileView]:
    if batch_size <= 0:
        logger.warning(
            f"Batched pyspark column processing was called with batch_size of {batch_size}, falling back to non-batched profiling."
        )
        return collect_column_profile_views(input_df=input_df, schema=schema)

    columns = input_df.columns
    num_batches = len(columns) // batch_size  # Total number of batches

    # Initialize an empty dataset_profile
    combined_column_views = dict()

    # process the column profiles in batches
    for batch_num in range(num_batches):
        start_idx = batch_num * batch_size
        end_idx = (batch_num + 1) * batch_size
        selected_columns = columns[start_idx:end_idx]
        column_partitioned_batch_df = input_df.select(*selected_columns)
        column_views_dict = collect_column_profile_views(input_df=column_partitioned_batch_df, schema=schema)
        combined_column_views.update(column_views_dict)

    # Handle the last batch (if any) that may have fewer columns
    last_batch_columns = columns[num_batches * batch_size :]
    if last_batch_columns:
        column_partitioned_batch_df = input_df.select(*last_batch_columns)
        # Process the last batch DataFrame
        column_views_dict = collect_column_profile_views(input_df=column_partitioned_batch_df, schema=schema)
        combined_column_views.update(column_views_dict)

    return combined_column_views


def _get_column_batch_size() -> Optional[int]:
    if conf.column_batch_size:
        return conf.column_batch_size
    else:
        return None


def collect_dataset_profile_view(
    input_df: SparkDataFrame,
    dataset_timestamp: Optional[datetime] = None,
    creation_timestamp: Optional[datetime] = None,
    schema: Optional[DatasetSchema] = None,
) -> DatasetProfileView:
    now = datetime.now(timezone.utc)

    _dataset_timestamp = dataset_timestamp or now
    _creation_timestamp = creation_timestamp or now
    if schema and schema.segments:
        raise ValueError(
            "collect_dataset_profile_view returns a single DatasetProfileView but segmentation was defined, "
            f": {schema.segments} may return multiple segmented profiles. Please use `collect_segmented_results` if you want to profile"
            " in spark with segments defined."
        )
    batch_size = _get_column_batch_size()
    if batch_size is not None:
        column_views_dict = _collect_column_profile_views_batched(batch_size, input_df=input_df, schema=schema)
    else:
        column_views_dict = collect_column_profile_views(input_df=input_df, schema=schema)

    profile_view = DatasetProfileView(
        columns=column_views_dict, dataset_timestamp=_dataset_timestamp, creation_timestamp=_creation_timestamp
    )

    return profile_view
