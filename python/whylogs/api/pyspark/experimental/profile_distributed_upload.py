from functools import partial
from logging import getLogger
from typing import List, Optional, Tuple

import whylogs as why
from whylogs.api.logger.result_set import ResultSetWriter
from whylogs.api.usage_stats import emit_usage
from whylogs.api.writer.whylabs import WhyLabsWriter
from whylogs.core import DatasetSchema
from whylogs.core.stubs import pd

diagnostic_logger = getLogger(__name__)
emit_usage("distributed_pyspark")


try:  # type: ignore
    from pyspark.sql import DataFrame as SparkDataFrame  # types: ignore
except ImportError:  # noqa
    diagnostic_logger.error("No pyspark available")
    SparkDataFrame = None  # type: ignore


def _profile_partition(
    partition, partition_schema, dataset_timestamp: Optional[int] = None, schema: Optional[DatasetSchema] = None
):
    pandas_df = pd.DataFrame(list(partition), columns=partition_schema.fieldNames())
    results = why.log(pandas_df, schema=schema)
    if dataset_timestamp:
        results.set_dataset_timestamp(dataset_timestamp)
    yield results


def _set_broadcast_params(spark_session):
    import os

    api_key = os.environ["WHYLABS_API_KEY"]
    dataset_id = os.environ["WHYLABS_DEFAULT_DATASET_ID"]
    org_id = os.environ["WHYLABS_DEFAULT_ORG_ID"]

    whylabs_writer_params = {"api_key": api_key, "dataset_id": dataset_id, "org_id": org_id}
    broadcast_params = spark_session.sparkContext.broadcast(whylabs_writer_params)
    return broadcast_params


def _get_distributed_upload_method(broadcast_params):
    def distributed_upload(profiles_partition):
        whylabs_writer_params = broadcast_params.value
        writer = WhyLabsWriter(**whylabs_writer_params)
        statuses = []
        for results in profiles_partition:
            result_writer = ResultSetWriter(results=results, writer=writer)
            status: List[Tuple[bool, str]] = result_writer.write()
            statuses += status
        yield statuses

    return distributed_upload


def _profiles_rdd(
    input_df: SparkDataFrame, dataset_timestamp: Optional[int] = None, schema: Optional[DatasetSchema] = None
):
    profiler_with_schema = partial(_profile_partition, schema=schema, dataset_timestamp=dataset_timestamp)
    profiles_rdd = input_df.rdd.mapPartitions(profiler_with_schema)
    return profiles_rdd


def _get_distributed_upload_function(spark_session):
    broadcast_params = _set_broadcast_params(spark_session)
    return _get_distributed_upload_method(broadcast_params)


def _profile_and_upload_to_whylabs(
    input_df: SparkDataFrame,
    dataset_timestamp: Optional[int] = None,
    schema: Optional[DatasetSchema] = None,
    max_partitions: int = 1024,
):
    profiler_with_schema = partial(
        _profile_partition, partition_schema=input_df.schema, schema=schema, dataset_timestamp=dataset_timestamp
    )
    num_partitions = input_df.rdd.getNumPartitions()
    diagnostic_logger.info(f"Number of partitions is {num_partitions} and max_partitions is set to {max_partitions}")
    profiles_rdd = input_df.rdd.mapPartitions(profiler_with_schema)

    if num_partitions > max_partitions:
        raise ValueError(
            f"profile_and_upload_to_whylabs requires less than {max_partitions} and this dataframe "
            f"has {num_partitions}, so that you are not uploading too many files at once. To work around "
            "this you can coalesce the input dataframe or try collect_dataset_profile_view()."
        )

    spark_session = input_df.sparkSession
    broadcast_params = _set_broadcast_params(spark_session)
    distributed_upload = _get_distributed_upload_method(broadcast_params)
    statuses = profiles_rdd.mapPartitions(distributed_upload)
    return statuses


def profile_distributed_upload(
    input_df: SparkDataFrame,
    dataset_timestamp: Optional[int] = None,
    schema: Optional[DatasetSchema] = None,
    max_partitions: int = 1024,
):
    statuses = _profile_and_upload_to_whylabs(
        input_df=input_df, dataset_timestamp=dataset_timestamp, schema=schema, max_partitions=max_partitions
    )
    local_statuses = statuses.collect()
    return local_statuses
