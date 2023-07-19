from functools import partial
from logging import getLogger
from typing import Optional

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
    logger.error("No pyspark available")
    SparkDataFrame = None  # type: ignore


def _profile_partition(partition, dataset_timestamp: Optional[int] = None, schema: Optional[DatasetSchema] = None):
    pandas_df = pd.DataFrame(list(partition))
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
            status = result_writer.write()
            statuses.append(status)
        yield statuses

    return distributed_upload


def profile_and_upload_to_whylabs(
    input_df: SparkDataFrame, dataset_timestamp: Optional[int] = None, schema: Optional[DatasetSchema] = None, max_partitions: int = 256
):
    profiler_with_schema = partial(_profile_partition, schema=schema, dataset_timestamp=dataset_timestamp)
    parts = input_df.rdd.getNumPartitions()
    diagnostic_logger.info(f"Number of partitions was {parts}")
    if parts > max_partitions:
        repartitioned_input_df = input_df.repartition(max_partitions)
    else:
        repartitioned_input_df = input_df
    profiles_rdd = repartitioned_input_df.rdd.mapPartitions(profiler_with_schema)
    broadcast_params = _set_broadcast_params(input_df.sparkSession)
    distributed_upload = _get_distributed_upload_method(broadcast_params)
    statuses = profiles_rdd.mapPartitions(distributed_upload)
    return statuses.collect()
