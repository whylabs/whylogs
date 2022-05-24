import unittest
from logging import getLogger
from typing import Dict

from pyspark.sql import SparkSession

from whylogs.api.pyspark.experimental import (
    collect_column_profile_views,
    column_profile_bytes_aggregator,
    whylogs_pandas_map_profiler,
)
from whylogs.core import ColumnProfileView

TEST_LOGGER = getLogger(__name__)


class PysparkTest(unittest.TestCase):
    """Set-up a test SparkSession"""

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


class WhysparkTests(PysparkTest):
    def test_profile_mapper_function(self):
        input_df = self.spark.createDataFrame(
            data=[
                [0.0, 1.0, 2.0, 3.0],
                [0.1, 1.1, 2.1, 3.1],
                [0.2, 1.3, 2.3, 3.3],
            ],
            schema=["0", "1", "2", "3"],
        )

        # TODO: consider a constant for this, or better encapsulation.
        cp = "col_name string, col_profile binary"

        # test pyspark whylogs profiling
        profile_bytes_df = input_df.mapInPandas(whylogs_pandas_map_profiler, schema=cp)
        column_profiles = profile_bytes_df.groupby("col_name").applyInPandas(column_profile_bytes_aggregator, schema=cp)
        collected_profiles = map(
            lambda row: (row.col_name, ColumnProfileView.from_bytes(row.col_profile).to_summary_dict()),
            column_profiles.collect(),
        )
        assert profile_bytes_df.rdd.getNumPartitions() > 0
        assert profile_bytes_df.count() == 4
        local_column_profiles = list(collected_profiles)
        TEST_LOGGER.info(local_column_profiles)
        assert local_column_profiles is not None

    def test_collect_column_profiles(self):
        test_columns = ["0", "1", "2", "3"]
        input_df = self.spark.createDataFrame(
            data=[
                [0.0, 1.0, 2.0, 3.0],
                [0.1, 1.1, 2.1, 3.1],
                [0.2, 1.3, 2.3, 3.2],
            ],
            schema=test_columns,
        )

        # test pyspark whylogs profiling
        local_column_profiles = collect_column_profile_views(input_df)
        TEST_LOGGER.info(local_column_profiles)
        assert isinstance(local_column_profiles, Dict)
        TEST_LOGGER.info(local_column_profiles.keys())
        assert set(test_columns) == set(local_column_profiles.keys())
        # 'distribution/stddev': 0.1, 'distribution/n': 3, 'distribution/max': 0.2, 'distribution/min': 0.0,
        distribution_metric = local_column_profiles["0"].get_metric("distribution")
        assert distribution_metric.n == 3
        assert distribution_metric.stddev == 0.1
        assert distribution_metric.max == 0.2
        assert distribution_metric.min == 0.0
