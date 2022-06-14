from decimal import Decimal
from logging import getLogger
from typing import Dict

import pytest
from pyspark.sql import SparkSession

from whylogs.api.pyspark.experimental import (
    collect_column_profile_views,
    collect_dataset_profile_view,
    column_profile_bytes_aggregator,
    whylogs_pandas_map_profiler,
)
from whylogs.core import ColumnProfileView
from whylogs.core.stubs import pd as pd
from whylogs.core.view.dataset_profile_view import DatasetProfileView

TEST_LOGGER = getLogger(__name__)


class TestPySpark(object):
    @classmethod
    def setup_class(cls):
        cls.spark = SparkSession.builder.master("local[1]").getOrCreate()

    @classmethod
    def teardown_class(cls):
        cls.spark.stop()

    @pytest.fixture()
    def test_columns(self):
        return ["0", "1", "2", "3"]

    @pytest.fixture()
    def input_df(self, test_columns):
        input_df = self.spark.createDataFrame(
            data=[
                [0.0, 1.0, 2.0, 3.0],
                [0.1, 1.1, 2.1, 3.1],
                [0.2, 1.3, 2.3, 3.3],
            ],
            schema=test_columns,
        )
        return input_df

    def test_profile_mapper_function(self, input_df):
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

    def test_collect_column_profiles(self, input_df, test_columns):
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

    def test_collect_dataset_profile_view(self, input_df):
        profile_view = collect_dataset_profile_view(input_df=input_df)

        assert isinstance(profile_view, DatasetProfileView)
        assert len(profile_view.get_columns()) > 0
        assert profile_view.get_column("0").get_metric("counts").n.value == 3
        assert profile_view.get_column("0").get_metric("distribution").max == 0.2
        assert profile_view.get_column("0").get_metric("distribution").min == 0.0

    def test_decimals_are_fractional(self):
        decimal_data = [
            [Decimal(8.4), Decimal(13.8)],
            [Decimal(2.9), Decimal(7.2)],
        ]
        pandas_decimals = pd.DataFrame({"wine": [Decimal(8.4), Decimal(13.8)], "beer": [Decimal(2.9), Decimal(7.2)]})
        decimals_df = self.spark.createDataFrame(decimal_data, schema=["wine", "beer"])
        dataset_profile_view = collect_dataset_profile_view(decimals_df)
        type_counts = dataset_profile_view.get_column("wine").get_metric("types")
        assert type_counts.integral.value == 0
        assert type_counts.fractional.value == 2

        # check that pandas dataframes without spark also behave similarly
        import whylogs as why

        pandas_decimals_profile_view = why.log(pandas_decimals).view()
        pandas_type_counts = pandas_decimals_profile_view.get_column("wine").get_metric("types")
        assert pandas_type_counts.integral.value == 0
        assert pandas_type_counts.fractional.value == 2

        wine_column = dataset_profile_view.get_column("wine")

        distribution_metric = wine_column.get_metric("distribution")
        if distribution_metric is None:
            TEST_LOGGER.info(
                f"Could not find distribution metric on decimal series: metrics are {wine_column.get_metric_names()}"
            )
        assert distribution_metric is not None
