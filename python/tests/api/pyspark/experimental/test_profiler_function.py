from decimal import Decimal
from logging import getLogger
from typing import Dict

import pytest

import whylogs as why
from whylogs.api.logger.result_set import SegmentedResultSet
from whylogs.api.pyspark.experimental import (
    collect_column_profile_views,
    collect_dataset_profile_view,
    column_profile_bytes_aggregator,
    whylogs_pandas_map_profiler,
    collect_segmented_results,
)
from whylogs.core import ColumnProfileView, DatasetProfile, DatasetSchema, Resolver
from whylogs.core.metrics import StandardMetric
from whylogs.core.segmentation_partition import SegmentFilter, segment_on_column
from whylogs.core.stubs import pd as pd
from whylogs.core.view.dataset_profile_view import DatasetProfileView
from whylogs.core.view.segmented_dataset_profile_view import SegmentedDatasetProfileView

TEST_LOGGER = getLogger(__name__)

def _assert_segmented_result_sets_are_equal(results_a: SegmentedResultSet, results_b: SegmentedResultSet) -> None:
    assert type(results_a) == type(results_b)

    profiles_a = results_a.get_writables()
    profiles_b = results_b.get_writables()
    assert len(profiles_a) == len(profiles_b)
    for profile_a in profiles_a:
        assert isinstance(profile_a, SegmentedDatasetProfileView)
        segment = profile_a.segment
        segmented_view_a = profile_a.profile_view
        segmented_view_b = results_b.profile(segment)
        if isinstance(segmented_view_b, DatasetProfile):
            segmented_view_b = segmented_view_b.view()
        assert isinstance(segmented_view_b, DatasetProfileView)
        if segmented_view_a is None and segmented_view_b is None:
            return
        if segmented_view_a is None or segmented_view_b is None:
            assert segmented_view_a == segmented_view_b

        columns_in_a = segmented_view_a.get_columns()
        columns_in_b = segmented_view_b.get_columns()
        if not columns_in_b:
            assert columns_in_a == columns_in_b

        assert columns_in_a.keys() == columns_in_b.keys()

        for col_name in columns_in_a:
            assert col_name in columns_in_b
            assert (col_name, columns_in_a[col_name].to_protobuf()) == (col_name, columns_in_b[col_name].to_protobuf())

        assert segmented_view_a.creation_timestamp.timestamp() == pytest.approx(segmented_view_b.creation_timestamp.timestamp())
        assert segmented_view_a.dataset_timestamp.timestamp() == pytest.approx(segmented_view_b.dataset_timestamp.timestamp())

class TestPySpark(object):
    @pytest.fixture()
    def test_columns(self):
        return ["0", "1", "2", "3"]

    @pytest.fixture()
    def segment_columns(self):
        return ["A", "B", "C", "D"]

    @pytest.fixture()
    def input_df(self, test_columns, spark_session):
        input_df = spark_session.createDataFrame(
            data=[
                [0.0, 1.0, 2.0, 3.0],
                [0.1, 1.1, 2.1, 3.1],
                [0.2, 1.3, 2.3, 3.3],
            ],
            schema=test_columns,
        )
        return input_df

    @pytest.fixture()
    def segment_df(self, segment_columns, spark_session):
        segment_df = spark_session.createDataFrame(
            data=[
                [0, 'test', 2.0, 3.0],
                [1, 'test', 2.1, 3.1],
                [0, 'eval', 2.3, 3.3],
                [1, 'reserved', 2.4, 3.4],
            ],
            schema=segment_columns,
        )
        return segment_df

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
        assert profile_bytes_df.count() > 0
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

    def test_collect_dataset_profile_view_with_schema(self, input_df):
        class TestResolver(Resolver):
            def resolve(self, name, why_type, column_schema):
                metric_map = {"0": [StandardMetric.counts], "1": [], "2": [], "3": []}
                return {metric.name: metric.zero(column_schema.cfg) for metric in metric_map[name]}

        schema = DatasetSchema(resolvers=TestResolver())
        profile_view = collect_dataset_profile_view(input_df=input_df, schema=schema)

        assert isinstance(profile_view, DatasetProfileView)
        assert len(profile_view.get_columns()) > 0
        assert profile_view.get_column("0").get_metric_names() == ["counts"]
        assert profile_view.get_column("0").get_metric("counts").n.value == 3
        assert profile_view.get_column("1").get_metric_names() == []
        assert profile_view.get_column("2").get_metric_names() == []
        assert profile_view.get_column("3").get_metric_names() == []

    def test_profile_segments_same_in_pyspark_and_local_python_log(self, segment_df):
        segment_column = "B"
 
        d = {
            "A" : [0,1,0,1],
            "B" : ['test','test','eval','reserved'],
            "C" : [2.0,2.1,2.3,2.4],
            "D" : [3.0,3.1,3.3,3.4],
        }

        df = pd.DataFrame(data=d)
        test_segments = segment_on_column(segment_column)
        test_segments[segment_column].filter = SegmentFilter(filter_function=lambda df: df.C > 2.0)
        segmented_schema = DatasetSchema(segments=test_segments)

        local_results = why.log(df, schema=segmented_schema)
        pyspark_results = collect_segmented_results(input_df=segment_df, schema=segmented_schema)

        assert pyspark_results is not None
        assert local_results is not None
        assert pyspark_results.count == local_results.count
        _assert_segmented_result_sets_are_equal(pyspark_results, local_results)



    def test_collect_dataset_profile_view(self, input_df):
        profile_view = collect_dataset_profile_view(input_df=input_df)

        assert isinstance(profile_view, DatasetProfileView)
        assert len(profile_view.get_columns()) > 0
        assert profile_view.get_column("0").get_metric("counts").n.value == 3
        assert profile_view.get_column("0").get_metric("distribution").max == 0.2
        assert profile_view.get_column("0").get_metric("distribution").min == 0.0

    def test_decimals_are_fractional(self, spark_session):
        decimal_data = [
            [Decimal(8.4), Decimal(13.8)],
            [Decimal(2.9), Decimal(7.2)],
        ]
        pandas_decimals = pd.DataFrame({"wine": [Decimal(8.4), Decimal(13.8)], "beer": [Decimal(2.9), Decimal(7.2)]})
        decimals_df = spark_session.createDataFrame(decimal_data, schema=["wine", "beer"])
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
