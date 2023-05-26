import cProfile
import pstats
import random
from dataclasses import dataclass, field
from io import StringIO
from logging import getLogger
from typing import Any, Dict

import numpy as np
import pandas as pd
import pytest
import whylogs_sketching as ds  # type: ignore

import whylogs
from whylogs.core import ColumnProfile, ColumnSchema
from whylogs.core.dataset_profile import DatasetProfile
from whylogs.core.metrics.metrics import MetricConfig
from whylogs.core.resolvers import (
    HistogramCountingTrackingResolver,
    LimitedTrackingResolver,
    Resolver,
    StandardResolver,
)

TEST_LOGGER = getLogger(__name__)

_TEST_RESOLVERS = [HistogramCountingTrackingResolver(), LimitedTrackingResolver(), StandardResolver()]


# TODO: this is from the baseline benchmark, but its not integrated with our metrics
@dataclass
class CustomHistogramMetric:
    histogram: ds.kll_floats_sketch = field(
        default=ds.kll_floats_sketch(MetricConfig().kll_k),
    )

    def track(self, val: Any) -> "CustomHistogramMetric":
        if pd.isna(val):
            return self
        self.histogram.update(val)
        return self


@pytest.mark.load
@pytest.mark.parametrize("test_resolver", _TEST_RESOLVERS)
def test_track_column_benchmark(test_resolver: Resolver) -> None:
    dataframe_shapes = [(3400000, 43)]  # 10x less rows to estimate
    TEST_LOGGER.info(f"Running test_track_column_benchmark with {len(dataframe_shapes)} different test dataframes")
    for num_rows, num_columns in dataframe_shapes:
        TEST_LOGGER.info(f"shape of test dataframe is ({num_rows},{num_columns})...")

        profiler = cProfile.Profile()
        string_output_stream = StringIO()
        profiler.enable()
        for column_index in range(num_columns):
            column_name = str(column_index)
            col_df = pd.DataFrame(np.random.random(size=(num_rows, 1)), columns=[column_name])
            col_prof = ColumnProfile(
                name="perf_test", schema=ColumnSchema(float, resolver=test_resolver), cache_size=1024
            )
            if column_index == 0:
                TEST_LOGGER.info(
                    f"using the following trackers {[metric for metric in col_prof._metrics]} and {col_prof._schema.resolver}"
                )
            col_prof.track_column(col_df[column_name])
        profiler.disable()
        stats = pstats.Stats(profiler, stream=string_output_stream).sort_stats("cumulative")
        stats.print_stats(10)
        TEST_LOGGER.info(
            f"track_column stats using the following trackers {[metric_name for metric_name in col_prof._metrics]} "
            f"are\n{string_output_stream.getvalue()}"
        )
        assert col_prof.view().get_metric("distribution") is not None


@pytest.mark.load
def test_track_dataset_benchmark() -> None:
    dataframe_shapes = [(120000, 34), (3400000, 43)]  # 10x less rows to estimate
    TEST_LOGGER.info(f"Running dataset_profile.track with {len(dataframe_shapes)} shapes")
    for num_rows, num_columns in dataframe_shapes:
        TEST_LOGGER.info(f"shape of test dataframe is ({num_rows},{num_columns})...")

        profiler = cProfile.Profile()
        string_output_stream = StringIO()
        full_df = pd.DataFrame(
            np.random.random(size=(num_rows, num_columns)), columns=[str(i) for i in range(num_columns)]
        )
        dataset_profile = DatasetProfile()
        profiler.enable()
        dataset_profile.track(full_df)
        profiler.disable()
        stats = pstats.Stats(profiler, stream=string_output_stream).sort_stats("cumulative")
        stats.print_stats(20)
        test_column_name = next(iter(dataset_profile._columns))
        TEST_LOGGER.info(
            f"dataset_profile.track stats on ({num_rows},{num_columns}) using "
            f"{[metric_name for metric_name in dataset_profile._columns[test_column_name]._metrics]} are"
            f"\n{string_output_stream.getvalue()}"
        )
        for column_name in dataset_profile._columns:
            assert dataset_profile._columns[column_name].view().get_metric("distribution") is not None


@pytest.mark.load
def test_track_baseline_benchmark() -> None:
    # dataframe_shapes = [(1200000, 34), (34000000, 43)]  # full baseline
    dataframe_shapes = [(120000, 34)]  # 10x less rows, first test only to estimate in reasonable time
    TEST_LOGGER.info(f"Running custom_metric.track with {len(dataframe_shapes)} different test dataframes")
    for num_rows, num_columns in dataframe_shapes:
        TEST_LOGGER.info(f"shape of test dataframe is ({num_rows},{num_columns})...")

        profiler = cProfile.Profile()
        string_output_stream = StringIO()
        baseline_metric = CustomHistogramMetric()
        profiler.enable()
        for column_index in range(num_columns):
            column_name = str(column_index)
            baseline_metric = CustomHistogramMetric()
            col_df = pd.DataFrame(np.random.random(size=(num_rows, 1)), columns=[column_name])
            if column_index == 0:
                TEST_LOGGER.info(f"using the following trackers {baseline_metric}")
            for value in col_df[column_name]:
                baseline_metric.track(value)
            TEST_LOGGER.info(f"\tcolumn: {column_index}")

        profiler.disable()
        stats = pstats.Stats(profiler, stream=string_output_stream).sort_stats("cumulative")
        stats.print_stats(20)
        TEST_LOGGER.info(
            f"stats for baseline_benchmark (custom_metric.track) on df({num_rows},{num_columns}) "
            f"are\n{string_output_stream.getvalue()}"
        )


def _gen_test_row_message(i: int) -> Dict[str, Any]:
    additional_fields = 40
    test_message = {
        "jobtitle": "software engineer",
        "employer": "whylabs",
        "city": "seattle",
        "state": "washington",
        "country": "united states",
        "date": "2022-11-02",
        "optional_features": i % 100,
        "nan_feature": float("nan") if i % 13 == 0 else float(i) / 3.3,
        "None_feature": None if i % 3 == 0 else {"a": 1},
        "debug": True,
    }
    for i in range(additional_fields):
        test_message[f"field_{i}"] = random.random()
    return test_message


def _gen_test_df() -> pd.DataFrame:
    num_rows = 1
    num_columns = 50
    full_df = pd.DataFrame(np.random.random(size=(num_rows, num_columns)), columns=[str(i) for i in range(num_columns)])
    return full_df


@pytest.mark.load
def test_rolling_logger_latency_row_benchmark() -> None:
    number_of_iterations = 1000
    TEST_LOGGER.info(f"Running latency test with {number_of_iterations} iterations")
    test_log = whylogs.logger(mode="rolling", interval=60, when="S", fork=True)
    test_log.append_writer("local")

    profiler = cProfile.Profile()
    string_output_stream = StringIO()
    profiler.enable()

    for i in range(number_of_iterations):
        msg = _gen_test_row_message(i)
        test_log.log(msg)

    test_log.close()
    profiler.disable()
    stats = pstats.Stats(profiler, stream=string_output_stream).sort_stats("cumulative")
    stats.print_stats(20)
    TEST_LOGGER.info(f"stats for rolling latency benchmark are\n{string_output_stream.getvalue()}")


@pytest.mark.load
@pytest.mark.parametrize("mode", ["pandas_to_row", "row", "pandas"])
def test_rolling_logger_latency_benchmark(mode) -> None:
    number_of_iterations = 1000
    TEST_LOGGER.info(f"Running latency test with {number_of_iterations} iterations")
    test_log = whylogs.logger(mode="rolling", interval=60, when="S", fork=True)
    test_log.append_writer("local")

    profiler = cProfile.Profile()
    string_output_stream = StringIO()
    profiler.enable()

    for i in range(number_of_iterations):
        data = None
        if mode == "pandas":
            data = _gen_test_df()
        elif mode == "row":
            data = _gen_test_row_message(i)
        elif mode == "pandas_to_row":
            df = _gen_test_df()
            records = df.to_dict("records")
            data = records[0]
        else:
            raise ValueError(f"Mode: ({mode}) not supported, must be 'row', 'pandas'...")
        test_log.log(data)

    test_log.close()
    profiler.disable()
    stats = pstats.Stats(profiler, stream=string_output_stream).sort_stats("cumulative")
    stats.print_stats(20)
    TEST_LOGGER.info(f"stats for rolling latency [{mode}]based benchmark are\n{string_output_stream.getvalue()}")
