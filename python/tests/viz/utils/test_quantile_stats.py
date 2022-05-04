import math

import numpy as np

from whylogs.viz.utils import _calculate_quantile_statistics  # type: ignore
from whylogs.viz.utils.quantile_stats import (
    _calculate_bins,
    _get_min_interval,
    _resize_bins,
)


def test_quantile_stats_should_return_none_if_column_is_none(profile_view):
    column_view = profile_view.get_column(col_name="any_feature")
    actual_stats = _calculate_quantile_statistics(column_view=column_view)
    assert actual_stats is None


def test_calculate_quantile_stats_should_return_dict(profile_view):
    column_view = profile_view.get_column(col_name="weight")
    actual_stats = _calculate_quantile_statistics(column_view=column_view)

    for key in actual_stats.keys():
        assert key in ["fifth_percentile", "iqr", "q1", "median", "q3", "ninety_fifth_percentile"]

    expected_iqr = actual_stats["q3"] - actual_stats["q1"]
    actual_iqr = actual_stats["iqr"]

    assert expected_iqr == actual_iqr


def test_resize_bins():
    start = 1
    end = 23
    n_buckets = 1
    width = 2

    actual_n_buckets, actual_width = _resize_bins(
        start=start, end=end, width=width, n_buckets=n_buckets, min_interval=3.5
    )

    assert actual_width > width
    assert actual_n_buckets > n_buckets


def test_get_min_interval():
    abs_start = 0.2
    abs_end = 23.2

    max_magnitude = max(abs_start, abs_end)
    log_min_interval = math.floor(math.log2(max_magnitude)) - np.finfo(np.float32).nmant
    expected_min_interval = math.pow(2, log_min_interval)

    actual_min_interval = _get_min_interval(abs_start, abs_end)

    assert expected_min_interval == actual_min_interval


def test_calculate_bins():
    end, start, n, avg_per_bucket, max_buckets = (23.0, 2.1, 1, 0.5, 3)

    actual_bins, actual_end = _calculate_bins(
        end=end, start=start, n=n, avg_per_bucket=avg_per_bucket, max_buckets=max_buckets
    )

    end += abs(end) * 1e-7

    n_buckets = min(math.ceil(n / avg_per_bucket), max_buckets)
    width = (end - start) / n_buckets

    min_interval = _get_min_interval(abs(start), abs(end))
    if width < min_interval:
        n_buckets, width = _resize_bins(start, end, min_interval, width, n_buckets)

    expected_bins = [start + i * width for i in range(n_buckets + 1)]

    assert isinstance(actual_bins, list)
    assert isinstance(end, float)
    assert expected_bins == actual_bins
    assert actual_end == end
