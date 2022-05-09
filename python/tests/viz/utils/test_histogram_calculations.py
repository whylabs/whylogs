from unittest.mock import MagicMock, Mock

import numpy as np
import pandas as pd
import pytest

import whylogs as why
from whylogs.viz.utils import _calculate_bins
from whylogs.viz.utils.histogram_calculations import histogram_from_view, _histogram_from_sketch


MAX_HIST_BUCKETS = 30
HIST_AVG_NUMBER_PER_BUCKET = 4.0
FLOAT_TYPES = [float, np.float16, np.float32, np.float64, np.floating, np.float_, np.longdouble]


@pytest.fixture
def mock_column_view():
    column_view = Mock()
    column_view.get_metric = MagicMock(return_value=None)
    return column_view


def test_histogram_from_view_raises_exception_if_metric_is_null(mock_column_view):
    with pytest.raises(ValueError, match="Distribution Metrics not found for feature my_feature"):
        histogram_from_view(column_view=mock_column_view, feature_name="my_feature")


def test_histogram_from_view(profile_view):
    column_view = profile_view.get_column("weight")
    result = histogram_from_view(column_view=column_view, feature_name="weight")
    for key in result.keys():
        assert key in [
            "start",
            "end",
            "width",
            "counts",
            "max",
            "min",
            "bins",
            "n"
        ]


def test_private_histogram_from_view(profile_view):
    column_view = profile_view.get_column("weight")
    kll = column_view.get_metric("distribution").kll.value
    result = _histogram_from_sketch(kll)

    expected_n = kll.get_n()
    expected_start = kll.get_min_value()
    expected_end = kll.get_max_value()
    expected_width = 0

    expected_bins, expected_end = _calculate_bins(
        expected_end, expected_start, expected_n, HIST_AVG_NUMBER_PER_BUCKET, MAX_HIST_BUCKETS
    )
    pmf = kll.get_pmf(expected_bins)
    counts = [round(p * expected_n) for p in pmf]
    expected_counts = counts[1:-1]

    assert result["n"] == expected_n
    assert result["start"] == expected_start
    assert result["end"] == expected_end
    assert result["width"] == expected_width
    assert result["counts"] == expected_counts
    assert result["bins"] == expected_bins


def test_histogram_with_smaller_n():
    data = {"col1": [1.5]}
    profile = why.log(pandas=pd.DataFrame(data))
    profile_view = profile.view()
    kll = profile_view.get_column("col1").get_metric("distribution").kll.value

    actual_result = _histogram_from_sketch(kll)

    expected_n = kll.get_n()
    expected_start = kll.get_min_value()
    expected_width = 0

    dx = abs(expected_start) * 1e-7
    expected_end = expected_start + dx
    expected_bins = [expected_start, expected_end]
    expected_counts = [expected_n]

    assert actual_result["n"] == expected_n
    assert actual_result["start"] == expected_start
    assert actual_result["end"] == expected_end
    assert actual_result["width"] == expected_width
    assert actual_result["counts"] == expected_counts
    assert actual_result["bins"] == expected_bins


@pytest.mark.parametrize("data_type", FLOAT_TYPES)
def test_histogram_with_different_data_types(data_type):
    data = {"col1": [1.5, 4., 55.3, 66.13, 36.333555, 63.35, 0.73, .77, 7.3, 0.0001]}
    profile = why.log(pandas=pd.DataFrame(data, dtype=data_type))
    profile_view = profile.view()
    kll = profile_view.get_column("col1").get_metric("distribution").kll.value

    actual_result = _histogram_from_sketch(kll)

    expected_n = kll.get_n()
    expected_start = kll.get_min_value()
    expected_end = kll.get_max_value()
    expected_width = 0

    expected_bins, expected_end = _calculate_bins(
        expected_end, expected_start, expected_n, HIST_AVG_NUMBER_PER_BUCKET, MAX_HIST_BUCKETS
    )
    pmf = kll.get_pmf(expected_bins)
    counts = [round(p * expected_n) for p in pmf]
    expected_counts = counts[1:-1]

    assert actual_result["n"] == expected_n
    assert actual_result["start"] == expected_start
    assert actual_result["end"] == expected_end
    assert actual_result["width"] == expected_width
    assert actual_result["counts"] == expected_counts
    assert actual_result["bins"] == expected_bins
