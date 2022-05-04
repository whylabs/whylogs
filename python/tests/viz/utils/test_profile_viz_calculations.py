from unittest.mock import MagicMock, Mock

import pytest

from whylogs.core import ColumnProfileView
from whylogs.core.metrics.metrics import FrequentItem
from whylogs.viz.utils.profile_viz_calculations import (
    _get_cardinality_metrics_from_column_view,
    _get_distribution_metrics,
    add_feature_statistics,
    get_frequent_items_estimate,
)


@pytest.fixture
def mock_freq_items_list():
    mock_frequent_items = Mock(wraps=FrequentItem)
    mock_frequent_items.est = MagicMock(return_value=3)
    mock_frequent_items.value = MagicMock(return_value=2)
    return [mock_frequent_items]


@pytest.fixture
def mock_column_profile_view():
    mock_column_profile_view = Mock(wraps=ColumnProfileView)
    mock_column_profile_view.get_metric = MagicMock(return_value=None)
    return mock_column_profile_view


def test_get_frequent_items_estimate(mock_freq_items_list):
    freq_items_list = get_frequent_items_estimate(mock_freq_items_list)
    assert isinstance(freq_items_list, list)
    assert type(freq_items_list[0]) == dict
    for key in freq_items_list[0].keys():
        assert key in ["value", "estimate"]


def test_get_distribution_metrics(profile_view):
    column_view = profile_view.get_column(col_name="weight")

    min_val, max_val, range_val = _get_distribution_metrics(column_view=column_view)

    assert isinstance(min_val, float)
    assert isinstance(max_val, float)
    assert isinstance(range_val, float)

    assert max_val >= min_val

    expected_range_val = max_val - min_val
    assert range_val == expected_range_val


def test_get_distribution_metric_returns_none_if_metrics_are_none(mock_column_profile_view):
    min_val, max_val, range_val = _get_distribution_metrics(column_view=mock_column_profile_view)
    mock_column_profile_view.get_metric.assert_called_with("dist")
    assert not min_val
    assert not max_val
    assert not range_val


def test_get_cardinality_metrics_from_column_view(profile_view):
    column_view = profile_view.get_column(col_name="weight")
    count_n = 3
    count_missing = 1
    distinct = _get_cardinality_metrics_from_column_view(
        column_view=column_view, count_n=count_n, count_missing=count_missing
    )

    cardinality = column_view.get_metric("card")
    card_estimate = cardinality.hll.value.get_estimate()
    expected_distinct = card_estimate / (count_n - count_missing) * 100

    assert isinstance(distinct, float)
    assert distinct == expected_distinct


def test_get_cardinality_metrics_returns_none_if_not_card(mock_column_profile_view):
    distinct = _get_cardinality_metrics_from_column_view(column_view=mock_column_profile_view)
    mock_column_profile_view.get_metric.assert_called_with("card")
    assert not distinct


def test_add_feature_statistics_returns_dict(profile_view):
    column_view = profile_view.get_column(col_name="weight")
    feature_dict = add_feature_statistics(column_view=column_view, feature_name="weight")

    assert type(feature_dict) == dict
    for key in feature_dict["weight"].keys():
        assert key in ["missing", "distinct", "min", "max", "range", "quantile_statistics", "descriptive_statistics"]


def test_histogram_from_sketch():
    # TODO build test case -> at the moment its not implemented or used
    pass
