from unittest.mock import MagicMock, Mock

import pytest

from whylogs.core.metrics.metrics import FrequentItem
from whylogs.viz.utils.frequent_items_calculations import (
    frequent_items_from_view,
    get_frequent_items_estimate,
    get_frequent_stats,
)


@pytest.fixture
def mock_freq_items_list():
    mock_frequent_items = Mock(wraps=FrequentItem)
    mock_frequent_items.est = MagicMock(return_value=3)
    mock_frequent_items.value = MagicMock(return_value=2)
    return [mock_frequent_items]


@pytest.fixture
def mock_column_view():
    mock_column_view = Mock()
    mock_column_view.get_metric = MagicMock(return_value=None)
    return mock_column_view


def test_get_frequent_items_estimate(mock_freq_items_list):
    freq_items_list = get_frequent_items_estimate(mock_freq_items_list)
    assert isinstance(freq_items_list, list)
    assert type(freq_items_list[0]) == dict
    for key in freq_items_list[0].keys():
        assert key in ["value", "estimate"]


def test_get_frequent_stats_returns_none(mock_column_view):
    actual = get_frequent_stats(column_view=mock_column_view, config=None)
    mock_column_view.get_metric.assert_called_once_with("frequent_items")
    assert not actual


def test_get_frequent_stats_returns_expected_keys(profile_view):
    column_view = profile_view.get_column("animal")
    actual = get_frequent_stats(column_view=column_view, config=None)

    for key in actual.keys():
        assert key in ["frequent_items", "total_count", "unique_count"]


def test_frequent_items_from_view_returns_error(mock_column_view):
    with pytest.raises(ValueError, match="Frequent Items Metrics not found for feature any_feature"):
        frequent_items_from_view(column_view=mock_column_view, feature_name="any_feature", config=Mock())
    mock_column_view.get_metric.assert_called_once_with("frequent_items")


def test_frequent_items_from_view(profile_view):
    column_view = profile_view.get_column("animal")
    actual_result = frequent_items_from_view(column_view=column_view, feature_name="animal", config=None)

    assert isinstance(actual_result, list)
    for dict_ in actual_result:
        for key in dict_.keys():
            assert key in ["value", "estimate"]
