from unittest.mock import Mock, MagicMock

import pytest

from whylogs.core.metrics.metrics import FrequentItem
from whylogs.viz.utils.frequent_items_calculations import get_frequent_items_estimate, get_frequent_stats


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
