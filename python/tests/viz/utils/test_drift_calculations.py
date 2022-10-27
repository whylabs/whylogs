from unittest.mock import MagicMock, Mock

import numpy as np
import pandas as pd
import pytest

import whylogs as why
from whylogs.viz.utils.drift_calculations import (
    _compute_chi_squared_test_p_value,
    _compute_ks_test_p_value,
    _get_chi2_p_value,
    _get_ks_p_value,
    _get_hellinger_distance,
    calculate_drift_values,
)

FLOAT_TYPES = [float, np.float16, np.float32, np.float64, np.floating, np.float_, np.longdouble]


@pytest.fixture
def view_columns(profile_view):
    view_columns = profile_view.get_columns()
    return view_columns


@pytest.fixture
def mock_view_column():
    mock_view_column = Mock()
    mock_view_column.get_metric = MagicMock(return_value=None)
    mock_view_column.get_frequent_stats = MagicMock(return_value=None)
    return mock_view_column


@pytest.fixture
def reference_distribution():
    reference_distribution = {
        "frequent_items": [{"value": "some_value", "estimate": 10.1}],
        "total_count": 1,
        "unique_count": 1,
    }
    return reference_distribution


def test_get_ks_p_value_float(view_columns):
    actual_result = _get_ks_p_value(view_columns["weight"], view_columns["weight"])
    assert actual_result["test"] == "ks"
    assert isinstance(actual_result["p_value"], float)


def test_get_ks_p_value_int(view_columns):
    actual_result = _get_ks_p_value(view_columns["legs"], view_columns["legs"])
    assert actual_result["test"] == "ks"
    assert isinstance(actual_result["p_value"], float)


def test_get_ks_p_value_returns_none(mock_view_column):
    actual_result = _get_ks_p_value(mock_view_column, mock_view_column)
    assert not actual_result
    mock_view_column.get_metric.assert_called_with("distribution")


def test_get_chi2_p_value_str(view_columns):
    actual_result = _get_chi2_p_value(view_columns["animal"], view_columns["animal"])
    assert actual_result["test"] == "chi-squared"
    assert isinstance(actual_result["p_value"], float)


def test_get_chi2_p_value_returns_none(mock_view_column):
    result = _get_chi2_p_value(mock_view_column, mock_view_column)
    assert not result


def test_get_hellinger_distance_float(view_columns):
    actual_result = _get_hellinger_distance(view_columns["weight"], view_columns["weight"])
    assert actual_result["algorithm"] == "hellinger"
    assert isinstance(actual_result["statistic"], float)


def test_calculate_drift_values_result_format(profile_view):
    result_dict = calculate_drift_values(target_view=profile_view, reference_view=profile_view)
    expected_columns = profile_view.get_columns()

    for key, dict_value in result_dict.items():
        assert key in expected_columns
        for dict_value_key in dict_value.keys():
            assert dict_value_key in ["p_value", "test"]

    statistic_dict = calculate_drift_values(target_view=profile_view, reference_view=profile_view, statistic=True)
    for key, dict_value in statistic_dict.items():
        if key == "animal":
            assert not dict_value
        if key in ["legs", "weight"]:
            for dict_value_key in dict_value.keys():
                assert dict_value_key in ["statistic", "algorithm"]


def test_compute_chi_squared_test_p_value(reference_distribution):
    result = _compute_chi_squared_test_p_value(
        reference_distribution=reference_distribution, target_distribution=reference_distribution
    )
    assert result["test"] == "chi-squared"
    assert result["p_value"] >= 0.95


def test_compute_chi_squared_returns_none_if_total_count_is_zero(reference_distribution):
    target_distribution = reference_distribution.copy()
    reference_distribution["total_count"] = 0

    result = _compute_chi_squared_test_p_value(
        reference_distribution=reference_distribution, target_distribution=target_distribution
    )
    assert result is None

    reference_distribution["total_count"] = 1
    target_distribution["total_count"] = -1
    result = _compute_chi_squared_test_p_value(
        reference_distribution=reference_distribution, target_distribution=target_distribution
    )
    assert result is None


@pytest.mark.parametrize("data_type", FLOAT_TYPES)
def test_compute_ks_test_p_value_with_datatypes(data_type):
    data = {"col1": [1.5, 4.0, 55.3, 66.13, 0.0001]}
    profile = why.log(pandas=pd.DataFrame(data, dtype=data_type))
    profile_view = profile.view()
    kll = profile_view.get_column("col1").get_metric("distribution").kll.value

    result = _compute_ks_test_p_value(reference_distribution=kll, target_distribution=kll)
    actual_p_value = result["p_value"]

    assert isinstance(actual_p_value, float)
    assert 0.0 <= actual_p_value <= 1.0
    assert result["test"] == "ks"
