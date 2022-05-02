from whylogs.viz.utils.descriptive_stats import (
    _calculate_descriptive_statistics,
    _get_count_metrics_from_column_view, _get_dist_metrics_from_column_view
)


def test_descriptive_statistics_returns_none_if_dist_metric_is_none(profile_view):
    column_view = profile_view.get_column(col_name="any_feature")
    actual = _calculate_descriptive_statistics(column_view=column_view)
    assert actual is None


def test_count_metrics_should_return_count_values(profile_view):
    column_view = profile_view.get_column(col_name="weight")

    count_n, count_missing = _get_count_metrics_from_column_view(column_view=column_view)

    assert isinstance(count_n, int)
    assert isinstance(count_missing, int)


def test_dist_metrics_should_return_expected_values(profile_view):
    column_view = profile_view.get_column(col_name="weight")

    stddev, mean, variance = _get_dist_metrics_from_column_view(column_view)

    assert isinstance(stddev, float)
    assert isinstance(variance, float)
    assert isinstance(mean, float)


def test_descriptive_statistics_returns_typed_dict(profile_view):
    column_view = profile_view.get_column(col_name="weight")
    actual = _calculate_descriptive_statistics(column_view=column_view)

    assert len(actual) == 5
    for key in actual.keys():
        assert key in ["stddev", "coefficient_of_variation", "sum", "variance", "mean"]
