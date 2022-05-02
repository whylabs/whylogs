from whylogs.viz.utils import _calculate_quantile_statistics  # type: ignore


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
    # TODO resize bins expected vs. actual logic
    pass
