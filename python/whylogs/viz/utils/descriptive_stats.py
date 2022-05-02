from typing import TypedDict, Tuple, Union

from whylogs.core.view.column_profile_view import ColumnProfileView


class DescStats(TypedDict):
    stddev: float
    coefficient_of_variation: float
    sum: float
    variance: float
    mean: float


def _get_count_metrics_from_column_view(column_view: ColumnProfileView) -> Tuple[float, float]:
    column_counts_metric_: ColumnCountsMetric = column_view.get_metric("cnt")  # type: ignore
    count_n = column_counts_metric_.n.value
    count_missing = column_counts_metric_.null.value
    return count_n, count_missing


def _get_dist_metrics_from_column_view(column_view: ColumnProfileView) -> Tuple[float, float, float]:
    distribution_metric = column_view.get_metric("dist")
    stddev = distribution_metric.stddev
    mean = distribution_metric.mean.value
    variance = distribution_metric.variance
    return stddev, mean, variance


def _calculate_descriptive_statistics(
        column_view: Union[ColumnProfileView, None]
) -> Union[None, DescStats]:

    if column_view is None:
        return None

    stddev, mean, variance = _get_dist_metrics_from_column_view(column_view=column_view)
    count_n, count_missing = _get_count_metrics_from_column_view(column_view=column_view)

    descriptive_statistics: DescStats = {
        "stddev": stddev,
        "coefficient_of_variation": stddev / mean,
        "sum": (count_n - count_missing) * mean,
        "variance": variance,
        "mean": mean,
    }
    return descriptive_statistics
