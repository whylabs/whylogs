from typing import Tuple, Union

from typing_extensions import TypedDict

from whylogs.core.metrics import ColumnCountsMetric
from whylogs.core.view.column_profile_view import ColumnProfileView


class DescStats(TypedDict):
    stddev: float
    coefficient_of_variation: float
    sum: Union[float, None]
    variance: float
    mean: float


def _get_count_metrics_from_column_view(
    column_view: ColumnProfileView,
) -> Union[Tuple[None, None], Tuple[float, float]]:
    column_counts_metric: ColumnCountsMetric = column_view.get_metric("cnt")  # type: ignore
    if not column_counts_metric:
        return None, None
    count_n = column_counts_metric.n.value
    count_missing = column_counts_metric.null.value
    return count_n, count_missing


def _get_dist_metrics_from_column_view(column_view: ColumnProfileView) -> Tuple[float, float, float]:
    distribution_metric = column_view.get_metric("dist")
    stddev = distribution_metric.stddev
    mean = distribution_metric.mean.value
    variance = distribution_metric.variance
    return stddev, mean, variance


def _calculate_descriptive_statistics(column_view: Union[ColumnProfileView, None]) -> Union[None, DescStats]:
    if column_view is None:
        return None

    stddev, mean, variance = _get_dist_metrics_from_column_view(column_view=column_view)
    count_n, count_missing = _get_count_metrics_from_column_view(column_view=column_view)

    if count_n and count_missing:
        sum_ = (count_n - count_missing) * mean
    else:
        sum_ = None

    descriptive_statistics: DescStats = {
        "stddev": stddev,
        "coefficient_of_variation": stddev / mean,
        "sum": sum_,
        "variance": variance,
        "mean": mean,
    }
    return descriptive_statistics
