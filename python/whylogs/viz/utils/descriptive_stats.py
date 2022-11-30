from typing import Optional, Tuple, Union

from typing_extensions import TypedDict

from whylogs.core.metrics import ColumnCountsMetric, DistributionMetric
from whylogs.core.view.column_profile_view import ColumnProfileView


class DescriptiveStatistics(TypedDict):
    """Collection of descriptive statistics, such as standard deviation,
    coefficient of variation, sum, variance and mean."""

    stddev: Optional[float]
    coefficient_of_variation: Optional[float]
    sum: Optional[float]
    variance: Optional[float]
    mean: Optional[float]


def _get_count_metrics_from_column_view(
    column_view: ColumnProfileView,
) -> Union[Tuple[None, None, None], Tuple[float, float, float]]:
    column_counts_metric: ColumnCountsMetric = column_view.get_metric("counts")  # type: ignore
    if column_counts_metric is None:
        return None, None, None
    count_n = column_counts_metric.n.value
    count_missing = column_counts_metric.null.value
    total_count = count_n - count_missing
    return total_count, count_n, count_missing


def _get_dist_metrics_from_column_view(
    column_view: ColumnProfileView,
) -> Union[Tuple[None, None, None], Tuple[float, float, float]]:
    distribution_metric: Optional[DistributionMetric] = column_view.get_metric("distribution")
    if distribution_metric is None or distribution_metric.kll.value.is_empty():
        return None, None, None
    stddev = distribution_metric.stddev
    mean = distribution_metric.mean.value
    variance = distribution_metric.variance
    return stddev, mean, variance


def _calculate_descriptive_statistics(
    column_view: Union[ColumnProfileView, None]
) -> Union[None, DescriptiveStatistics]:
    if column_view is None:
        return None

    stddev, mean, variance = _get_dist_metrics_from_column_view(column_view=column_view)
    _, count_n, count_missing = _get_count_metrics_from_column_view(column_view=column_view)

    if count_n is not None and count_missing is not None and mean is not None:
        sum_ = (count_n - count_missing) * mean
    else:
        sum_ = None

    if stddev is not None and mean:
        coefficient_of_variation = stddev / mean
    else:
        coefficient_of_variation = None

    descriptive_statistics: DescriptiveStatistics = {
        "stddev": stddev,
        "coefficient_of_variation": coefficient_of_variation,
        "sum": sum_,
        "variance": variance,
        "mean": mean,
    }
    return descriptive_statistics
