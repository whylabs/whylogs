from typing import Optional, Tuple, Union

from whylogs.core.metrics import DistributionMetric
from whylogs.core.view.column_profile_view import ColumnProfileView


def get_distribution_metrics(
    column_view: Optional[ColumnProfileView],
) -> Union[Tuple[None, None, None], Tuple[float, float, float]]:
    if column_view is None:
        raise ValueError("Column view not found.")
    distribution_metric: Optional[DistributionMetric] = column_view.get_metric("distribution")
    if distribution_metric is None:
        return None, None, None

    min_val = distribution_metric.kll.value.get_min_value()
    max_val = distribution_metric.kll.value.get_max_value()
    range_val = max_val - min_val
    return min_val, max_val, range_val
