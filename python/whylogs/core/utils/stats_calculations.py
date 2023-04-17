from typing import Optional, Tuple, Union

from whylogs.core.metrics import DistributionMetric
from whylogs.core.view.column_profile_view import ColumnProfileView
from typing_extensions import TypedDict


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


def is_probably_unique(column_profile: ColumnProfileView) -> bool:
    cardinality = column_profile.get_metric("cardinality").to_summary_dict()
    counts = column_profile.get_metric("counts").to_summary_dict()
    if "cardinality" in column_profile.get_metric_names() and "counts" in column_profile.get_metric_names():
        if cardinality["lower_1"] <= counts["n"] - counts["null"] <= cardinality["upper_1"]:
            return True
    return False


class CardinalityEstimate(TypedDict):
    est: float
    unique_pct: float


def get_cardinality_estimate(column_profile: ColumnProfileView) -> float:
    cardinality = column_profile.get_metric("cardinality")
    counts = column_profile.get_metric("counts")
    est_value, est_ratio = None, None
    if cardinality is not None:
        est_value = cardinality.to_summary_dict().get("est", None)
    if cardinality is not None and counts is not None:
        n_value = counts.to_summary_dict().get("n", None) - counts.to_summary_dict().get("null", None)
        if n_value is not None and est_value is not None:
            est_ratio = est_value / n_value
    return CardinalityEstimate(est=est_value, unique_pct=est_ratio)
