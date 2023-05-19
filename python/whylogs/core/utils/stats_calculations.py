from typing import Optional, Tuple, Union

from typing_extensions import TypedDict

from whylogs.core.configs import SummaryConfig
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


def is_probably_unique(column_profile: ColumnProfileView, hll_stddev: int = 3) -> bool:
    cardinality = column_profile.get_metric("cardinality").to_summary_dict(SummaryConfig(hll_stddev=hll_stddev))
    counts = column_profile.get_metric("counts").to_summary_dict()
    if "cardinality" in column_profile.get_metric_names() and "counts" in column_profile.get_metric_names():
        if cardinality[f"lower_{hll_stddev}"] <= counts["n"] - counts["null"] <= cardinality[f"upper_{hll_stddev}"]:
            return True
    return False


class CardinalityEstimate(TypedDict):
    est: Optional[float]
    unique_pct: Optional[float]


def get_cardinality_estimate(column_profile: ColumnProfileView) -> CardinalityEstimate:
    cardinality = column_profile.get_metric("cardinality")
    counts = column_profile.get_metric("counts")
    est_value: Optional[float] = None
    est_ratio: Optional[float] = None
    if cardinality is not None:
        est_value = cardinality.to_summary_dict().get("est", None)
    if cardinality is not None and counts is not None:
        n_value = counts.to_summary_dict().get("n", None) - counts.to_summary_dict().get("null", None)
        if n_value is not None and est_value is not None:
            try:
                est_ratio = est_value / n_value
            except ZeroDivisionError:
                est_ratio = None
    return CardinalityEstimate(est=est_value, unique_pct=est_ratio)


def only_null_values(column_profile: ColumnProfileView) -> bool:
    counts = column_profile.get_metric("counts")
    if counts is not None:
        counts_summary = counts.to_summary_dict()
        if counts_summary.get("n", None) == counts_summary.get("null", None):
            return True
    return False
