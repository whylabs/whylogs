from typing import Any, Dict, Optional

from whylogs.core.metrics import (
    CardinalityMetric,
    ColumnCountsMetric,
    DistributionMetric,
)
from whylogs.core.view.column_profile_view import ColumnProfileView


def _calculate_descriptive_statistics(feature_name: str, column_view: Optional[ColumnProfileView]) -> Dict[str, float]:
    descriptive_statistics: Dict[str, float] = {}

    distribution_metric_: DistributionMetric = column_view.get_metric("dist")  # type: ignore
    stddev = distribution_metric_.stddev
    mean = distribution_metric_.mean.value
    column_counts_metric_: ColumnCountsMetric = column_view.get_metric("cnt")  # type: ignore
    count_n = column_counts_metric_.n.value
    count_missing = column_counts_metric_.null.value
    descriptive_statistics["stddev"] = stddev
    descriptive_statistics["coefficient_of_variation"] = stddev / mean
    descriptive_statistics["sum"] = (count_n - count_missing) * mean
    descriptive_statistics["variance"] = stddev**2
    descriptive_statistics["mean"] = mean

    return descriptive_statistics


def _calculate_quantile_statistics(feature_name: str, column_view: Optional[ColumnProfileView]) -> Dict[str, float]:
    quantile_statistics = {}
    distribution_metric_: DistributionMetric = column_view.get_metric("dist")  # type: ignore
    quantiles = distribution_metric_.kll.value.get_quantiles([0.05, 0.25, 0.75, 0.95])
    median = distribution_metric_.kll.value.get_quantiles([0.5])[0]
    quantile_statistics["iqr"] = quantiles[2] - quantiles[1]
    quantile_statistics["q1"] = quantiles[1]
    quantile_statistics["q3"] = quantiles[2]
    quantile_statistics["ninety_fifth_percentile"] = quantiles[3]
    quantile_statistics["fifth_percentile"] = quantiles[0]
    quantile_statistics["median"] = median
    return quantile_statistics


def add_feature_statistics(feature_name: str, column_view: Optional[ColumnProfileView]) -> Dict[str, Dict[str, Any]]:
    feature_with_statistics: Dict[str, Dict[str, Any]] = {feature_name: {}}
    card_: CardinalityMetric = column_view.get_metric("card")  # type: ignore
    card_estimate = card_.hll.value.get_estimate()
    column_counts_metric_: ColumnCountsMetric = column_view.get_metric("cnt")  # type: ignore
    count_n = column_counts_metric_.n.value
    count_missing = column_counts_metric_.null.value

    feature_with_statistics[feature_name]["missing"] = count_missing

    feature_with_statistics[feature_name]["distinct"] = card_estimate / (count_n - count_missing) * 100
    distribution_metric_: DistributionMetric = column_view.get_metric("dist")  # type: ignore
    feature_with_statistics[feature_name]["min"] = distribution_metric_.kll.value.get_min_value()
    feature_with_statistics[feature_name]["max"] = distribution_metric_.kll.value.get_max_value()
    feature_with_statistics[feature_name]["range"] = (
        feature_with_statistics[feature_name]["max"] - feature_with_statistics[feature_name]["min"]
    )

    feature_with_statistics[feature_name]["quantile_statistics"] = _calculate_quantile_statistics(
        feature_name, column_view
    )

    feature_with_statistics[feature_name]["descriptive_statistics"] = _calculate_descriptive_statistics(
        feature_name, column_view
    )

    return feature_with_statistics
