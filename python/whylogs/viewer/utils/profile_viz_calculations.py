from typing import Any, Dict

from whylogs.core import ColumnProfile
from whylogs.core.metrics import (
    CardinalityMetric,
    ColumnCountsMetric,
    DistributionMetric,
)


def _calculate_descriptive_statistics(feature_name: str, profile: Dict[str, ColumnProfile]) -> Dict[str, float]:
    descriptive_statistics: Dict[str, float] = {}

    distribution_metric_: DistributionMetric = profile[feature_name]._metrics["dist"]  # type: ignore
    stddev = distribution_metric_.stddev
    mean = distribution_metric_.mean.value
    column_counts_metric_: ColumnCountsMetric = profile[feature_name]._metrics["cnt"]  # type: ignore
    count_n = column_counts_metric_.n.value
    count_missing = column_counts_metric_.null.value
    descriptive_statistics["stddev"] = stddev
    descriptive_statistics["coefficient_of_variation"] = stddev / mean
    descriptive_statistics["sum"] = (count_n - count_missing) * mean
    descriptive_statistics["variance"] = stddev**2
    descriptive_statistics["mean"] = mean

    return descriptive_statistics


def _calculate_quantile_statistics(feature_name: str, profile: Dict[str, ColumnProfile]) -> Dict[str, float]:
    quantile_statistics = {}
    distribution_metric_: DistributionMetric = profile[feature_name]._metrics["dist"]  # type: ignore
    quantiles = distribution_metric_.kll.value.get_quantiles([0.05, 0.25, 0.75, 0.95])
    median = distribution_metric_.kll.value.get_quantiles([0.5])[0]
    quantile_statistics["iqr"] = quantiles[2] - quantiles[1]
    quantile_statistics["q1"] = quantiles[1]
    quantile_statistics["q3"] = quantiles[2]
    quantile_statistics["ninety_fifth_percentile"] = quantiles[3]
    quantile_statistics["fifth_percentile"] = quantiles[0]
    quantile_statistics["median"] = median
    return quantile_statistics


def add_feature_statistics(feature_name: str, profile: Dict[str, ColumnProfile]) -> Dict[str, Dict[str, Any]]:
    feature_with_statistics: Dict[str, Dict[str, Any]] = {feature_name: {}}
    card_: CardinalityMetric = profile[feature_name]._metrics["card"]  # type: ignore
    card_estimate = card_.hll.value.get_estimate()
    column_counts_metric_: ColumnCountsMetric = profile[feature_name]._metrics["cnt"]  # type: ignore
    count_n = column_counts_metric_.n.value
    count_missing = column_counts_metric_.null.value

    feature_with_statistics[feature_name]["missing"] = count_missing

    feature_with_statistics[feature_name]["distinct"] = card_estimate / (count_n - count_missing) * 100
    distribution_metric_: DistributionMetric = profile[feature_name]._metrics["dist"]  # type: ignore
    feature_with_statistics[feature_name]["min"] = distribution_metric_.kll.value.get_min_value()
    feature_with_statistics[feature_name]["max"] = distribution_metric_.kll.value.get_max_value()
    feature_with_statistics[feature_name]["range"] = (
        feature_with_statistics[feature_name]["max"] - feature_with_statistics[feature_name]["min"]
    )

    feature_with_statistics[feature_name]["quantile_statistics"] = _calculate_quantile_statistics(feature_name, profile)

    feature_with_statistics[feature_name]["descriptive_statistics"] = _calculate_descriptive_statistics(
        feature_name, profile
    )

    return feature_with_statistics
