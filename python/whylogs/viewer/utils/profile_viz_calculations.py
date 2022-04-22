from typing import Any, Dict

from whylogs.core import ColumnProfile
from whylogs.core.metrics import (
    CardinalityMetric,
    ColumnCountsMetric,
    DistributionMetric,
)


def _calculate_descriptive_statistics(feature_name: str, profile: Dict[str, ColumnProfile]) -> Dict[str, float]:
    descriptive_statistics: Dict[str, float] = {}

    dist_: DistributionMetric = profile[feature_name]._metrics["dist"]  # type: ignore
    stddev = dist_.stddev
    mean = dist_.mean.value
    cnt_: ColumnCountsMetric = profile[feature_name]._metrics["cnt"]  # type: ignore
    cnt_n = cnt_.n.value
    cnt_missing = cnt_.null.value
    descriptive_statistics["stddev"] = stddev
    descriptive_statistics["coefficient_of_variation"] = stddev / mean
    descriptive_statistics["sum"] = (cnt_n - cnt_missing) * mean
    descriptive_statistics["variance"] = stddev * 2
    descriptive_statistics["mean"] = mean
    return descriptive_statistics


def _calculate_quantile_statistics(feature_name: str, profile: Dict[str, ColumnProfile]) -> Dict[str, float]:
    quantile_statistics = {}
    dist_: DistributionMetric = profile[feature_name]._metrics["dist"]  # type: ignore
    qts = dist_.kll.value.get_quantiles([0.05, 0.25, 0.75, 0.95])
    median = dist_.kll.value.get_quantiles([0.5])[0]
    quantile_statistics["iqr"] = qts[2] - qts[1]
    quantile_statistics["q1"] = qts[1]
    quantile_statistics["q3"] = qts[2]
    quantile_statistics["ninety_fifth_percentile"] = qts[3]
    quantile_statistics["fifth_percentile"] = qts[0]
    quantile_statistics["median"] = median
    return quantile_statistics


def add_feature_statistics(feature_name: str, profile: Dict[str, ColumnProfile]) -> Dict[str, Dict[str, Any]]:
    feature_with_statistics: Dict[str, Dict[str, Any]] = {feature_name: {}}
    card_: CardinalityMetric = profile[feature_name]._metrics["card"]  # type: ignore
    card_estimate = card_.hll.value.get_estimate()
    cnt_: ColumnCountsMetric = profile[feature_name]._metrics["cnt"]  # type: ignore
    cnt_n = cnt_.n.value
    cnt_missing = cnt_.null.value

    feature_with_statistics[feature_name]["missing"] = cnt_missing

    feature_with_statistics[feature_name]["distinct"] = card_estimate / (cnt_n - cnt_missing) * 100
    dist_: DistributionMetric = profile[feature_name]._metrics["dist"]  # type: ignore
    feature_with_statistics[feature_name]["min"] = dist_.kll.value.get_min_value()
    feature_with_statistics[feature_name]["max"] = dist_.kll.value.get_max_value()
    feature_with_statistics[feature_name]["range"] = (
        feature_with_statistics[feature_name]["max"] - feature_with_statistics[feature_name]["min"]
    )

    feature_with_statistics[feature_name]["quantile_statistics"] = _calculate_quantile_statistics(feature_name, profile)

    feature_with_statistics[feature_name]["descriptive_statistics"] = _calculate_descriptive_statistics(
        feature_name, profile
    )

    return feature_with_statistics
