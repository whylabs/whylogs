import math
from logging import getLogger
import re
from typing import Any, Dict, List, Optional

import numpy as np
from typing_extensions import TypedDict

from whylogs.core.metrics import (
    CardinalityMetric,
    ColumnCountsMetric,
    DistributionMetric,
)
from whylogs.viz.utils.histogram_calculations import (
    histogram_from_view,
    HistogramSummary,
)
from whylogs.core.metrics.metrics import FrequentItem
from whylogs.core.view.column_profile_view import ColumnProfileView
from scipy import stats
from whylogs.core.configs import SummaryConfig
import json


logger = getLogger(__name__)

QUANTILES = [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0]


class DescStats(TypedDict):
    stddev: float
    coefficient_of_variation: float
    sum: float
    variance: float
    mean: float


def _calculate_descriptive_statistics(column_view: ColumnProfileView) -> Optional[DescStats]:
    distribution_metric: Optional[DistributionMetric] = column_view.get_metric("dist")
    if distribution_metric is None:
        return None
    stddev = distribution_metric.stddev
    mean = distribution_metric.mean.value
    column_counts_metric_: ColumnCountsMetric = column_view.get_metric("cnt")  # type: ignore
    count_n = column_counts_metric_.n.value
    count_missing = column_counts_metric_.null.value

    return {
        "stddev": stddev,
        "coefficient_of_variation": stddev / mean,
        "sum": (count_n - count_missing) * mean,
        "variance": distribution_metric.variance,
        "mean": mean,
    }


class QuantileStats(TypedDict):
    iqr: float
    q1: float
    q3: float
    ninety_fifth_percentile: float
    fifth_percentile: float
    median: float


def _calculate_quantile_statistics(column_view: ColumnProfileView) -> Optional[QuantileStats]:
    distribution_metric: Optional[DistributionMetric] = column_view.get_metric("dist")
    if distribution_metric is None:
        return None

    quantiles = distribution_metric.kll.value.get_quantiles([0.05, 0.25, 0.5, 0.75, 0.95])
    quantile_statistics: QuantileStats = {
        "fifth_percentile": quantiles[0],
        "iqr": quantiles[2] - quantiles[1],
        "q1": quantiles[1],
        "median": quantiles[2],
        "q3": quantiles[3],
        "ninety_fifth_percentile": quantiles[4],
    }
    return quantile_statistics


class ColumnDriftValue(TypedDict):
    p_value: float
    test: str


def _ks_test_compute_p_value(target_distribution, reference_distribution) -> Optional[ColumnDriftValue]:
    """
    Compute the Kolmogorov-Smirnov test p-value of two continuous distributions.
    Uses the quantile values and the corresponding CDFs to calculate the approximate KS statistic.
    Only applicable to continuous distributions.
    The null hypothesis expects the samples to come from the same distribution.

    Parameters
    ----------
    target_distribution : datasketches.kll_floats_sketch
        A kll_floats_sketch (quantiles sketch) from the target distribution's values
    reference_distribution : datasketches.kll_floats_sketch
        A kll_floats_sketch (quantiles sketch) from the reference (expected) distribution's values
        Can be generated from a theoretical distribution, or another sample for the same feature.

    Returns
    -------
        p_value : float
        The estimated p-value from the parametrized KS test, applied on the target and reference distributions'
        kll_floats_sketch summaries

    """

    D_max = 0
    target_quantile_values = target_distribution.get_quantiles(QUANTILES)
    ref_quantile_values = reference_distribution.get_quantiles(QUANTILES)

    num_quantiles = len(QUANTILES)
    i, j = 0, 0
    while i < num_quantiles and j < num_quantiles:

        if target_quantile_values[i] < ref_quantile_values[j]:
            current_quantile = target_quantile_values[i]
            i += 1
        else:
            current_quantile = ref_quantile_values[j]
            j += 1

        cdf_target = target_distribution.get_cdf([current_quantile])[0]
        cdf_ref = reference_distribution.get_cdf([current_quantile])[0]
        D = abs(cdf_target - cdf_ref)
        if D > D_max:
            D_max = D

    while i < num_quantiles:
        cdf_target = target_distribution.get_cdf([target_quantile_values[i]])[0]
        cdf_ref = reference_distribution.get_cdf([target_quantile_values[i]])[0]
        D = abs(cdf_target - cdf_ref)
        if D > D_max:
            D_max = D
        i += 1

    while j < num_quantiles:
        cdf_target = target_distribution.get_cdf([ref_quantile_values[j]])[0]
        cdf_ref = reference_distribution.get_cdf([ref_quantile_values[j]])[0]
        D = abs(cdf_target - cdf_ref)
        if D > D_max:
            D_max = D
        j += 1

    m, n = sorted([target_distribution.get_n(), reference_distribution.get_n()], reverse=True)
    en = m * n / (m + n)

    p_value = stats.distributions.kstwo.sf(D_max, np.round(en))

    return {"p_value": p_value, "test": "ks"}


def _compute_chi_squared_test_p_value(target_distribution, reference_distribution) -> Optional[ColumnDriftValue]:
    """
    Calculates the Chi-Squared test p-value for two discrete distributions.
    Uses the top frequent items summary, unique count estimate and total count estimate for each feature,
    to calculate the estimated Chi-Squared statistic.
    Applicable only to discrete distributions.

    Parameters
    ----------
    target_distribution : ReferenceDistributionDiscreteMessage
        The summary message of the target feature's distribution.
        Should be a ReferenceDistributionDiscreteMessage containing the frequent items,
        unique, and total count summaries.
    reference_distribution : ReferenceDistributionDiscreteMessage
        The summary message of the reference feature's distribution.
        Should be a ReferenceDistributionDiscreteMessage containing the frequent items,
        unique, and total count summaries.

    Returns
    -------
        p_value : float
        The estimated p-value from the Chi-Squared test, applied on the target and reference distributions'
        frequent and unique items summaries
    """
    target_freq_items = target_distribution["frequent_items"]
    target_total_count = target_distribution["total_count"]
    target_unique_count = target_distribution["unique_count"]
    ref_total_count = reference_distribution["total_count"]

    if ref_total_count <= 0 or target_total_count <= 0:
        return None

    ref_dist_items = dict()
    for item in reference_distribution["frequent_items"]:
        ref_dist_items[item["value"]] = item["estimate"]

    proportion_ref_dist_items = {k: v / ref_total_count for k, v in ref_dist_items.items()}

    chi_sq = 0
    for item in target_freq_items:
        target_frequency = item["estimate"]
        if item["value"] in ref_dist_items:
            ref_frequency = int(proportion_ref_dist_items[item["value"]] * target_total_count)
        else:
            ref_frequency = 0

        if ref_frequency == 0:
            chi_sq = np.inf
            break
        chi_sq += (target_frequency - ref_frequency) ** 2 / ref_frequency

    degrees_of_freedom = target_unique_count - 1
    degrees_of_freedom = degrees_of_freedom if degrees_of_freedom > 0 else 1
    p_value = stats.chi2.sf(chi_sq, degrees_of_freedom)
    return {"p_value": p_value, "test": "chi-squared"}


class FrequentItemEstimate(TypedDict):
    value: str
    estimate: float


def _get_frequent_items_estimate(frequent_items: List[FrequentItem]) -> List[FrequentItemEstimate]:
    return [{"value": x.value, "estimate": x.est} for x in frequent_items]


def frequent_items_from_view(
    column_view: ColumnProfileView, feature_name: str, config: SummaryConfig
) -> Dict[str, Any]:
    column_frequent_items_metric = column_view.get_metric("fi")
    if not column_frequent_items_metric:
        raise ValueError("Frequent Items Metrics not found for feature {}.".format(feature_name))

    target_frequent_items = column_frequent_items_metric.to_summary_dict(config)["fs"]
    frequent_items = _get_frequent_items_estimate(target_frequent_items)
    return frequent_items


class FeatureStats(TypedDict):
    total_count: Optional[int]
    missing: Optional[int]
    distinct: Optional[float]
    min: Optional[float]
    max: Optional[float]
    range: Optional[float]
    quantile_statistics: Optional[QuantileStats]
    descriptive_statistics: Optional[DescStats]


def add_feature_statistics(feature_name: str, column_view: Optional[ColumnProfileView]) -> Dict[str, FeatureStats]:
    if column_view is None:
        return {}

    feature_with_statistics: Dict[str, FeatureStats] = {}

    column_counts: Optional[ColumnCountsMetric] = column_view.get_metric("cnt")
    count_n = count_missing = None
    if column_counts is not None:
        count_n = column_counts.n.value
        count_missing = column_counts.null.value

    cardinality: Optional[CardinalityMetric] = column_view.get_metric("card")
    distinct = None
    if cardinality is not None and count_n is not None and count_missing is not None:
        card_estimate = cardinality.hll.value.get_estimate()
        distinct = card_estimate / (count_n - count_missing) * 100

    distribution_metric: Optional[DistributionMetric] = column_view.get_metric("dist")

    min_val = max_val = range_val = None
    if distribution_metric is not None:
        min_val = distribution_metric.kll.value.get_min_value()
        max_val = distribution_metric.kll.value.get_max_value()
        range_val = max_val - min_val

    feature_stats: FeatureStats = {
        "total_count": count_n - count_missing,
        "missing": count_missing,
        "distinct": distinct,
        "min": min_val,
        "max": max_val,
        "range": range_val,
        "quantile_statistics": _calculate_quantile_statistics(column_view),
        "descriptive_statistics": _calculate_descriptive_statistics(column_view),
    }
    feature_with_statistics[feature_name] = feature_stats

    return feature_with_statistics


class ColumnSummary(TypedDict):
    histogram: Optional[HistogramSummary]
    frequentItems: Optional[List[FrequentItemEstimate]]
    drift_from_ref: Optional[float]
    isDiscrete: Optional[bool]
    featureStats: Optional[FeatureStats]


def generate_summaries(target_view, ref_view, config: Optional[SummaryConfig]) -> Dict[str, Any]:
    if config is None:
        config = SummaryConfig()

    overall_stats: OverallStats = add_overall_statistics(target_view)
    drift_values = calculate_drift_values(target_view, ref_view, config=None)

    target_col_views = target_view.get_columns()
    ref_col_views = ref_view.get_columns()
    ref_summary = {"columns": {}, "properties": overall_stats}
    target_summary = {"columns": {}}
    for target_col_name in target_col_views:
        if target_col_name in ref_col_views:
            target_column_summary: ColumnSummary = {}
            ref_column_summary: ColumnSummary = {}

            target_col_view = target_col_views[target_col_name]
            ref_col_view = ref_col_views[target_col_name]

            target_stats = add_feature_statistics(target_col_name, target_col_view)
            target_column_summary["featureStats"] = target_stats[target_col_name]
            if target_col_name in drift_values:
                ref_column_summary["drift_from_ref"] = drift_values[target_col_name]["p_value"]
            if target_col_views[target_col_name].get_metric("dist") and ref_col_views[target_col_name].get_metric(
                "dist"
            ):
                target_column_summary["isDiscrete"] = ref_column_summary["isDiscrete"] = False

                target_histogram = histogram_from_view(target_col_view, target_col_name)
                target_column_summary["histogram"] = target_histogram
                ref_col_view = ref_col_views[target_col_name]
                ref_histogram = histogram_from_view(ref_col_view, target_col_name)
                ref_column_summary["histogram"] = ref_histogram
            elif target_col_views[target_col_name].get_metric("fi") and ref_col_views[target_col_name].get_metric("fi"):
                target_column_summary["isDiscrete"] = ref_column_summary["isDiscrete"] = True

                target_frequent_items = frequent_items_from_view(target_col_view, target_col_name, config)
                target_column_summary["frequentItems"] = target_frequent_items

                ref_frequent_items = frequent_items_from_view(ref_col_view, target_col_name, config)
                ref_column_summary["frequentItems"] = ref_frequent_items
            target_summary["columns"][target_col_name] = target_column_summary
            ref_summary["columns"][target_col_name] = ref_column_summary
    summaries = {
        "profile_from_whylogs": json.dumps(target_summary),
        "reference_profile_from_whylogs": json.dumps(ref_summary),
    }
    return summaries


class FrequentItemStats(TypedDict):
    frequent_items: List[FrequentItemEstimate]
    total_count: int
    unique_count: Optional[int]


def calculate_drift_values(target_view, ref_view, config: Optional[SummaryConfig]) -> Dict[str, ColumnDriftValue]:
    if config is None:
        config = SummaryConfig()
    drift_values = {}
    target_col_views = target_view.get_columns()
    ref_col_views = ref_view.get_columns()
    for target_col_name in target_col_views:
        if target_col_name in ref_col_views:
            if target_col_views[target_col_name].get_metric("dist") and ref_col_views[target_col_name].get_metric(
                "dist"
            ):
                target_dist_metric = target_col_views[target_col_name].get_metric("dist")
                target_kll_sketch = target_dist_metric.kll.value
                ref_dist_metric = ref_col_views[target_col_name].get_metric("dist")
                ref_kll_sketch = ref_dist_metric.kll.value

                ks_p_value = _ks_test_compute_p_value(target_kll_sketch, ref_kll_sketch)
                drift_values[target_col_name] = ks_p_value
            elif target_col_views[target_col_name].get_metric("fi") and ref_col_views[target_col_name].get_metric("fi"):
                target_fi_metric = target_col_views[target_col_name].get_metric("fi")
                target_frequent_items = target_fi_metric.to_summary_dict(config)["fs"]
                target_cnt_metric = target_col_views[target_col_name].get_metric("cnt")
                target_count = target_cnt_metric.n.value
                target_card_metric = target_col_views[target_col_name].get_metric("card")
                target_unique_count = int(target_card_metric.hll.value.get_estimate())

                ref_fi_metric = ref_col_views[target_col_name].get_metric("fi")
                ref_frequent_items = ref_fi_metric.to_summary_dict(config)["fs"]
                ref_cnt_metric = ref_col_views[target_col_name].get_metric("cnt")
                ref_count = ref_cnt_metric.n.value

                target_frequent_stats = {
                    "frequent_items": _get_frequent_items_estimate(target_frequent_items),
                    "total_count": target_count,
                    "unique_count": target_unique_count,
                }

                ref_frequent_stats = {
                    "frequent_items": _get_frequent_items_estimate(ref_frequent_items),
                    "total_count": ref_count,
                }

                chi2_p_value = _compute_chi_squared_test_p_value(target_frequent_stats, ref_frequent_stats)
                drift_values[target_col_name] = chi2_p_value

    return drift_values


class OverallStats(TypedDict):
    observations: int
    missing_cells: int
    missing_percentage: float


def add_overall_statistics(target_view) -> OverallStats:
    observations: int = 0
    missing_cells: int = 0
    missing_percentage: float = 0
    target_col_views = target_view.get_columns()
    for target_col_view in target_col_views.values():
        cnt_metric = target_col_view.get_metric("cnt")
        observations += cnt_metric.n.value
        null_count = cnt_metric.null.value
        missing_cells += null_count if null_count else 0
    missing_percentage = (missing_cells / observations) * 100 if observations else 0
    overall_stats = {
        "observations": observations,
        "missing_cells": missing_cells,
        "missing_percentage": missing_percentage,
    }
    return overall_stats
