import math
from logging import getLogger
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from typing_extensions import TypedDict
from whylogs_sketching import kll_doubles_sketch  # type: ignore

from whylogs.core.metrics import (
    CardinalityMetric,
    ColumnCountsMetric,
    DistributionMetric,
)
from whylogs.core.metrics.metrics import FrequentItem
from whylogs.core.view.column_profile_view import ColumnProfileView
from scipy import stats
from whylogs.core.configs import SummaryConfig


MAX_HIST_BUCKETS = 30
HIST_AVG_NUMBER_PER_BUCKET = 4.0

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


def _calculate_bins(
    end: float, start: float, n: int, avg_per_bucket: float, max_buckets: int
) -> Tuple[List[float], float, float]:
    # Include the max value in the right-most bin
    end += abs(end) * 1e-7
    abs_end = abs(end)
    abs_start = abs(start)
    max_magnitude = max(abs_end, abs_start)

    # the kll_floats_sketch use 32bit floats, so we check precision against np.float32
    float_mantissa_bits = np.finfo(np.float32).nmant

    # Include the right edge in the bin edges
    n_buckets = min(math.ceil(n / avg_per_bucket), max_buckets)
    width = (end - start) / n_buckets

    # Figure out the floating point precision at the scale of the bin boundaries
    # min_interval is the smallest difference between floats at this scale
    log_min_interval = math.floor(math.log2(max_magnitude)) - float_mantissa_bits
    min_interval = math.pow(2, log_min_interval)

    # If the bin width is smaller than min_interval, we need bigger bins
    if width < min_interval:
        new_buckets = math.floor((end - start) / min_interval)
        logger.warning(
            f"A bin width of {width} won't work with values in range of [{start}, {end}] "
            f"because numbers closer to each other than {int(min_interval)} might not be distinct "
            "when passed as float32: avoiding bin edge collisions by resizing from: "
            f"{n_buckets} to: {new_buckets} histogram buckets in summary."
        )
        n_buckets = max(new_buckets, 1)
        width = (end - start) / n_buckets
        logger.info(f"New bin widh is: {width} across {n_buckets} buckets")

    # Calculate histograms from the Probability Mass Function
    bins = [start + i * width for i in range(n_buckets + 1)]
    logger.debug(f"about to get pmf using start: {start} end:{end} width:{width} and n_buckets:{n_buckets}")
    logger.debug(f"bin: {bins}")
    return bins, end, start


def _ks_test_compute_p_value(target_distribution, reference_distribution):
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

    return type("Object", (), {"ks_test": p_value})


def compute_chi_squared_test_p_value(target_distribution, reference_distribution):
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
    return type("Object", (), {"chi_squared_test": p_value})


class FrequentItemEstimate(TypedDict):
    value: str
    estimate: float


def get_frequent_items_estimate(frequent_items: List[FrequentItem]) -> List[FrequentItemEstimate]:
    return [{"value": x.value, "estimate": x.est} for x in frequent_items]


class FeatureStats(TypedDict):
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


class FrequentItemStats(TypedDict):
    frequent_items: List[FrequentItemEstimate]
    total_count: int
    unique_count: Optional[int]


def calculate_drift_values(target_view, ref_view, config: Optional[SummaryConfig]):
    if config is None:
        config = SummaryConfig()

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
                pass
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
                    "frequent_items": get_frequent_items_estimate(target_frequent_items),
                    "total_count": target_count,
                    "unique_count": target_unique_count,
                }

                ref_frequent_stats = {
                    "frequent_items": get_frequent_items_estimate(ref_frequent_items),
                    "total_count": ref_count,
                }

                compute_chi_squared_test_p_value(target_frequent_stats, ref_frequent_stats)

                pass

    return


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


def histogram_from_sketch(
    sketch: kll_doubles_sketch, max_buckets: int = None, avg_per_bucket: Optional[float] = None
) -> Dict[str, Any]:
    """
    Generate a summary of a kll_floats_sketch, including a histogram

    Parameters
    ----------
    sketch : kll_floats_sketch
        Data sketch
    max_buckets : int
        Override the default maximum number of buckets
    avg_per_bucket : int
        Override the default target number of items per bucket.

    Returns
    -------
    histogram : HistogramSummary
        Protobuf histogram message
    """
    histogram = {}
    n = sketch.get_n()
    start = sketch.get_min_value()
    max_val = sketch.get_max_value()
    end = max_val
    if max_buckets is None:
        max_buckets = MAX_HIST_BUCKETS
    if avg_per_bucket is None:
        avg_per_bucket = HIST_AVG_NUMBER_PER_BUCKET

    if (n < 2) or (start == end):
        dx = abs(start) * 1e-7
        end = start + dx
        bins = [start, end]
        counts = [n]
    else:
        bins, end, start = _calculate_bins(end, start, n, avg_per_bucket, max_buckets)
        pmf = sketch.get_pmf(bins)
        counts = [round(p * n) for p in pmf]
        counts = counts[1:-1]

    histogram = {
        "start": start,
        "end": end,
        "width": 0,
        "counts": counts,
        "max": max_val,
        "min": start,
        "bins": bins,
        "n": n,
    }
    return histogram
