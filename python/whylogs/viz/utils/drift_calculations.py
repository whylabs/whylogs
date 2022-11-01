import warnings
from typing import Dict, List, Optional, Union

import numpy as np
from scipy import stats  # type: ignore
from scipy.spatial.distance import euclidean
from typing_extensions import TypedDict
from whylogs_sketching import kll_doubles_sketch  # type: ignore

from whylogs.core.view.column_profile_view import ColumnProfileView  # type: ignore
from whylogs.core.view.dataset_profile_view import DatasetProfileView  # type: ignore
from whylogs.viz.configs import HellingerConfig, KSTestConfig
from whylogs.viz.utils import _calculate_bins
from whylogs.viz.utils.frequent_items_calculations import (
    FrequentStats,
    get_frequent_stats,
    zero_padding_frequent_items,
)


class ColumnDriftValue(TypedDict):
    """p-value for applied statistical test, along with the name of the applied test."""

    p_value: float
    test: str


class ColumnDriftStatistic(TypedDict):
    """Statistic for applied algorithm, along with the name of the applied algorithm."""

    statistic: float
    algorithm: str


def _compute_ks_test_p_value(
    target_distribution: kll_doubles_sketch,
    reference_distribution: kll_doubles_sketch,
    quantiles: Optional[List[float]] = None,
) -> Optional[ColumnDriftValue]:
    """Compute the Kolmogorov-Smirnov test p-value of two continuous distributions.

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
    quantiles: Optional[List[float]], optional
        Bucket of quantiles used to get the CDF's for both target and reference profiles.

    Returns
    -------
        p_value : float
        The estimated p-value from the parametrized KS test, applied on the target and reference distributions'
        kll_floats_sketch summaries

    """

    if not quantiles:
        QUANTILES = KSTestConfig().quantiles
    else:
        QUANTILES = quantiles

    if reference_distribution.is_empty() or target_distribution.is_empty():
        return None

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


def _get_ks_p_value(target_view_column, reference_view_column) -> Optional[ColumnDriftValue]:
    target_dist_metric = target_view_column.get_metric("distribution")
    ref_dist_metric = reference_view_column.get_metric("distribution")

    if target_dist_metric is None or ref_dist_metric is None:
        return None

    target_kll_sketch = target_dist_metric.kll.value
    ref_kll_sketch = ref_dist_metric.kll.value

    ks_p_value = _compute_ks_test_p_value(target_kll_sketch, ref_kll_sketch)
    return ks_p_value


def _compute_chi_squared_test_p_value(
    target_distribution: FrequentStats, reference_distribution: FrequentStats
) -> Optional[ColumnDriftValue]:
    """
    Calculate the Chi-Squared test p-value for two discrete distributions.

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
        p_value : ColumnDriftValue or None. ColumnDriftValue has fields `p-value` with the test's result,
        and the name of the test applied in the `test` field (chi-squared).
        The estimated p-value from the Chi-Squared test, applied on the target and reference distributions'
        frequent and unique items summaries
    """
    target_freq_items = target_distribution["frequent_items"]
    ref_freq_items = reference_distribution["frequent_items"]
    target_total_count = target_distribution["total_count"]
    target_unique_count = target_distribution["unique_count"]
    ref_total_count = reference_distribution["total_count"]

    if ref_total_count <= 0 or target_total_count <= 0:
        return None

    target_freq_items, ref_freq_items = zero_padding_frequent_items(target_freq_items, ref_freq_items)

    ref_dist_items = dict()
    for item in reference_distribution["frequent_items"]:
        ref_dist_items[item["value"]] = item["estimate"]
    proportion_ref_dist_items = {k: v / ref_total_count for k, v in ref_dist_items.items()}

    chi_sq = 0.0
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


def _get_chi2_p_value(target_view_column, reference_view_column) -> Optional[ColumnDriftValue]:
    target_frequent_stats: FrequentStats = get_frequent_stats(target_view_column, config=None)
    ref_frequent_stats: FrequentStats = get_frequent_stats(reference_view_column, config=None)

    if not target_frequent_stats or not ref_frequent_stats:
        return None

    chi2_p_value = _compute_chi_squared_test_p_value(
        target_distribution=target_frequent_stats, reference_distribution=ref_frequent_stats
    )
    return chi2_p_value


def calculate_hellinger_distance(target_pmf: List[float], reference_pmf: List[float]) -> float:
    """Calculates hellinger distance between two discrete probability distributions.

    Parameters
    ----------
    target_pmf : List[float]
        Target discrete probability distribution.
    reference_pmf : List[float]
        Reference discrete probability distribution.

    Returns
    -------
    float
        The hellinger distance between the two discrete probability distributions.
        Between 0 (identical distributions) and 1 (completely different distributions).
    """
    # https://en.wikipedia.org/wiki/Hellinger_distance
    distance = euclidean(np.sqrt(target_pmf), np.sqrt(reference_pmf)) / np.sqrt(2)
    return distance


def _get_hellinger_distance(
    target_view_column: ColumnProfileView, reference_view_column: ColumnProfileView, nbins: Optional[int] = None
) -> Optional[ColumnDriftStatistic]:
    MAX_HIST_BUCKETS = HellingerConfig().max_hist_buckets
    HIST_AVG_NUMBER_PER_BUCKET = HellingerConfig().hist_avg_number_per_bucket
    if not nbins:
        nbins = MAX_HIST_BUCKETS
    target_dist_metric = target_view_column.get_metric("distribution")
    ref_dist_metric = reference_view_column.get_metric("distribution")

    if target_dist_metric is None or ref_dist_metric is None:
        warnings.warn("Column must have a Distribution Metric assigned to it.")
        return None

    target_kll_sketch = target_dist_metric.kll.value
    ref_kll_sketch = ref_dist_metric.kll.value

    if target_kll_sketch.is_empty() or ref_kll_sketch.is_empty():
        warnings.warn("Distribution sketch must not be empty.")
        return None

    start = min([target_kll_sketch.get_min_value(), ref_kll_sketch.get_min_value()])
    end = max([target_kll_sketch.get_max_value(), ref_kll_sketch.get_max_value()])
    n = target_kll_sketch.get_n() + ref_kll_sketch.get_n()
    bins, end = _calculate_bins(end=end, start=start, n=n, avg_per_bucket=HIST_AVG_NUMBER_PER_BUCKET, max_buckets=nbins)

    target_pmf = target_kll_sketch.get_pmf(bins)
    ref_pmf = ref_kll_sketch.get_pmf(bins)
    distance = calculate_hellinger_distance(target_pmf=target_pmf, reference_pmf=ref_pmf)
    return {"statistic": distance, "algorithm": "hellinger"}


def calculate_drift_values(
    target_view: DatasetProfileView, reference_view: DatasetProfileView, statistic=False
) -> Dict[str, Optional[Union[ColumnDriftValue, ColumnDriftStatistic]]]:
    """Calculate drift values between both profiles. Applicable for numerical and categorical features.

    Calculates drift only for features found in both profiles, and ignore those not found in either profile.

    Parameters
    ----------
    target_view : DatasetProfileView
        Target Profile View
    reference_view : DatasetProfileView
        Reference Profile View
    statistic: bool
        If false, value will be a pvalue. If true value will be a statistic.


    Returns
    -------
    drift_values: Dict[str, Optional[ColumnDriftValue]]
        A dictionary of the p-values, along with the type of test applied, for the given features.
    """
    drift_values: Dict[str, Optional[Union[ColumnDriftValue, ColumnDriftStatistic]]] = {}
    target_view_columns = target_view.get_columns()
    reference_view_columns = reference_view.get_columns()
    for target_column_name in target_view_columns:
        if target_column_name in reference_view_columns:
            target_view_column = target_view_columns[target_column_name]
            reference_view_column = reference_view_columns[target_column_name]

            if not statistic:
                drift_values[target_column_name] = _get_ks_p_value(
                    target_view_column=target_view_column, reference_view_column=reference_view_column
                ) or _get_chi2_p_value(
                    target_view_column=target_view_column, reference_view_column=reference_view_column
                )
            else:
                drift_values[target_column_name] = _get_hellinger_distance(
                    target_view_column=target_view_column, reference_view_column=reference_view_column
                )
    return drift_values
