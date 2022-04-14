"""
Library module defining function for generating summaries
"""
import math
from logging import getLogger
from typing import Union

import datasketches
import numpy as np
from datasketches import (
    frequent_items_error_type,
    frequent_strings_sketch,
    kll_floats_sketch,
    update_theta_sketch,
)
from scipy import stats

from whylogs.proto import (
    ColumnSummary,
    FrequentStringsSummary,
    HistogramSummary,
    InferredType,
    QuantileSummary,
    ReferenceDistributionDiscreteMessage,
    UniqueCountSummary,
)

MAX_HIST_BUCKETS = 30
HIST_AVG_NUMBER_PER_BUCKET = 4.0
QUANTILES = [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0]

logger = getLogger(__name__)


def from_sketch(sketch: update_theta_sketch, num_std_devs: float = 1):
    """
    Generate a protobuf summary message from a datasketches theta sketch

    Parameters
    ----------
    sketch
        Theta sketch to summarize
    num_std_devs
        Number of standard deviations for calculating bounds

    Returns
    -------
    summary : UniqueCountSummary
    """
    return UniqueCountSummary(
        estimate=sketch.get_estimate(),
        upper=sketch.get_upper_bound(num_std_devs),
        lower=sketch.get_lower_bound(num_std_devs),
    )


def from_string_sketch(sketch: frequent_strings_sketch):
    """
    Generate a protobuf summary message from a string sketch

    Parameters
    ----------
    sketch
        Frequent strings sketch

    Returns
    -------
    summary : FrequentStringsSummary
    """
    frequent_items = sketch.get_frequent_items(frequent_items_error_type.NO_FALSE_NEGATIVES)
    # Note: frequent items is a list of tuples containing info about the
    # most frequent strings and their count:
    # [(string, est_count, lower bound, upper bound)]
    if len(frequent_items) == 0:
        return

    items = [{"value": x[0], "estimate": x[1]} for x in frequent_items]
    return FrequentStringsSummary(items=items)


def quantiles_from_sketch(sketch: kll_floats_sketch, quantiles=None):
    """
    Calculate quantiles from a data sketch

    Parameters
    ----------
    sketch : kll_floats_sketch
        Data sketch
    quantiles : list-like
        Override the default quantiles.  Should be a list of values from
        0 to 1 inclusive.
    """
    if quantiles is None:
        quantiles = QUANTILES
    qvals = sketch.get_quantiles(quantiles)
    return QuantileSummary(
        quantiles=quantiles,
        quantile_values=qvals,
    )


def single_quantile_from_sketch(sketch: kll_floats_sketch, quantile: float):
    """
    Calculate the specified quantile from a data sketch

    Parameters
    ----------
    sketch : kll_floats_sketch
        Data sketch
    quantile : float
        Override the default quantiles to a single quantile.  Should be a value from
        0 to 1 inclusive.

    Returns
    ----------
    Anonymous object with one filed equal to the quantile value
    """
    if quantile is None:
        raise ValueError("The quantile value is required and should be of type float")
    qval = sketch.get_quantiles([quantile])
    return type("Object", (), {"quantile": qval[0]})


def _calculate_bins(end: float, start: float, n: int, avg_per_bucket: float, max_buckets: int):
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


def histogram_from_sketch(sketch: kll_floats_sketch, max_buckets: int = None, avg_per_bucket: int = None):
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

    return HistogramSummary(
        start=start,
        end=end,
        width=0,
        counts=counts,
        max=max_val,
        min=start,
        bins=bins,
        n=n,
    )


def entropy_from_column_summary(summary: ColumnSummary, histogram: datasketches.kll_floats_sketch):
    """
    Calculate the estimated entropy for a ColumnProfile, using the ColumnSummary
    Can be used for both continuous and discrete types of data.

    Parameters
    ----------
    summary : ColumnSummary
        Protobuf summary message
    histogram: datasketches.kll_floats_sketch
        Data sketch for quantiles

    Returns
    -------
    entropy : float
        Estimated entropy value,
        np.nan if the inferred data type of the column is not categorical or numeric

    """

    frequent_items = summary.frequent_items
    unique_count = summary.unique_count.estimate
    inferred_type = summary.schema.inferred_type.type
    total_count = summary.counters.count

    if inferred_type == InferredType.Type.FRACTIONAL:
        if histogram.get_min_value() == histogram.get_max_value() or histogram.get_n() <= 1:
            return 0
        bins = np.linspace(histogram.get_min_value(), histogram.get_max_value(), 100)
        pmf = histogram.get_pmf(bins)
        pmf = list(filter(lambda x: x > 0, pmf))
        entropy = -np.sum(pmf * np.log(pmf))
        return entropy

    elif inferred_type in (InferredType.Type.INTEGRAL, InferredType.Type.STRING, InferredType.Type.BOOLEAN):
        if total_count == 0:
            return 0

        entropy = 0
        for item in frequent_items.items:
            i_frequency = item.estimate / total_count
            entropy += i_frequency * np.log(i_frequency)

        frequent_items_count = len(frequent_items.items)
        n_singles = unique_count - frequent_items_count
        if math.isclose(n_singles, 0.0, abs_tol=10e-3):
            return -entropy

        n_singles_frequency = n_singles / total_count
        entropy += n_singles_frequency * np.log(n_singles_frequency)
        return -entropy

    return np.nan


def ks_test_compute_p_value(target_distribution: kll_floats_sketch, reference_distribution: kll_floats_sketch):
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


def compute_kl_divergence(
    target_distribution: Union[kll_floats_sketch, ReferenceDistributionDiscreteMessage],
    reference_distribution: Union[kll_floats_sketch, ReferenceDistributionDiscreteMessage],
):
    """
    Calculates the KL divergence between a target feature and a reference feature.
    Applicable to both continuous and discrete distributions.
    Uses the pmf and the `datasketches.kll_floats_sketch` to calculate the KL divergence in the continuous case.
    Uses the top frequent items to calculate the KL divergence in the discrete case.

    Parameters
    ----------
    target_distribution : Union[kll_floats_sketch, ReferenceDistributionDiscreteMessage]
        The target distribution. Should be a `datasketches.kll_floats_sketch` if the target distribution is continuous.
        Should be a ReferenceDistributionDiscreteMessage if the target distribution is discrete.
        Both the target distribution, specified in `target_distribution`, and the reference distribution,
        specified in `reference_distribution` must be of the same type.
    reference_distribution : Union[kll_floats_sketch, ReferenceDistributionDiscreteMessage]
        The reference distribution. Should be a `datasketches.kll_floats_sketch` if the reference distribution
        is continuous. Should be a ReferenceDistributionDiscreteMessage if the reference distribution is discrete.
        Both the target distribution, specified in `target_distribution`, and the reference distribution,
        specified in `reference_distribution` must be of the same type.

    Returns
    -------
        kl_divergence : float
        The estimated value of the KL divergence between the target and the reference feature

    """

    if isinstance(target_distribution, kll_floats_sketch) and isinstance(reference_distribution, kll_floats_sketch):
        return _compute_kl_divergence_continuous_distributions(target_distribution, reference_distribution)
    elif all([isinstance(v, ReferenceDistributionDiscreteMessage) for v in (target_distribution, reference_distribution)]):
        return _compute_kl_divergence_discrete_distributions(target_distribution, reference_distribution)
    else:
        raise ValueError("Both provided distributions should be categorical or numeric, but not from mixed type")


def _compute_kl_divergence_continuous_distributions(target_distribution: kll_floats_sketch, reference_distribution: kll_floats_sketch):
    """
    Calculates the estimated KL divergence for two continuous distributions.
    Uses the `datasketches.kll_floats_sketch` sketch to calculate the KL divergence based on the PMFs.
    Only applicable to continuous distributions.

    Parameters
    ----------
    target_distribution : datasketches.kll_floats_sketch
        The quantiles summary of the target feature's distribution.
    reference_distribution : datasketches.kll_floats_sketch
        The quantiles summary of the reference feature's distribution.

    Returns
    -------
        kl_divergence : float
        The estimated KL divergence between two continuous features.

    """
    almost_zero_probability_of_event = 10e-5
    bins_target = np.linspace(target_distribution.get_min_value(), target_distribution.get_max_value(), 100)
    pmf_target = np.array(target_distribution.get_pmf(bins_target))

    pmf_reference = np.array(reference_distribution.get_pmf(bins_target))
    pmf_reference[pmf_reference == 0] = almost_zero_probability_of_event

    kl_divergence = np.sum(np.where(pmf_target != 0, pmf_target * np.log(pmf_target / pmf_reference), 0))
    return type("Object", (), {"kl_divergence": kl_divergence})


def _compute_kl_divergence_discrete_distributions(
    target_distribution: ReferenceDistributionDiscreteMessage, reference_distribution: ReferenceDistributionDiscreteMessage
):
    """
    Calculates the estimated KL divergence for two discrete distributions.
    Uses the frequent items summary to calculate the estimated frequencies of items in each distribution.
    Only applicable to discrete distributions.

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
        kl_divergence : float
        The estimated KL divergence between two discrete features.

    """

    target_frequent_items = target_distribution.frequent_items
    target_unique_count = target_distribution.unique_count.estimate
    target_total_count = target_distribution.total_count

    ref_frequent_items = reference_distribution.frequent_items
    ref_unique_count = reference_distribution.unique_count.estimate
    ref_total_count = reference_distribution.total_count

    if any([c <= 0 for c in (target_total_count, ref_total_count)]):
        return None

    ref_freq_items_map = {}
    for item in ref_frequent_items.items:
        ref_freq_items_map[item.json_value] = item.estimate

    kl_divergence = 0
    for item in target_frequent_items.items:
        i_frequency = item.estimate / target_total_count
        if i_frequency == 0:
            continue
        ref_frequency = ref_freq_items_map[item.json_value] / ref_total_count if item.json_value in ref_freq_items_map.keys() else 0
        if ref_frequency == 0:
            kl_divergence = np.inf
            break
        kl_divergence += i_frequency * np.log(i_frequency / ref_frequency)

    target_frequent_items_count = len(target_frequent_items.items)
    target_n_singles = target_unique_count - target_frequent_items_count

    ref_freq_items_count = len(ref_frequent_items.items)
    ref_n_singles = ref_unique_count - ref_freq_items_count
    if math.isclose(target_n_singles, 0.0, abs_tol=10e-3):
        return type("Object", (), {"kl_divergence": kl_divergence})

    target_n_singles_frequency = target_n_singles / target_frequent_items_count
    ref_n_singles_frequency = ref_n_singles / ref_freq_items_count
    kl_divergence += target_n_singles_frequency * np.log(target_n_singles_frequency / ref_n_singles_frequency)
    return type("Object", (), {"kl_divergence": kl_divergence})


def compute_chi_squared_test_p_value(target_distribution: ReferenceDistributionDiscreteMessage, reference_distribution: ReferenceDistributionDiscreteMessage):
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
    target_freq_items = target_distribution.frequent_items
    target_total_count = target_distribution.total_count
    target_unique_count = target_distribution.unique_count.estimate
    ref_total_count = reference_distribution.total_count

    if ref_total_count <= 0 or target_total_count <= 0:
        return None

    ref_dist_items = dict()
    for item in reference_distribution.frequent_items.items:
        ref_dist_items[item.json_value] = item.estimate

    proportion_ref_dist_items = {k: v / ref_total_count for k, v in ref_dist_items.items()}

    chi_sq = 0
    for item in target_freq_items.items:
        target_frequency = item.estimate
        if item.json_value in ref_dist_items:
            ref_frequency = int(proportion_ref_dist_items[item.json_value] * target_total_count)
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
