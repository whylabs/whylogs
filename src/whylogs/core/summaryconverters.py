"""
Library module defining function for generating summaries
"""
import math
from typing import Union

import numpy as np
import scipy.special
from datasketches import (
    frequent_items_error_type,
    frequent_strings_sketch,
    kll_floats_sketch,
    update_theta_sketch,
)

from whylogs.proto import (
    FrequentStringsSummary,
    HistogramSummary,
    QuantileSummary,
    ReferenceDistributionDiscreteMessage,
    UniqueCountSummary,
)

MAX_HIST_BUCKETS = 30
HIST_AVG_NUMBER_PER_BUCKET = 4.0
QUANTILES = [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0]


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
        # Include the max value in the right-most bin
        end += abs(end) * 1e-7
        # Include the right edge in the bin edges
        n_buckets = min(math.ceil(n / avg_per_bucket), max_buckets)
        width = (end - start) / n_buckets
        # Calculate histograms from the Probability Mass Function
        bins = [start + i * width for i in range(n_buckets + 1)]
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


def ks_test_compute_p_value(target_distribution: kll_floats_sketch, reference_distribution: kll_floats_sketch):
    D_max = 0
    quantile_values = reference_distribution.get_quantiles(QUANTILES)
    for quant in quantile_values:
        cdf_target = target_distribution.get_cdf([quant])[0]
        cdf_ref = reference_distribution.get_cdf([quant])[0]
        D = abs(cdf_target - cdf_ref)
        if D > D_max:
            D_max = D
    n_samples = min(target_distribution.get_n(), reference_distribution.get_n())
    p_value = scipy.special.kolmogorov(np.sqrt(n_samples) * D_max)
    return type("Object", (), {"ks_test": p_value})


def compute_kl_divergence(
    target_distribution: Union[kll_floats_sketch, ReferenceDistributionDiscreteMessage],
    reference_distribution: Union[kll_floats_sketch, ReferenceDistributionDiscreteMessage],
):
    if isinstance(target_distribution, kll_floats_sketch) and isinstance(reference_distribution, kll_floats_sketch):
        return _compute_kl_divergence_continuous_distributions(target_distribution, reference_distribution)
    elif all([isinstance(v, ReferenceDistributionDiscreteMessage) for v in (target_distribution, reference_distribution)]):
        return _compute_kl_divergence_discrete_distributions(target_distribution, reference_distribution)
    else:
        raise ValueError("Both provided distributions should be categorical or numeric, but not from mixed type")


def _compute_kl_divergence_continuous_distributions(target_distribution: kll_floats_sketch, reference_distribution: kll_floats_sketch):
    bins_target = np.linspace(target_distribution.get_min_value(), target_distribution.get_max_value(), 100)
    pmf_target = np.array(target_distribution.get_pmf(bins_target))

    pmf_reference = np.array(reference_distribution.get_pmf(bins_target))

    kl_divergence = np.sum(np.where(pmf_target != 0, pmf_target * np.log(pmf_target / pmf_reference), 0))
    return type("Object", (), {"kl_divergence": kl_divergence})


def _compute_kl_divergence_discrete_distributions(
    target_distribution: ReferenceDistributionDiscreteMessage, reference_distribution: ReferenceDistributionDiscreteMessage
):
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
