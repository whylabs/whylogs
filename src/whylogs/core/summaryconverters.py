"""
Library module defining function for generating summaries
"""
import math

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
