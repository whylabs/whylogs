import math
from whylabs.logs.proto import UniqueCountSummary, \
    FrequentStringsSummary, HistogramSummary
from datasketches import update_theta_sketch, frequent_strings_sketch, \
    frequent_items_error_type, kll_floats_sketch

MAX_HIST_BUCKETS = 100
HIST_AVG_NUMBER_PER_BUCKET = 4.0


def from_sketch(sketch: update_theta_sketch, num_std_devs: float=1):
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
        lower=sketch.get_lower_bound(num_std_devs)
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
    frequent_items = sketch.get_frequent_items(
        frequent_items_error_type.NO_FALSE_NEGATIVES)
    # Note: frequent items is a list of tuples containing info about the
    # most frequent strings and their count:
    # [(string, est_count, lower bound, upper bound)]
    if len(frequent_items) == 0:
        return

    items = [{'value': x[0], 'estimate': x[1]} for x in frequent_items]
    return FrequentStringsSummary(items=items)


def from_kll_floats_sketch(sketch: kll_floats_sketch):
    """
    Generate a summary of a kll_floats_sketch, including a histogram


    """
    n = sketch.get_n()
    start = sketch.get_min_value()
    max_val = sketch.get_max_value()
    end = max_val

    if (n < 2) or (start == end):
        dx = abs(start) * 1e-7
        end = start + dx
        bins = [start, end]
        counts = [n]
    else:
        # Include the max value in the right-most bin
        end += abs(end) * (1e-7)
        # Include the right edge in the bin edges
        n_buckets = min(math.ceil(n / HIST_AVG_NUMBER_PER_BUCKET),
                        MAX_HIST_BUCKETS)
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
        n=n
    )
