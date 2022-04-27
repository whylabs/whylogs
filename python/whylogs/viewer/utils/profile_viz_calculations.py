import math
from logging import getLogger
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from whylogs_sketching import kll_doubles_sketch  # type: ignore

from whylogs.core.metrics import (
    CardinalityMetric,
    ColumnCountsMetric,
    DistributionMetric,
)
from whylogs.core.view.column_profile_view import ColumnProfileView

MAX_HIST_BUCKETS = 30
HIST_AVG_NUMBER_PER_BUCKET = 4.0

logger = getLogger(__name__)


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
