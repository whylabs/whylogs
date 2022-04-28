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

MAX_HIST_BUCKETS = 30
HIST_AVG_NUMBER_PER_BUCKET = 4.0

logger = getLogger(__name__)


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

    descriptive_statistics: DescStats = {
        "stddev": stddev,
        "coefficient_of_variation": stddev / mean,
        "sum": (count_n - count_missing) * mean,
        "variance": distribution_metric.variance,
        "mean": mean,
    }

    return descriptive_statistics


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
