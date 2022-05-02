from logging import getLogger
from typing import Any, Dict, Optional, List

from typing_extensions import TypedDict
from whylogs_sketching import kll_doubles_sketch  # type: ignore

from whylogs.core.metrics import (
    CardinalityMetric,
    ColumnCountsMetric,
    DistributionMetric
)
from whylogs.core.metrics.metrics import FrequentItem
from whylogs.core.view.column_profile_view import ColumnProfileView
from whylogs.viz.utils import *

MAX_HIST_BUCKETS = 30
HIST_AVG_NUMBER_PER_BUCKET = 4.0

logger = getLogger(__name__)


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
        bins, end = _calculate_bins(end, start, n, avg_per_bucket, max_buckets)
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


class FrequentItemEstimate(TypedDict):
    value: str
    estimate: float


def get_frequent_items_estimate(frequent_items: List[FrequentItem]) -> List[FrequentItemEstimate]:
    return [{"value": x.value, "estimate": x.est} for x in frequent_items]
