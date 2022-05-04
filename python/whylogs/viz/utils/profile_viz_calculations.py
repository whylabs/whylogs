from logging import getLogger
from typing import Any, Dict, List, Optional, Tuple, Union

from typing_extensions import TypedDict
from whylogs_sketching import kll_doubles_sketch  # type: ignore

from whylogs.core.metrics import CardinalityMetric, DistributionMetric
from whylogs.core.metrics.metrics import FrequentItem
from whylogs.core.view.column_profile_view import ColumnProfileView
from whylogs.viz.utils import (
    DescriptiveStatistics,
    QuantileStats,
    _calculate_bins,
    _calculate_descriptive_statistics,
    _calculate_quantile_statistics,
)
from whylogs.viz.utils.descriptive_stats import _get_count_metrics_from_column_view

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
    descriptive_statistics: Optional[DescriptiveStatistics]


def _get_distribution_metrics(
    column_view: ColumnProfileView,
) -> Union[Tuple[None, None, None], Tuple[float, float, float]]:
    distribution_metric: Optional[DistributionMetric] = column_view.get_metric("dist")
    if distribution_metric is None:
        return None, None, None

    min_val = distribution_metric.kll.value.get_min_value()
    max_val = distribution_metric.kll.value.get_max_value()
    range_val = max_val - min_val
    return min_val, max_val, range_val


def _get_cardinality_metrics_from_column_view(
    column_view: ColumnProfileView, count_n: Optional[float] = None, count_missing: Optional[float] = None
) -> Union[None, float]:
    cardinality: Optional[CardinalityMetric] = column_view.get_metric("card")
    if cardinality and count_n and count_missing:
        card_estimate = cardinality.hll.value.get_estimate()
        distinct = card_estimate / (count_n - count_missing) * 100
        return distinct
    else:
        return None


def add_feature_statistics(
    feature_name: str,
    column_view: Union[None, ColumnProfileView],
) -> Dict[str, FeatureStats]:
    if column_view is None:
        return {}

    feature_with_statistics: Dict[str, FeatureStats] = {}

    count_n, count_missing = _get_count_metrics_from_column_view(column_view=column_view)
    distinct = _get_cardinality_metrics_from_column_view(
        column_view=column_view, count_n=count_n, count_missing=count_missing
    )
    min_val, max_val, range_val = _get_distribution_metrics(column_view=column_view)

    quantile_stats = _calculate_quantile_statistics(column_view)
    desc_stats = _calculate_descriptive_statistics(column_view)

    feature_stats: FeatureStats = {
        "missing": count_missing,
        "distinct": distinct,
        "min": min_val,
        "max": max_val,
        "range": range_val,
        "quantile_statistics": quantile_stats,
        "descriptive_statistics": desc_stats,
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
