from logging import getLogger
from typing import List, Optional

from typing_extensions import TypedDict
from whylogs_sketching import kll_doubles_sketch  # type: ignore

from whylogs.core.metrics import DistributionMetric
from whylogs.core.view.column_profile_view import ColumnProfileView
from whylogs.viz.configs import HistogramConfig
from whylogs.viz.utils import _calculate_bins

logger = getLogger(__name__)


MAX_HIST_BUCKETS = HistogramConfig().max_hist_buckets
HIST_AVG_NUMBER_PER_BUCKET = HistogramConfig().hist_avg_number_per_bucket
MIN_N_BUCKETS = HistogramConfig().min_n_buckets


class HistogramSummary(TypedDict):
    start: float
    end: float
    width: float
    counts: List[int]
    max: float
    min: float
    bins: List[float]
    n: int


def histogram_from_view(column_view: ColumnProfileView, feature_name: str) -> HistogramSummary:
    col_dist: Optional[DistributionMetric] = column_view.get_metric("distribution")
    if not col_dist:
        raise ValueError("Distribution Metrics not found for feature {}.".format(feature_name))

    target_kill = col_dist.kll.value
    target_histogram = _histogram_from_sketch(target_kill)
    return target_histogram


def _histogram_from_sketch(
    sketch: kll_doubles_sketch,
    max_buckets: Optional[int] = None,
    avg_per_bucket: Optional[float] = None,
    min_n_buckets: Optional[int] = None,
) -> HistogramSummary:
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
    if min_n_buckets is None:
        min_n_buckets = MIN_N_BUCKETS

    if (n < 2) or (start == end):
        dx = abs(start) * 1e-7
        end = start + dx
        bins = [start, end]
        counts = [n]
    else:
        bins, end = _calculate_bins(end, start, n, avg_per_bucket, max_buckets, min_n_buckets)
        pmf = sketch.get_pmf(bins)
        counts = [round(p * n) for p in pmf]
        counts = counts[1:-1]

    histogram: HistogramSummary = {
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
