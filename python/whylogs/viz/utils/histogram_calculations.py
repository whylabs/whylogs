from whylogs.core.view.column_profile_view import ColumnProfileView
from typing_extensions import TypedDict
from typing import List, Optional, Tuple
from whylogs.core.metrics import (
    DistributionMetric,
)
import math
from whylogs_sketching import kll_doubles_sketch  # type: ignore
from logging import getLogger
import numpy as np

logger = getLogger(__name__)

MAX_HIST_BUCKETS = 30
HIST_AVG_NUMBER_PER_BUCKET = 4.0


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
    col_dist: Optional[DistributionMetric] = column_view.get_metric("dist")
    if not col_dist:
        raise ValueError("Distribution Metrics not found for feature {}.".format(feature_name))

    target_kill = col_dist.kll.value
    target_histogram = _histogram_from_sketch(target_kill)
    return target_histogram


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


def _histogram_from_sketch(
    sketch: kll_doubles_sketch, max_buckets: int = None, avg_per_bucket: Optional[float] = None
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
