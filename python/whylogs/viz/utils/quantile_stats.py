import math
from logging import getLogger
from typing import List, Optional, Tuple, Union

import numpy as np
from typing_extensions import TypedDict

from whylogs.core.metrics import DistributionMetric
from whylogs.core.view.column_profile_view import ColumnProfileView

logger = getLogger(__name__)

# the kll_floats_sketch use 32bit floats, so we check precision against np.float32
FLOAT_MANTISSA_BITS = np.finfo(np.float32).nmant


class QuantileStats(TypedDict):
    iqr: float
    q1: float
    q3: float
    ninety_fifth_percentile: float
    fifth_percentile: float
    median: float


def _calculate_quantile_statistics(column_view: Union[ColumnProfileView, None]) -> Union[None, QuantileStats]:
    if column_view is None:
        return None

    distribution_metric: Optional[DistributionMetric] = column_view.get_metric("distribution")
    desired_quantiles = [0.05, 0.25, 0.5, 0.75, 0.95]

    if distribution_metric is None or distribution_metric.kll.value.is_empty():
        return None

    quantiles = distribution_metric.kll.value.get_quantiles(desired_quantiles)

    quantile_statistics: QuantileStats = {
        "fifth_percentile": quantiles[0],
        "iqr": quantiles[3] - quantiles[1],
        "q1": quantiles[1],
        "median": quantiles[2],
        "q3": quantiles[3],
        "ninety_fifth_percentile": quantiles[4],
    }
    return quantile_statistics


def _resize_bins(start: float, end: float, min_interval: float, width: float, n_buckets: int) -> Tuple[int, float]:
    new_buckets = math.floor((end - start) / min_interval)
    new_buckets = max(new_buckets, 1)
    logger.warning(
        f"A bin width of {width} won't work with values in range of [{start}, {end}] "
        f"because numbers closer to each other than {int(min_interval)} might not be distinct "
        "when passed as float32: avoiding bin edge collisions by resizing from: "
        f"{n_buckets} to: {new_buckets} histogram buckets in summary."
    )

    width = (end - start) / new_buckets
    logger.info(f"New bin width is: {width} across {new_buckets} buckets")
    return new_buckets, width


def _get_min_interval(abs_start: float, abs_end: float) -> float:
    """
    Figure out the floating point precision at the scale of the bin boundaries
    min_interval is the smallest difference between floats at this scale.
    """
    max_magnitude = max(abs_start, abs_end)
    log_min_interval = math.floor(math.log2(max_magnitude)) - FLOAT_MANTISSA_BITS
    min_interval = math.pow(2, log_min_interval)

    return min_interval


def _calculate_bins(
    end: float, start: float, n: int, avg_per_bucket: float, max_buckets: int, min_n_buckets: int = 2
) -> Tuple[List[float], float]:
    # Include the max value in the right-most bin
    end += abs(end) * 1e-7 if end != 0 else 1e-7
    abs_end = abs(end)
    abs_start = abs(start)

    # Include the right edge in the bin edges
    n_buckets = min(math.ceil(n / avg_per_bucket), max_buckets)
    n_buckets = max(n_buckets, min_n_buckets)
    width = (end - start) / n_buckets

    min_interval = _get_min_interval(abs_start, abs_end)

    # If the bin width is smaller than min_interval, we need bigger bins
    if width < min_interval:
        n_buckets, width = _resize_bins(start, end, min_interval, width, n_buckets)

    # Calculate histograms from the Probability Mass Function
    bins = [start + i * width for i in range(n_buckets + 1)]
    logger.debug(f"about to get pmf using start: {start} end:{end} width:{width} and n_buckets:{n_buckets}")
    logger.debug(f"bin: {bins}")
    return bins, end
