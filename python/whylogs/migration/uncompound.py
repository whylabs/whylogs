import logging
from typing import Dict

from whylogs.core import ColumnProfileView, DatasetProfileView
from whylogs.core.metrics import Metric
from whylogs.core.metrics.compound_metric import CompoundMetric
from whylogs.core.metrics.multimetric import MultiMetric

try:
    from PIL.Image import Image as ImageType  # noqa trigger ImportError if unavailable

    from whylogs.extras.image_metric import ImageMetric
except ImportError:
    # dummy ImageMetric class so nothing will be an instance of it
    class ImageMetric:  # type: ignore[no-redef]
        pass


logger = logging.getLogger(__name__)


def _uncompound_metric_feature_flag() -> bool:
    """
    v0 whylabs doesn't understand compound metrics. If this is True, turn
    each submetric in a compound metric into its own column in the profile
    so that v0 whylabs will only see metrics it understands.
    """
    return True


def _v0_compatible_image_feature_flag() -> bool:
    """
    v0 whylogs only supported logging a single image in a profile.
    v1 supports multiple image in a profile by giving each image "channel"
    a unique name. If this returns True, use the old V0 column naming
    convention that only supports a single image.
    """
    return False


def _uncompounded_column_name(column_name: str, metric_name: str, submetric_name: str, metric: Metric) -> str:
    if isinstance(metric, ImageMetric) and _v0_compatible_image_feature_flag():
        return submetric_name
    return f"{column_name}.{metric_name}.{submetric_name}"


def _uncompound_metric(col_name: str, metric_name: str, metric: CompoundMetric) -> Dict[str, ColumnProfileView]:
    result: Dict[str, ColumnProfileView] = dict()
    for submetric_name, submetric in metric.submetrics.items():
        new_col_name = _uncompounded_column_name(col_name, metric_name, submetric_name, metric)
        result[new_col_name] = ColumnProfileView({submetric.namespace: submetric})
    return result


def _uncompounded_multimetric_column_name(column_name: str, submetric_name: str, metric: Metric) -> str:
    if isinstance(metric, ImageMetric) and _v0_compatible_image_feature_flag():
        return submetric_name
    return f"{column_name}.{submetric_name}"


def _uncompound_multimetric(col_name: str, metric_name: str, metric: MultiMetric) -> Dict[str, ColumnProfileView]:
    result: Dict[str, ColumnProfileView] = dict()
    for submetric_name, submetrics in metric.submetrics.items():
        new_col_name = _uncompounded_multimetric_column_name(col_name, submetric_name, metric)
        result[new_col_name] = ColumnProfileView(submetrics)

    return result


def _uncompound_dataset_profile(prof: DatasetProfileView) -> DatasetProfileView:
    """
    v0 whylabs doesn't understand compound metrics. This creates a new column for
    each submetric in a compound metric so that whylabs only sees metrics it understands.
    """
    new_prof = DatasetProfileView(
        columns=prof._columns,
        dataset_timestamp=prof._dataset_timestamp,
        creation_timestamp=prof._creation_timestamp,
        metrics=prof._metrics,
    )
    new_columns: Dict[str, ColumnProfileView] = dict()
    for col_name, col_prof in new_prof._columns.items():
        for metric_name, metric in col_prof._metrics.items():
            if isinstance(metric, CompoundMetric):
                new_columns.update(_uncompound_metric(col_name, metric_name, metric))
            if isinstance(metric, MultiMetric):
                new_columns.update(_uncompound_multimetric(col_name, metric_name, metric))

    new_prof._columns.update(new_columns)
    return new_prof
