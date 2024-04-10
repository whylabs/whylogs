import logging
from dataclasses import dataclass
from typing import Dict, Optional

from whylogs.core import ColumnProfileView, DatasetProfileView
from whylogs.core.metrics import Metric
from whylogs.core.metrics.column_metrics import ColumnCountsMetric, TypeCountersMetric
from whylogs.core.metrics.compound_metric import CompoundMetric
from whylogs.core.metrics.condition_count_metric import ConditionCountMetric
from whylogs.core.metrics.metric_components import IntegralComponent
from whylogs.core.metrics.multimetric import MultiMetric

try:
    from PIL.Image import Image as ImageType  # noqa trigger ImportError if unavailable

    from whylogs.extras.image_metric import ImageMetric
except ImportError:
    # dummy ImageMetric class so nothing will be an instance of it
    class ImageMetric:  # type: ignore[no-redef]
        pass


logger = logging.getLogger(__name__)


@dataclass
class FeatureFlags:
    uncompound_condition_count: bool = False


def _uncompound_metric_feature_flag() -> bool:
    """
    v0 whylabs doesn't understand compound metrics. If this is True, turn
    each submetric in a compound metric or MultiMetric into its own column
    in the profile so that v0 whylabs will only see metrics it understands.
    """
    return True


def _uncompound_condition_count_feature_flag(flags: Optional[FeatureFlags] = None) -> bool:
    """
    v0 whylabs doesn't understand condition count metrics. If this is True, turn
    each condition into its three ColumnCountMetric columns in the profile
    so that v0 whylabs will only see metrics it understands.
    """
    if flags is None:
        return False
    return flags.uncompound_condition_count


def _uncompound_performance_estimation_magic_string() -> str:
    """
    Column name prefix for performance estimation metrics
    """
    return "Ω.whylabs.output_performance_estimation."


def _uncompound_performance_estimation_feature_flag() -> bool:
    return True


def _v0_compatible_image_feature_flag() -> bool:
    """
    v0 whylogs only supported logging a single image in a profile.
    v1 supports multiple image in a profile by giving each image "channel"
    a unique name. If this returns True, use the old V0 column naming
    convention that only supports a single image.
    """
    return False


def _condition_count_magic_string() -> str:
    """
    Column name prefix for uncompounded ConditionCountMetric columns
    """
    return "Ω.whylabs.condition."


def _uncompounded_column_name(column_name: str, metric_name: str, submetric_name: str, metric: Metric) -> str:
    if isinstance(metric, ImageMetric) and _v0_compatible_image_feature_flag():
        return submetric_name
    return f"{column_name}.{metric_name}.{submetric_name}"


def _uncompound_metric(col_name: str, metric_name: str, metric: CompoundMetric) -> Dict[str, ColumnProfileView]:
    if not _uncompound_metric_feature_flag():
        return dict()

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
    if not _uncompound_metric_feature_flag():
        return dict()

    result: Dict[str, ColumnProfileView] = dict()
    for submetric_name, submetrics in metric.submetrics.items():
        new_col_name = _uncompounded_multimetric_column_name(col_name, submetric_name, metric)
        result[new_col_name] = ColumnProfileView(submetrics)

    return result


def _uncompound_condition_count(
    col_name: str, metric_name: str, metric: ConditionCountMetric, flags: Optional[FeatureFlags] = None
) -> Dict[str, ColumnProfileView]:
    if not _uncompound_condition_count_feature_flag(flags):
        return dict()

    result: Dict[str, ColumnProfileView] = dict()
    type_metric = TypeCountersMetric(
        integral=metric.total,  # total condition evaluations
        fractional=IntegralComponent(0),
        boolean=IntegralComponent(0),
        string=IntegralComponent(0),
        object=IntegralComponent(0),
        tensor=IntegralComponent(0),
    )
    for condition_name, count_component in metric.matches.items():
        new_col_name = f"{_condition_count_magic_string()}{col_name}.{condition_name}.total"
        new_metric = ColumnCountsMetric(
            n=metric.total,  # total condition evaluations
            null=IntegralComponent(0),  # unused
            nan=IntegralComponent(0),  # unused
            inf=IntegralComponent(0),  # unused
        )
        result[new_col_name] = ColumnProfileView(
            {
                ColumnCountsMetric.get_namespace(): new_metric,
                TypeCountersMetric.get_namespace(): type_metric,
            }
        )
        new_col_name = f"{_condition_count_magic_string()}{col_name}.{condition_name}.matches"
        new_metric = ColumnCountsMetric(
            n=count_component,  # count of evaluations that matched condition
            null=IntegralComponent(0),  # unused
            nan=IntegralComponent(0),  # unused
            inf=IntegralComponent(0),  # unused
        )
        result[new_col_name] = ColumnProfileView(
            {
                ColumnCountsMetric.get_namespace(): new_metric,
                TypeCountersMetric.get_namespace(): type_metric,
            }
        )
        new_col_name = f"{_condition_count_magic_string()}{col_name}.{condition_name}.non_matches"
        new_metric = ColumnCountsMetric(
            n=IntegralComponent(metric.total.value - count_component.value),  # evaluations that didn't match
            null=IntegralComponent(0),  # unused
            nan=IntegralComponent(0),  # unused
            inf=IntegralComponent(0),  # unused
        )
        result[new_col_name] = ColumnProfileView(
            {
                ColumnCountsMetric.get_namespace(): new_metric,
                TypeCountersMetric.get_namespace(): type_metric,
            }
        )

    return result


def _uncompound_dataset_profile(prof: DatasetProfileView, flags: Optional[FeatureFlags] = None) -> DatasetProfileView:
    """
    v0 whylabs doesn't understand compound metrics. This creates a new column for
    each submetric in a compound metric so that whylabs only sees metrics it understands.
    """
    new_prof = DatasetProfileView(
        columns=prof._columns,
        dataset_timestamp=prof._dataset_timestamp,
        creation_timestamp=prof._creation_timestamp,
        metrics=prof._metrics,
        metadata=prof._metadata,
    )
    new_columns: Dict[str, ColumnProfileView] = dict()
    for col_name, col_prof in new_prof._columns.items():
        for metric_name, metric in col_prof._metrics.items():
            if isinstance(metric, CompoundMetric):
                new_columns.update(_uncompound_metric(col_name, metric_name, metric))
            if isinstance(metric, MultiMetric):
                new_columns.update(_uncompound_multimetric(col_name, metric_name, metric))
            if isinstance(metric, ConditionCountMetric):
                new_columns.update(_uncompound_condition_count(col_name, metric_name, metric, flags))

    new_prof._columns.update(new_columns)
    return new_prof
