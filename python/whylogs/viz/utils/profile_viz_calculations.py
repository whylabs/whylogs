import json
from logging import getLogger
from typing import Any, Dict, List, Optional, Union

from typing_extensions import TypedDict

from whylogs.core.configs import SummaryConfig
from whylogs.core.metrics import CardinalityMetric
from whylogs.core.utils import get_distribution_metrics
from whylogs.core.view.column_profile_view import ColumnProfileView
from whylogs.core.view.dataset_profile_view import DatasetProfileView
from whylogs.extras.image_metric import ImageMetric
from whylogs.viz.utils import (
    DescriptiveStatistics,
    QuantileStats,
    _calculate_descriptive_statistics,
    _calculate_quantile_statistics,
)
from whylogs.viz.utils.descriptive_stats import _get_count_metrics_from_column_view
from whylogs.viz.utils.drift_calculations import calculate_drift_values
from whylogs.viz.utils.frequent_items_calculations import (
    FrequentItemEstimate,
    frequent_items_from_view,
)
from whylogs.viz.utils.histogram_calculations import (
    HistogramSummary,
    histogram_from_view,
)

logger = getLogger(__name__)


class FeatureStats(TypedDict):
    total_count: Optional[float]
    missing: Optional[float]
    distinct: Optional[float]
    min: Optional[float]
    max: Optional[float]
    range: Optional[float]
    quantile_statistics: Optional[QuantileStats]
    descriptive_statistics: Optional[DescriptiveStatistics]


def _get_cardinality_metrics_from_column_view(
    column_view: ColumnProfileView, count_n: Optional[float] = None, count_missing: Optional[float] = None
) -> Union[None, float]:
    cardinality: Optional[CardinalityMetric] = column_view.get_metric("cardinality")
    if cardinality is not None and count_n is not None and count_missing is not None:
        card_estimate = cardinality.hll.value.get_estimate()
        if (count_n - count_missing) != 0:
            distinct = card_estimate / (count_n - count_missing) * 100
            return distinct
    return None


def add_feature_statistics(
    feature_name: str,
    column_view: Union[None, ColumnProfileView],
) -> Dict[str, FeatureStats]:
    if column_view is None:
        return {}

    feature_with_statistics: Dict[str, FeatureStats] = {}

    total_count, count_n, count_missing = _get_count_metrics_from_column_view(column_view=column_view)
    distinct = _get_cardinality_metrics_from_column_view(
        column_view=column_view, count_n=count_n, count_missing=count_missing
    )
    min_val, max_val, range_val = get_distribution_metrics(column_view=column_view)

    quantile_stats = _calculate_quantile_statistics(column_view)
    desc_stats = _calculate_descriptive_statistics(column_view)

    feature_stats: FeatureStats = {
        "total_count": total_count,
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


class OverallStats(TypedDict):
    observations: int
    missing_cells: int
    missing_percentage: float


class ColumnSummary(TypedDict):
    histogram: Optional[HistogramSummary]
    frequentItems: Optional[List[FrequentItemEstimate]]
    drift_from_ref: Optional[float]
    isDiscrete: Optional[bool]
    featureStats: Optional[FeatureStats]


class DatasetSummary(TypedDict):
    columns: Dict[str, Optional[ColumnSummary]]
    properties: Optional[OverallStats]


def generate_profile_summary(
    target_view: DatasetProfileView, config: Optional[SummaryConfig]
) -> Optional[Dict[str, Any]]:
    if config is None:
        config = SummaryConfig()

    if not target_view:
        raise ValueError("This method has to get target Dataset Profile View")

    overall_stats: OverallStats = add_overall_statistics(target_view)
    target_col_views = target_view.get_columns()
    target_summary: DatasetSummary = {"columns": {}, "properties": overall_stats}

    for target_col_name in target_col_views:
        target_column_summary: ColumnSummary = {
            "histogram": None,
            "frequentItems": None,
            "drift_from_ref": None,
            "isDiscrete": None,
            "featureStats": None,
        }

        target_col_view = target_col_views[target_col_name]
        target_stats = add_feature_statistics(target_col_name, target_col_view)
        target_column_summary["featureStats"] = target_stats[target_col_name]
        target_dist = target_col_view.get_metric("distribution")

        if target_dist and not target_dist.kll.value.is_empty():
            target_column_summary["isDiscrete"] = False
            target_histogram = histogram_from_view(target_col_view, target_col_name)
            target_column_summary["histogram"] = target_histogram
        elif target_col_view.get_metric("frequent_items"):
            target_column_summary["isDiscrete"] = True
            target_frequent_items = frequent_items_from_view(target_col_view, target_col_name, config)
            target_column_summary["frequentItems"] = target_frequent_items

        target_summary["columns"][target_col_name] = target_column_summary

    summaries = {
        "profile_from_whylogs": json.dumps(target_summary),
    }
    return summaries


def is_image_compound_metric(col_view: ColumnProfileView) -> bool:
    if isinstance(col_view.get_metric("image"), ImageMetric):
        return True
    return False


def generate_summaries(
    target_view: DatasetProfileView, ref_view: Optional[DatasetProfileView], config: Optional[SummaryConfig]
) -> Optional[Dict[str, Any]]:
    if config is None:
        config = SummaryConfig()

    if not target_view or not ref_view:
        raise ValueError("This method has to get both target and reference profiles")

    overall_stats: OverallStats = add_overall_statistics(target_view)
    drift_values = calculate_drift_values(target_view, ref_view)

    target_col_views = target_view.get_columns()
    ref_col_views = ref_view.get_columns()
    ref_summary: DatasetSummary = {"columns": {}, "properties": overall_stats}
    target_summary: DatasetSummary = {"columns": {}, "properties": None}
    for target_col_name in target_col_views:
        if target_col_name in ref_col_views and not is_image_compound_metric(target_col_views[target_col_name]):
            target_column_summary: ColumnSummary = {
                "histogram": None,
                "frequentItems": None,
                "drift_from_ref": None,
                "isDiscrete": None,
                "featureStats": None,
            }
            ref_column_summary: ColumnSummary = {
                "histogram": None,
                "frequentItems": None,
                "drift_from_ref": None,
                "isDiscrete": None,
                "featureStats": None,
            }

            target_col_view = target_col_views[target_col_name]
            ref_col_view = ref_col_views[target_col_name]
            if not target_col_view or not ref_col_view:
                continue

            target_stats = add_feature_statistics(target_col_name, target_col_view)
            target_column_summary["featureStats"] = target_stats[target_col_name]

            if target_col_name in drift_values:
                col_drift_value = drift_values[target_col_name]
                if col_drift_value:
                    ref_column_summary["drift_from_ref"] = col_drift_value["p_value"]
            target_dist = target_col_view.get_metric("distribution")
            reference_dist = ref_col_view.get_metric("distribution")
            if (
                target_dist
                and reference_dist
                and not target_dist.kll.value.is_empty()
                and not reference_dist.kll.value.is_empty()
            ):
                target_column_summary["isDiscrete"] = ref_column_summary["isDiscrete"] = False

                target_histogram = histogram_from_view(target_col_view, target_col_name)
                target_column_summary["histogram"] = target_histogram

                ref_histogram = histogram_from_view(ref_col_view, target_col_name)
                ref_column_summary["histogram"] = ref_histogram

            elif target_col_view.get_metric("frequent_items") and ref_col_view.get_metric("frequent_items"):
                target_column_summary["isDiscrete"] = ref_column_summary["isDiscrete"] = True

                target_frequent_items = frequent_items_from_view(target_col_view, target_col_name, config)
                target_column_summary["frequentItems"] = target_frequent_items

                ref_frequent_items = frequent_items_from_view(ref_col_view, target_col_name, config)
                ref_column_summary["frequentItems"] = ref_frequent_items
            target_summary["columns"][target_col_name] = target_column_summary
            ref_summary["columns"][target_col_name] = ref_column_summary

    summaries = {
        "profile_from_whylogs": json.dumps(target_summary),
        "reference_profile_from_whylogs": json.dumps(ref_summary),
    }
    return summaries


def add_overall_statistics(target_view: DatasetProfileView) -> OverallStats:
    observations: int = 0
    missing_cells: int = 0
    missing_percentage: float = 0
    target_col_views = target_view.get_columns()
    for target_col_view in target_col_views.values():
        if target_col_view:
            cnt_metric = target_col_view.get_metric("counts")
            if cnt_metric:
                observations += cnt_metric.n.value
                null_count = cnt_metric.null.value
                missing_cells += null_count if null_count else 0
    missing_percentage = (missing_cells / observations) * 100 if observations else 0
    overall_stats: OverallStats = {
        "observations": observations,
        "missing_cells": missing_cells,
        "missing_percentage": missing_percentage,
    }
    return overall_stats
