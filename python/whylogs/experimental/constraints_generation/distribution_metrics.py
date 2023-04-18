from math import isnan
from typing import List

from whylogs.core import CardinalityThresholds
from whylogs.core.constraints.factories import is_in_range, is_non_negative
from whylogs.core.constraints.metric_constraints import MetricConstraint
from whylogs.core.utils import (
    get_cardinality_estimate,
    get_distribution_metrics,
    is_probably_unique,
)
from whylogs.core.view.column_profile_view import ColumnProfileView


def generate_column_distribution_constraints(
    column_name: str, column_profile: ColumnProfileView
) -> List[MetricConstraint]:
    """Generates constraints for the count metrics of a column.
    Parameters
    ----------
    column_name : str
        Name of the column
    column_profile : ColumnProfileView
        Profile of the column
    """
    constraints = []
    min = column_profile.get_metric("distribution").to_summary_dict()["min"]
    cardinality_estimate = get_cardinality_estimate(column_profile)
    if min is not None and min >= 0:
        constraints.append(is_non_negative(column_name))
    if (
        not is_probably_unique(column_profile)
        and cardinality_estimate.get("est") is not None
        and cardinality_estimate.get("est") > CardinalityThresholds.few
    ):
        dist_stats = get_distribution_metrics(column_profile)
        min_val, max_val = dist_stats[0], dist_stats[1]
        if min_val is not None and max_val is not None and not isnan(min_val) and not isnan(max_val):
            constraints.append(is_in_range(column_name, min_val, max_val))

    return constraints
