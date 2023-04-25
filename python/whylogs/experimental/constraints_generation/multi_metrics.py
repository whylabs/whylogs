from typing import List

from whylogs.core.constraints.factories import column_is_probably_unique
from whylogs.core.constraints.metric_constraints import MetricConstraint
from whylogs.core.utils import is_probably_unique
from whylogs.core.utils.stats_calculations import only_null_values
from whylogs.core.view.column_profile_view import ColumnProfileView


def generate_column_multi_metrics_constraints(
    column_name: str, column_profile: ColumnProfileView
) -> List[MetricConstraint]:
    constraints = []
    if is_probably_unique(column_profile) and not only_null_values(column_profile):
        constraints.append(column_is_probably_unique(column_name))
    return constraints
