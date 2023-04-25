from typing import List

from whylogs.core.constraints.factories import condition_meets, condition_never_meets
from whylogs.core.constraints.metric_constraints import MetricConstraint
from whylogs.core.view.column_profile_view import ColumnProfileView


def generate_column_condition_count_constraints(
    column_name: str, column_profile: ColumnProfileView
) -> List[MetricConstraint]:
    """Generates constraints for the condition count metrics of a column.
    Parameters
    ----------
    column_name : str
        Name of the column
    column_profile : ColumnProfileView
        Profile of the column
    """
    constraints = []
    condition_count_metric = column_profile.get_metric("condition_count")
    for condition, matches in condition_count_metric.matches.items():
        if matches.value == condition_count_metric.total.value:
            constraints.append(condition_meets(column_name=column_name, condition_name=condition))
        if matches.value == 0:
            constraints.append(condition_never_meets(column_name=column_name, condition_name=condition))
    return constraints
