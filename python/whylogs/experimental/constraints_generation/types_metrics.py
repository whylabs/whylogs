from typing import List

from whylogs.core.constraints.factories import column_has_zero_count_types
from whylogs.core.constraints.metric_constraints import MetricConstraint
from whylogs.core.view.column_profile_view import ColumnProfileView


def generate_column_types_constraints(column_name: str, column_profile: ColumnProfileView) -> List[MetricConstraint]:
    """
    Generates constraints for the types metrics of a column.
    Parameters
    ----------
    column_name : str
        Name of the column
    column_profile : ColumnProfileView
        Profile of the column
    """
    constraints = []
    types_metric = column_profile.get_metric("types")
    zero_empty_types = []
    for key, component in vars(types_metric).items():
        if component.value == 0:
            zero_empty_types.append(key)
    if zero_empty_types:
        constraints.append(column_has_zero_count_types(column_name=column_name, types_list=zero_empty_types))
    return constraints
