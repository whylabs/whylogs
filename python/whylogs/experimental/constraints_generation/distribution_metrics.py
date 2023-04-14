from typing import List

from whylogs.core.constraints.factories import is_non_negative
from whylogs.core.constraints.metric_constraints import MetricConstraint
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
    if min is not None and min >= 0:
        constraints.append(is_non_negative(column_name))
    return constraints
