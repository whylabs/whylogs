from typing import List

from whylogs.core.constraints.factories import no_missing_values
from whylogs.core.constraints.metric_constraints import MetricConstraint
from whylogs.core.view.column_profile_view import ColumnProfileView


def generate_column_count_constraints(column_name: str, column_profile: ColumnProfileView) -> List[MetricConstraint]:
    """Generates constraints for the count metrics of a column.
    Parameters
    ----------
    column_name : str
        Name of the column
    column_profile : ColumnProfileView
        Profile of the column
    """
    constraints = []
    column_null_values = column_profile.get_metric("counts").null.value
    if column_null_values == 0:
        no_missing_values_constraint = no_missing_values(column_name)
        constraints.append(no_missing_values_constraint)
    return constraints
