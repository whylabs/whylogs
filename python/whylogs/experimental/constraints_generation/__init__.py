from typing import List

from whylogs.core.constraints.metric_constraints import MetricConstraint
from whylogs.core.view.dataset_profile_view import DatasetProfileView
from whylogs.experimental.constraints_generation.condition_counts import (
    generate_column_condition_count_constraints,
)
from whylogs.experimental.constraints_generation.count_metrics import (
    generate_column_count_constraints,
)
from whylogs.experimental.constraints_generation.types_metrics import (
    generate_column_types_constraints,
)


def generate_constraints_from_reference_profile(reference_profile_view: DatasetProfileView) -> List[MetricConstraint]:
    """
    Generates constraints from a reference profile view.
    Parameters
    ----------
    reference_profile_view : DatasetProfileView
        Reference profile view
    """

    if not reference_profile_view:
        raise ValueError("Reference profile view is not set.")
    constraints = []
    reference_column_profiles = reference_profile_view.get_columns()
    for column_name, column_profile in reference_column_profiles.items():
        if "counts" in column_profile.get_metric_names():
            column_count_constraint = generate_column_count_constraints(column_name, column_profile)
            constraints.extend(column_count_constraint)
        if "types" in column_profile.get_metric_names():
            column_types_constraint = generate_column_types_constraints(column_name, column_profile)
            constraints.extend(column_types_constraint)
        if "condition_count" in column_profile.get_metric_names():
            column_condition_count_constraint = generate_column_condition_count_constraints(column_name, column_profile)
            constraints.extend(column_condition_count_constraint)
    return constraints


__ALL__ = [
    generate_column_count_constraints,
    generate_constraints_from_reference_profile,
    generate_column_condition_count_constraints,
    generate_column_types_constraints,
]
