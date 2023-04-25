from typing import List, Optional

from whylogs.core.constraints.metric_constraints import MetricConstraint
from whylogs.core.view.dataset_profile_view import DatasetProfileView
from whylogs.experimental.constraints_generation.condition_counts import (
    generate_column_condition_count_constraints,
)
from whylogs.experimental.constraints_generation.count_metrics import (
    generate_column_count_constraints,
)
from whylogs.experimental.constraints_generation.distribution_metrics import (
    generate_column_distribution_constraints,
)
from whylogs.experimental.constraints_generation.frequent_items import (
    generate_column_frequent_items_constraints,
)
from whylogs.experimental.constraints_generation.multi_metrics import (
    generate_column_multi_metrics_constraints,
)
from whylogs.experimental.constraints_generation.types_metrics import (
    generate_column_types_constraints,
)


def generate_constraints_from_reference_profile(
    reference_profile_view: DatasetProfileView,
    included_columns: Optional[List[str]] = None,
    excluded_columns: Optional[List[str]] = None,
) -> List[MetricConstraint]:
    """
    Generates constraints from a reference profile view.

    Parameters
    ----------
    reference_profile_view : DatasetProfileView
        Reference profile view
    included_columns : Optional[List[str]], optional
        List of columns to include, by default None
    excluded_columns : Optional[List[str]], optional
        List of columns to exclude, by default None

    Returns
    -------
    List[MetricConstraint]
        List of generated constraints
    """
    if included_columns is not None and excluded_columns is not None:
        raise ValueError("Only one of `included_columns` or `excluded_columns` can be specified.")

    if not reference_profile_view:
        raise ValueError("Reference profile view is not set.")
    constraints = []
    reference_column_profiles = reference_profile_view.get_columns()
    for column_name, column_profile in reference_column_profiles.items():
        if included_columns and column_name not in included_columns:
            continue
        if excluded_columns and column_name in excluded_columns:
            continue
        multi_metric_constraints = generate_column_multi_metrics_constraints(column_name, column_profile)
        constraints.extend(multi_metric_constraints)
        if "counts" in column_profile.get_metric_names():
            column_count_constraint = generate_column_count_constraints(column_name, column_profile)
            constraints.extend(column_count_constraint)
        if "types" in column_profile.get_metric_names():
            column_types_constraint = generate_column_types_constraints(column_name, column_profile)
            constraints.extend(column_types_constraint)
        if "condition_count" in column_profile.get_metric_names():
            column_condition_count_constraint = generate_column_condition_count_constraints(column_name, column_profile)
            constraints.extend(column_condition_count_constraint)
        if "distribution" in column_profile.get_metric_names():
            column_distribution_constraint = generate_column_distribution_constraints(column_name, column_profile)
            constraints.extend(column_distribution_constraint)
        if "frequent_items" in column_profile.get_metric_names():
            column_frequent_items_constraint = generate_column_frequent_items_constraints(column_name, column_profile)
            constraints.extend(column_frequent_items_constraint)

    return constraints


__ALL__ = [
    generate_column_count_constraints,
    generate_constraints_from_reference_profile,
    generate_column_condition_count_constraints,
    generate_column_types_constraints,
]
