from typing import List

from whylogs.core import CardinalityThresholds
from whylogs.core.constraints.factories import frequent_strings_in_reference_set
from whylogs.core.constraints.metric_constraints import MetricConstraint
from whylogs.core.utils import get_cardinality_estimate
from whylogs.core.view.column_profile_view import ColumnProfileView


def generate_column_frequent_items_constraints(
    column_name: str, column_profile: ColumnProfileView
) -> List[MetricConstraint]:
    constraints = []
    fi_list = [fi.value for fi in column_profile.get_metric("frequent_items").to_summary_dict()["frequent_strings"]]
    cardinality_estimate = get_cardinality_estimate(column_profile)
    if (
        0 < cardinality_estimate.get("est") <= CardinalityThresholds.few
        and cardinality_estimate.get("unique_pct") < CardinalityThresholds.proportionately_few
    ):
        fi_set = set(fi_list)
        constraints.append(frequent_strings_in_reference_set(column_name, fi_set))
    return constraints
