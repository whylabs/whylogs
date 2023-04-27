from whylogs.core.constraints import (
    DatasetConstraint,
    MetricConstraint,
    PrefixCondition,
)
from whylogs.core.constraints.metric_constraints import ConstraintsParams


class ColumnIsProbablyUniqueParams(ConstraintsParams):
    pass


def column_is_probably_unique(column_name: str) -> MetricConstraint:
    constraint_name = f"{column_name} is probably unique"
    constraint_params = ColumnIsProbablyUniqueParams(
        column_name=column_name, name=constraint_name, factory="column_is_probably_unique", metric="multi-metric"
    )
    is_probably_unique_constraint = DatasetConstraint(
        name=constraint_name,
        condition=PrefixCondition(
            f"and <= :{column_name}:cardinality/lower_1 :{column_name}:counts/n <= :{column_name}:counts/n :{column_name}:cardinality/upper_1"
        ),
        _params=constraint_params,
    )
    return is_probably_unique_constraint
