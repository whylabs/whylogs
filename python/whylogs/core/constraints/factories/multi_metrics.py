from whylogs.core.constraints import (
    DatasetConstraint,
    MetricConstraint,
    PrefixCondition,
)


def column_is_probably_unique(column_name: str) -> MetricConstraint:
    is_probably_unique_constraint = DatasetConstraint(
        name=f"{column_name} is probably unique",
        condition=PrefixCondition(
            f"and <= :{column_name}:cardinality/lower_1 :{column_name}:counts/n <= :{column_name}:counts/n :{column_name}:cardinality/upper_1"
        ),
    )
    return is_probably_unique_constraint
