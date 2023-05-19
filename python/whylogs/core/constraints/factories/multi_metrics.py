from whylogs.core.configs import SummaryConfig
from whylogs.core.constraints import (
    DatasetConstraint,
    MetricConstraint,
    PrefixCondition,
)


def column_is_probably_unique(column_name: str, hll_stddev: int = 3) -> MetricConstraint:
    is_probably_unique_constraint = DatasetConstraint(
        name=f"{column_name} is probably unique",
        condition=PrefixCondition(
            f"and <= :{column_name}:cardinality/lower_{hll_stddev} - :{column_name}:counts/n :{column_name}:counts/null  <= - :{column_name}:counts/n :{column_name}:counts/null :{column_name}:cardinality/upper_{hll_stddev}",  # noqa: E501
            cfg=SummaryConfig(hll_stddev=hll_stddev),
        ),
    )
    return is_probably_unique_constraint
