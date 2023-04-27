from ..metric_constraints import MetricConstraint, MetricsSelector
from whylogs.core.constraints.metric_constraints import ConstraintsParams


class ConditionMeetsParams(ConstraintsParams):
    condition_name: str


def condition_meets(column_name: str, condition_name: str) -> MetricConstraint:
    """Checks that all values in column match predicate

    Parameters
    ----------
    column_name : str
        Name of the column to apply the constraint to
    condition_name : str
        Name of the condition that will be applied to each value of the column
    """

    def matches_all(x) -> bool:
        if x.matches[condition_name].value != x.total.value:
            return False
        return True

    constraint = MetricConstraint(
        name=f"{column_name} meets condition {condition_name}",
        condition=matches_all,
        metric_selector=MetricsSelector(column_name=column_name, metric_name="condition_count"),
    )
    constraint._set_params(ConditionMeetsParams(factory="condition_meets", condition_name=condition_name))
    return constraint


def condition_never_meets(column_name: str, condition_name: str) -> MetricConstraint:
    """Checks that no values in column match predicate

    Parameters
    ----------
    column_name : str
        Name of the column to apply the constraint to
    condition_name : str
        Name of the condition that will be applied to each value of the column
    """

    def matches_none(x) -> bool:
        if x.matches[condition_name].value != 0:
            return False
        return True

    constraint = MetricConstraint(
        name=f"{column_name} never meets condition {condition_name}",
        condition=matches_none,
        metric_selector=MetricsSelector(column_name=column_name, metric_name="condition_count"),
    )
    return constraint
