from ..metric_constraints import MetricConstraint, MetricsSelector


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


def condition_count_below(column_name: str, condition_name: str, max_count: int) -> MetricConstraint:
    def is_count_below(x) -> bool:
        return max_count >= x.matches[condition_name].value

    constraint = MetricConstraint(
        name=f"{column_name}.{condition_name} lower than or equal to {max_count}",
        condition=is_count_below,
        metric_selector=MetricsSelector(column_name=column_name, metric_name="condition_count"),
    )
    return constraint
