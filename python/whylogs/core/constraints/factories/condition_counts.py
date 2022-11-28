from ..metric_constraints import MetricConstraint, MetricsSelector


def condition_meets(column_name: str, condition_name: str) -> MetricConstraint:
    """ """

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
