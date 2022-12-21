from typing import Union

from whylogs.core.metrics.metrics import CardinalityMetric

from ..metric_constraints import MetricConstraint, MetricsSelector


def distinct_number_in_range(column_name: str, lower: Union[int, float], upper: Union[int, float]) -> MetricConstraint:
    """Number of distinct categories must be between lower and upper values (inclusive).

    Parameters
    ----------
    column_name : str
        Column the constraint is applied to
    lower : int
        Lower bound of defined range
    upper : int
        Upper bound of the value range
    """

    def distinct_in_range(metric: CardinalityMetric):
        distinct_estimate = metric.hll.value.get_estimate()
        if not lower > distinct_estimate and not distinct_estimate > upper:
            return True
        return False

    constraint = MetricConstraint(
        name=f"{column_name} distinct values estimate between {lower} and {upper} (inclusive)",
        condition=distinct_in_range,
        metric_selector=MetricsSelector(column_name=column_name, metric_name="cardinality"),
    )
    return constraint
