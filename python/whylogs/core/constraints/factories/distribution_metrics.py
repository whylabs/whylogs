from typing import Union

from whylogs.core.constraints import MetricConstraint, MetricsSelector
from whylogs.core.metrics import DistributionMetric


def greater_than_number(column_name: str, number: Union[float, int], skip_missing: bool = True) -> MetricConstraint:
    """Minimum value of given column must be above defined number.

    Parameters
    ----------
    column_name : str
        Column the constraint is applied to
    number : float
        reference value for applying the constraint
    skip_missing: bool
        If skip_missing is True, missing distribution metrics will make the check pass.
        If False, the check will fail on missing metrics
    """

    def is_greater(metric: DistributionMetric) -> bool:
        if not metric.kll.value.is_empty():
            return metric.min >= number
        else:
            return True if skip_missing else False

    constraint = MetricConstraint(
        name=f"{column_name} greater than number {number}",
        condition=is_greater,
        metric_selector=MetricsSelector(column_name=column_name, metric_name="distribution"),
    )
    return constraint


def smaller_than_number(column_name: str, number: float, skip_missing: bool = True) -> MetricConstraint:
    """Maximum value of given column must be below defined number.

    Parameters
    ----------
    column_name : str
        Column the constraint is applied to
    number : float
        reference value for applying the constraint
    skip_missing: bool
        If skip_missing is True, missing distribution metrics will make the check pass.
        If False, the check will fail on missing metrics
    """

    def is_smaller(metric) -> bool:
        if not metric.kll.value.is_empty():
            return number >= metric.max
        else:
            return True if skip_missing else False

    constraint = MetricConstraint(
        name=f"{column_name} smaller than number {number}",
        condition=is_smaller,
        metric_selector=MetricsSelector(column_name=column_name, metric_name="distribution"),
    )
    return constraint


def mean_between_range(column_name: str, lower: float, upper: float, skip_missing: bool = True) -> MetricConstraint:
    """Estimated mean must be between range defined by lower and upper bounds.

    Parameters
    ----------
    column_name : str
        Column the constraint is applied to
    lower : int
        Lower bound of defined range
    upper : int
        Upper bound of the value range
    skip_missing: bool
        If skip_missing is True, missing distribution metrics will make the check pass.
        If False, the check will fail on missing metrics
    """

    def is_mean_between(metric) -> bool:
        if not metric.kll.value.is_empty():
            return lower <= metric.avg <= upper
        else:
            return True if skip_missing else False

    constraint = MetricConstraint(
        name=f"{column_name} mean between {lower} and {upper} (inclusive)",
        condition=is_mean_between,
        metric_selector=MetricsSelector(column_name=column_name, metric_name="distribution"),
    )
    return constraint


def stddev_between_range(column_name: str, lower: float, upper: float, skip_missing: bool = True):
    """Estimated standard deviation must be between range defined by lower and upper bounds.

    Parameters
    ----------
    column_name: str
        Column the constraint is applied to
    lower: float
        Lower bound of defined range
    upper: float
        Upper bound of the value range
    skip_missing: bool
        If skip_missing is True, missing distribution metrics will make the check pass.
        If False, the check will fail on missing metrics
    """

    def is_stddev_between_range(metric):
        if not metric.kll.value.is_empty():
            return lower <= metric.stddev <= upper
        else:
            return True if skip_missing else False

    constraint = MetricConstraint(
        name=f"{column_name} standard deviation between {lower} and {upper} (inclusive)",
        condition=is_stddev_between_range,
        metric_selector=MetricsSelector(column_name=column_name, metric_name="distribution"),
    )
    return constraint


def quantile_between_range(column_name: str, quantile: float, lower: float, upper: float, skip_missing: bool = True):
    """Q-th quantile value must be withing the range defined by lower and upper boundaries.

    Parameters
    ----------
    column_name: str
        Column the constraint is applied to
    quantile: float
        Quantile value. E.g. median is equal to quantile_value=0.5
    lower: float
        Lower bound of defined range
    upper: float
        Upper bound of the value range
    skip_missing: bool
        If skip_missing is True, missing distribution metrics will make the check pass.
        If False, the check will fail on missing metrics
    """

    def quantile_in_range(metric):
        if not metric.kll.value.is_empty():
            quantile_value = metric.kll.value.get_quantile(quantile)
            result: bool = lower <= quantile_value <= upper
            return result
        else:
            return True if skip_missing else False

    constraint = MetricConstraint(
        name=f"{column_name} {quantile}-th quantile value between {lower} and {upper} (inclusive)",
        condition=quantile_in_range,
        metric_selector=MetricsSelector(column_name=column_name, metric_name="distribution"),
    )
    return constraint
