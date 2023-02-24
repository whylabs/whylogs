from typing import Union

from whylogs.core.constraints import MetricConstraint, MetricsSelector
from whylogs.core.relations import Require


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
        If False, the check will fail on missing metrics, such as on an empty dataset
    """

    constraint = MetricConstraint(
        name=f"{column_name} greater than number {number}",
        condition=Require("min").greater_than(number),
        metric_selector=MetricsSelector(column_name=column_name, metric_name="distribution"),
        require_column_existence=not skip_missing,
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
        If False, the check will fail on missing metrics, such as on an empty dataset
    """

    constraint = MetricConstraint(
        name=f"{column_name} smaller than number {number}",
        condition=lambda x: number > x.max,
        metric_selector=MetricsSelector(column_name=column_name, metric_name="distribution"),
        require_column_existence=not skip_missing,
    )
    return constraint


def is_non_negative(column_name: str, skip_missing: bool = True) -> MetricConstraint:
    """Checks if a column is non negative

    Parameters
    ----------
    column_name : str
        Column the constraint is applied to
    skip_missing: bool
        If skip_missing is True, missing distribution metrics will make the check pass.
        If False, the check will fail on missing metrics, such as on an empty dataset
    """
    constraint = MetricConstraint(
        name=f"{column_name} is non negative",
        condition=Require("min").greater_or_equals(0),
        metric_selector=MetricsSelector(column_name=column_name, metric_name="distribution"),
        require_column_existence=not skip_missing,
    )
    return constraint


def is_in_range(
    column_name: str, lower: Union[float, int], upper: Union[float, int], skip_missing: bool = True
) -> MetricConstraint:
    """Checks that all of column's values are in defined range (inclusive).

    For the constraint to pass, the column's minimum value should be higher or equal than `lower` and maximum value should be less than or equal to `upper`.

    Parameters
    ----------
    column_name : str
        Column the constraint is applied to
    lower : float
        lower bound of defined range
    upper : float
        upper bound of defined range
    skip_missing: bool
        If skip_missing is True, missing distribution metrics will make the check pass.
        If False, the check will fail on missing metrics, such as on an empty dataset
    """

    constraint = MetricConstraint(
        name=f"{column_name} is in range [{lower},{upper}]",
        condition=Require("min").greater_or_equals(lower).and_(Require("max").less_or_equals(upper)),
        metric_selector=MetricsSelector(column_name=column_name, metric_name="distribution"),
        require_column_existence=not skip_missing,
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
        If False, the check will fail on missing metrics, such as on an empty dataset
    """

    constraint = MetricConstraint(
        name=f"{column_name} mean between {lower} and {upper} (inclusive)",
        condition=Require("mean").greater_or_equals(lower).and_(Require("mean").less_or_equals(upper)),
        metric_selector=MetricsSelector(column_name=column_name, metric_name="distribution"),
        require_column_existence=not skip_missing,
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
        If False, the check will fail on missing metrics, such as on an empty dataset
    """

    constraint = MetricConstraint(
        name=f"{column_name} standard deviation between {lower} and {upper} (inclusive)",
        condition=Require("stddev").greater_or_equals(lower).and_(Require("stddev").less_or_equals(upper)),
        metric_selector=MetricsSelector(column_name=column_name, metric_name="distribution"),
        require_column_existence=not skip_missing,
    )
    return constraint


def quantile_between_range(
    column_name: str, quantile: float, lower: float, upper: float, skip_missing: bool = True
) -> MetricConstraint:
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
        If False, the check will fail on missing metrics, such as on an empty dataset
    """

    def quantile_in_range(metric) -> bool:
        quantile_value = metric.kll.value.get_quantile(quantile)
        result: bool = lower <= quantile_value <= upper
        return result

    constraint = MetricConstraint(
        name=f"{column_name} {quantile}-th quantile value between {lower} and {upper} (inclusive)",
        condition=quantile_in_range,
        metric_selector=MetricsSelector(column_name=column_name, metric_name="distribution"),
        require_column_existence=not skip_missing,
    )
    return constraint
