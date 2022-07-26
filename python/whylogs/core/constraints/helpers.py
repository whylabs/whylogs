from typing import Union

from whylogs.core.configs import SummaryConfig
from whylogs.core.constraints import MetricConstraint, MetricsSelector
from whylogs.core.metrics.metrics import Metric


def greater_than_number(column_name: str, number: Union[float, int]) -> MetricConstraint:
    """Minimum value of given column must be above defined number.

    Parameters
    ----------
    column_name : str
        Column the constraint is applied to
    number : float
        reference value for applying the constraint

    """
    constraint = MetricConstraint(
        name=f"{column_name} greater than number {number}",
        condition=lambda x: x.min >= number,
        metric_selector=MetricsSelector(column_name=column_name, metric_name="distribution"),
    )
    return constraint


def smaller_than_number(column_name: str, number: float) -> MetricConstraint:
    """Maximum value of given column must be below defined number.

    Parameters
    ----------
    column_name : str
        Column the constraint is applied to
    number : float
        reference value for applying the constraint

    """
    constraint = MetricConstraint(
        name=f"{column_name} smaller than number {number}",
        condition=lambda x: number >= x.max,
        metric_selector=MetricsSelector(column_name=column_name, metric_name="distribution"),
    )
    return constraint


def mean_between_range(column_name: str, lower: float, upper: float) -> MetricConstraint:
    """Estimated mean must be between range defined by lower and upper bounds.

    Parameters
    ----------
    column_name : str
        Column the constraint is applied to
    lower : int
        Lower bound of defined range
    upper : int
        Upper bound of the value range
    """

    constraint = MetricConstraint(
        name=f"{column_name} mean between {lower} and {upper} (inclusive)",
        condition=lambda x: lower <= x.avg <= upper,
        metric_selector=MetricsSelector(column_name=column_name, metric_name="distribution"),
    )
    return constraint


def stddev_between_range(column_name, lower, upper):
    """Estimated standard deviation must be between range defined by lower and upper bounds.

    Parameters
    ----------
    column_name: str
        Column the constraint is applied to
    lower: int
        Lower bound of defined range
    upper: int
        Upper bound of the value range
    """

    constraint = MetricConstraint(
        name=f"{column_name} standard deviation between {lower} and {upper} (inclusive)",
        condition=lambda x: lower <= x.stddev <= upper,
        metric_selector=MetricsSelector(column_name=column_name, metric_name="distribution"),
    )
    return constraint


def quantile_between_range(column_name: str, quantile: float, lower: float, upper: float, skip_missing: bool = True):
    """Q-th quantile value must be withing the range defined by lower and upper boundaries.

    Parameters
    ----------
    column_name: str
        Column the constraint is applied to
    quantile_value: float
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
            result = lower <= quantile_value <= upper
            return result
        else:
            return True if skip_missing else False

    constraint = MetricConstraint(
        name=f"{column_name} {quantile}-th quantile value between {lower} and {upper} (inclusive)",
        condition=quantile_in_range,
        metric_selector=MetricsSelector(column_name=column_name, metric_name="distribution"),
    )
    return constraint


def count_below_number(column_name: str, number: int) -> MetricConstraint:
    """Number of elements in a column must be below given number.

    Parameters
    ----------
    column_name : str
        Column the constraint is applied to
    number : float
        reference value for applying the constraint
    """

    constraint = MetricConstraint(
        name=f"count of {column_name} lower than {number}",
        condition=lambda x: number >= x.n.value,
        metric_selector=MetricsSelector(column_name=column_name, metric_name="counts"),
    )
    return constraint


def null_values_below_number(column_name: str, number: int) -> MetricConstraint:
    """Number of null values must be below given number.

    Parameters
    ----------
    column_name : str
        Column the constraint is applied to
    number : float
        reference value for applying the constraint
    """

    constraint = MetricConstraint(
        name=f"null values of {column_name} lower than {number}",
        condition=lambda x: x.null.value < number,
        metric_selector=MetricsSelector(column_name=column_name, metric_name="counts"),
    )
    return constraint


def null_percentage_below_number(column_name: str, number: float) -> MetricConstraint:
    """Percentage of null values must be below given number.

    Parameters
    ----------
    column_name : str
        Column the constraint is applied to
    number : float
        reference value for applying the constraint
    """

    constraint = MetricConstraint(
        name=f"null percentage of {column_name} lower than {number}",
        condition=lambda x: not x.null.value / x.n.value >= number,
        metric_selector=MetricsSelector(column_name=column_name, metric_name="counts"),
    )
    return constraint


def distinct_number_in_range(column_name: str, lower: int, upper: int) -> MetricConstraint:
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

    def distinct_in_range(metric: Metric):
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


def frequent_strings_in_reference_set(column_name: str, reference_set: dict) -> MetricConstraint:
    """Determine whether a set of variables appear in the frequent strings for a string column.
    Every item in frequent strings must be in defined reference set

    Parameters
    ----------
    column_name : str
        Columns the constraint is applied to.
    reference_set : dict
        Reference set for applying the constraint
    """
    frequent_strings = MetricsSelector(metric_name="frequent_items", column_name=column_name)

    def labels_in_set(metric):
        frequent_strings = metric.to_summary_dict(SummaryConfig())["frequent_strings"]
        result = all(item.value in reference_set for item in frequent_strings)
        return result

    constraint_name = f"{column_name} values in set {reference_set}"
    constraint = MetricConstraint(name=constraint_name, condition=labels_in_set, metric_selector=frequent_strings)
    return constraint


def n_most_common_items_in_set(column_name: str, n: int, reference_set: dict) -> MetricConstraint:
    """Validate if the top n most common items appear in the dataset.

    Parameters
    ----------
    column_name : str
        Columns the constraint is applied to.
    n : int
        n most common items or strings.
    reference_set : dict
        Reference set for applying the constraint
    """
    frequent_strings = MetricsSelector(metric_name="frequent_items", column_name=column_name)
    constraint_name = f"{column_name} {n}-most common items in set {reference_set}"

    def most_common_in_set(metric):
        frequent_strings = metric.to_summary_dict(SummaryConfig())["frequent_strings"]
        most_common_items = frequent_strings[:n]
        result = all(item.value in reference_set for item in most_common_items)
        return result

    constraint = MetricConstraint(name=constraint_name, condition=most_common_in_set, metric_selector=frequent_strings)
    return constraint
