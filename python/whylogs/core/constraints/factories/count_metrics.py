from whylogs.core.relations import Require

from ..metric_constraints import MetricConstraint, MetricsSelector

# TODO implement skip_missing for all methods


def no_missing_values(column_name: str) -> MetricConstraint:
    """Checks that there are no missing values in the column.

    Parameters
    ----------
    column_name : str
        Column the constraint is applied to
    """

    constraint = MetricConstraint(
        name=f"{column_name} has no missing values",
        condition=Require("null").equals(0),
        metric_selector=MetricsSelector(column_name=column_name, metric_name="counts"),
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

    def is_count_below(x) -> bool:
        return number >= x.n.value

    constraint = MetricConstraint(
        name=f"count of {column_name} lower than {number}",
        condition=is_count_below,
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

    def is_null_below(x):
        return x.null.value < number

    constraint = MetricConstraint(
        name=f"null values of {column_name} lower than {number}",
        condition=is_null_below,
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

    def is_null_percentage_below_number(x):
        return (x.null.value / x.n.value) <= number

    constraint = MetricConstraint(
        name=f"null percentage of {column_name} lower than {number}",
        condition=is_null_percentage_below_number,
        metric_selector=MetricsSelector(column_name=column_name, metric_name="counts"),
    )
    return constraint
