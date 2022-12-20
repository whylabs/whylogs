from whylogs.core.relations import Require

from ..metric_constraints import MetricConstraint, MetricsSelector
from whylogs.core.view.dataset_profile_view import DatasetProfileView
from whylogs.core.metric_getters import ProfileGetter

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


def is_probably_unique(column_name: str, profile_view: DatasetProfileView = None):
    # hll_stddev can be 1,2,or 3
    for std_dev in [
        1,
        2,
        3,
    ]:  # https://github.com/apache/datasketches-cpp/blob/46c5f796b798a798483d1eea608b13b367195e8e/python/src/hll_wrapper.cpp#L112
        estimate_path = f"cardinality/lower_{std_dev}"

        try:
            lower_card_estimate = ProfileGetter(profile_view, column_name=column_name, path=estimate_path)()
            upper_card_estimate = ProfileGetter(profile_view, column_name=column_name, path=estimate_path)()
            break
        except ValueError:
            continue

    if not lower_card_estimate or not upper_card_estimate:
        raise ValueError("lower and/or upper bounds for cardinality estimate not found")

    def probably_unique(metric):
        count_is_in_bounds = lower_card_estimate <= metric.n.value - metric.null.value <= upper_card_estimate
        return count_is_in_bounds

    constraint = MetricConstraint(
        name=f"{column_name} is probably unique",
        condition=Require().is_(probably_unique),
        metric_selector=MetricsSelector(column_name=column_name, metric_name="counts"),
    )
    return constraint
