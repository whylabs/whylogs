from typing import List

from whylogs.core.metrics.column_metrics import TypeCountersMetric

from ..metric_constraints import MetricConstraint, MetricsSelector


def column_has_non_zero_types(column_name: str, types_list: List[str]) -> MetricConstraint:
    def has_non_zero_types(x) -> bool:
        types_dict = x.to_summary_dict()
        for key in types_dict.keys():
            if key in types_list and types_dict[key] == 0:
                return False
        return True

    constraint = MetricConstraint(
        name=f"{column_name} types count non-zero for {types_list}",
        condition=has_non_zero_types,
        metric_selector=MetricsSelector(column_name=column_name, metric_name="types"),
    )
    return constraint


def column_has_zero_count_types(column_name: str, types_list: List[str]) -> MetricConstraint:
    def has_zero_types(x) -> bool:
        types_dict = x.to_summary_dict()
        for key in types_dict.keys():
            if key in types_list and types_dict[key] != 0:
                return False
        return True

    all_types = TypeCountersMetric.zero().to_summary_dict().keys()
    complement_types = list(set(all_types) - set(types_list))
    constraint = MetricConstraint(
        name=f"{column_name} allows for types {complement_types}",
        condition=has_zero_types,
        metric_selector=MetricsSelector(column_name=column_name, metric_name="types"),
    )
    return constraint


def column_is_nullable_integral(column_name: str) -> MetricConstraint:
    return column_is_nullable_datatype(column_name=column_name, datatype="integral")


def column_is_nullable_fractional(column_name: str) -> MetricConstraint:
    return column_is_nullable_datatype(column_name=column_name, datatype="fractional")


def column_is_nullable_boolean(column_name: str) -> MetricConstraint:
    return column_is_nullable_datatype(column_name=column_name, datatype="boolean")


def column_is_nullable_string(column_name: str) -> MetricConstraint:
    return column_is_nullable_datatype(column_name=column_name, datatype="string")


def column_is_nullable_object(column_name: str) -> MetricConstraint:
    return column_is_nullable_datatype(column_name=column_name, datatype="object")


def column_is_nullable_datatype(column_name: str, datatype: str) -> MetricConstraint:
    """Check if column contains only records of specific datatype.
    Datatypes can be: integral, fractional, boolean, string, object.

    Returns True if there is at least one record of type datatype and there is no records of remaining types.

    Parameters
    ----------
    column_name : str
        Column the constraint is applied to

    """

    def is_integer(x) -> bool:
        types_dict = x.to_summary_dict()
        for key in types_dict.keys():
            if key == datatype and types_dict[key] == 0:
                return False
            if key != datatype and types_dict[key] > 0:
                return False
        return True

    constraint = MetricConstraint(
        name=f"{column_name} is nullable {datatype}",
        condition=is_integer,
        metric_selector=MetricsSelector(column_name=column_name, metric_name="types"),
    )
    return constraint
