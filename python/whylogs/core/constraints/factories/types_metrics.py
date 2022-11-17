from ..metric_constraints import MetricConstraint, MetricsSelector


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
