from whylogs.core.configs import SummaryConfig

from ..metric_constraints import MetricConstraint, MetricsSelector


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
