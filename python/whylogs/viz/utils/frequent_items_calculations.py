from typing import List, Optional, Tuple

from typing_extensions import TypedDict

from whylogs.core.configs import SummaryConfig
from whylogs.core.metrics.metrics import FrequentItem
from whylogs.core.view.column_profile_view import ColumnProfileView


class FrequentItemEstimate(TypedDict):
    value: str
    estimate: float


def get_frequent_items_estimate(frequent_items: List[FrequentItem]) -> List[FrequentItemEstimate]:
    return [{"value": x.value, "estimate": x.est} for x in frequent_items]


class FrequentStats(TypedDict):
    frequent_items: List[FrequentItemEstimate]
    total_count: int
    unique_count: int


def get_frequent_stats(column_view: ColumnProfileView, config: Optional[SummaryConfig]) -> Optional[FrequentStats]:
    if config is None:
        config = SummaryConfig()

    target_fi_metric = column_view.get_metric("frequent_items")
    if target_fi_metric is None:
        return None

    target_frequent_items = target_fi_metric.to_summary_dict(config)["frequent_strings"]
    target_cnt_metric = column_view.get_metric("counts")
    target_count = target_cnt_metric.n.value
    target_card_metric = column_view.get_metric("cardinality")
    target_unique_count = int(target_card_metric.hll.value.get_estimate())

    target_frequent_stats: FrequentStats = {
        "frequent_items": get_frequent_items_estimate(target_frequent_items),
        "total_count": target_count,
        "unique_count": target_unique_count,
    }

    return target_frequent_stats


def frequent_items_from_view(
    column_view: ColumnProfileView, feature_name: str, config: Optional[SummaryConfig] = None
) -> List[FrequentItemEstimate]:
    config = config or SummaryConfig()
    column_frequent_items_metric = column_view.get_metric("frequent_items")
    if not column_frequent_items_metric:
        raise ValueError("Frequent Items Metrics not found for feature {}.".format(feature_name))

    target_frequent_items = column_frequent_items_metric.to_summary_dict(config)["frequent_strings"]
    frequent_items = get_frequent_items_estimate(target_frequent_items)
    return frequent_items


def zero_padding_frequent_items(
    target_feature_items: List[FrequentItemEstimate], reference_feature_items: List[FrequentItemEstimate]
) -> Tuple[List[FrequentItemEstimate], List[FrequentItemEstimate]]:
    """Fills estimate value of item with 0 when such item is present in the other profile but absent in the current profile.
    This is done for both profiles passed.

    Parameters
    ----------
    target_feature_items : List[FrequentItemEstimate]
        A list of frequent items of a given column for target profile
    reference_feature_items : List[FrequentItemEstimate]
        A list of frequent items of a given column for reference profile

    Returns
    -------
    Tuple[List[FrequentItemEstimate], List[FrequentItemEstimate]]
        The same list of items given in the input, but with zero padding for absent items.
    """
    for reference_item in reference_feature_items:
        item_value = reference_item["value"]
        if item_value not in [x["value"] for x in target_feature_items]:
            target_feature_items.append({"value": item_value, "estimate": 0})
    for target_item in target_feature_items:
        item_value = target_item["value"]
        if item_value not in [x["value"] for x in reference_feature_items]:
            reference_feature_items.append({"value": item_value, "estimate": 0})

    return target_feature_items, reference_feature_items
