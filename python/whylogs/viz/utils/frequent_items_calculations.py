from typing import Any, Dict, List, Optional

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

    target_fi_metric = column_view.get_metric("fi")
    if target_fi_metric:
        target_frequent_items = target_fi_metric.to_summary_dict(config)["fs"]
        target_cnt_metric = column_view.get_metric("cnt")
        target_count = target_cnt_metric.n.value
        target_card_metric = column_view.get_metric("card")
        target_unique_count = int(target_card_metric.hll.value.get_estimate())

        target_frequent_stats: FrequentStats = {
            "frequent_items": get_frequent_items_estimate(target_frequent_items),
            "total_count": target_count,
            "unique_count": target_unique_count,
        }
    else:
        return None

    return target_frequent_stats


def frequent_items_from_view(
    column_view: ColumnProfileView, feature_name: str, config: SummaryConfig
) -> List[FrequentItemEstimate]:
    column_frequent_items_metric = column_view.get_metric("fi")
    if not column_frequent_items_metric:
        raise ValueError("Frequent Items Metrics not found for feature {}.".format(feature_name))

    target_frequent_items = column_frequent_items_metric.to_summary_dict(config)["fs"]
    frequent_items = get_frequent_items_estimate(target_frequent_items)
    return frequent_items
