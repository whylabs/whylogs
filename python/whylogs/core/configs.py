from dataclasses import dataclass, field
from enum import Enum
from typing import List

import whylogs_sketching as ds  # type: ignore


class FrequentItemsErrorType(int, Enum):
    NO_FALSE_NEGATIVES = 1
    NO_FALSE_POSITIVES = 0

    def to_datasketches_type(self) -> ds.frequent_items_error_type:
        if self.value == FrequentItemsErrorType.NO_FALSE_NEGATIVES.value:
            return ds.frequent_items_error_type.NO_FALSE_NEGATIVES
        else:
            return ds.frequent_items_error_type.NO_FALSE_POSITIVES


@dataclass
class SummaryConfig:
    disabled_metrics: List[str] = field(default_factory=list)
    frequent_items_error_type: FrequentItemsErrorType = FrequentItemsErrorType.NO_FALSE_POSITIVES
    frequent_items_limit: int = -1
    hll_stddev: int = 1
