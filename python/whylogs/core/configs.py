from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

import whylogs_sketching as ds  # type: ignore

# MetricConfig default values

hll_lg_k: int = 12
kll_k: int = 256
fi_lg_max_k: int = 10  # 128 entries
fi_disabled: bool = False
track_unicode_ranges: bool = False
large_kll_k: bool = True
kll_k_large: int = 1024
unicode_ranges: Dict[str, Tuple[int, int]] = {
    "emoticon": (0x1F600, 0x1F64F),
    "control": (0x00, 0x1F),
    "digits": (0x30, 0x39),
    "latin-upper": (0x41, 0x5A),
    "latin-lower": (0x61, 0x7A),
    "basic-latin": (0x00, 0x7F),
    "extended-latin": (0x0080, 0x02AF),
}
lower_case: bool = True  # Convert Unicode characters to lower-case before counting Unicode ranges
normalize: bool = True  # Unicode normalize strings before counting Unicode ranges
max_frequent_item_size: int = 128
identity_column: Optional[str] = None
column_batch_size: Optional[int] = 1024


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
