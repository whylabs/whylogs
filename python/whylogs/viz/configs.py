from dataclasses import dataclass, field
from typing import List

import numpy as np


@dataclass
class HistogramConfig:
    max_hist_buckets: int = 30
    hist_avg_number_per_bucket: int = 4


@dataclass
class KSTestConfig:
    quantiles: List[float] = field(default_factory=lambda: list(np.linspace(0, 1, 100)))


@dataclass
class HellingerConfig:
    max_hist_buckets: int = 30
    hist_avg_number_per_bucket: int = 4
