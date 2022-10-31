from dataclasses import dataclass, field
import numpy as np
from typing import List


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
