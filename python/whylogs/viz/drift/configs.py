from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

import numpy as np


class DriftCategory(Enum):
    NO_DRIFT = 0
    POSSIBLE_DRIFT = 1
    DRIFT = 2


@dataclass
class DriftThresholds:
    NO_DRIFT: tuple
    DRIFT: tuple
    POSSIBLE_DRIFT: Optional[tuple] = None

    def to_dict(self):
        threshold_dict = {}
        threshold_dict["NO_DRIFT"] = self.NO_DRIFT
        if self.POSSIBLE_DRIFT:
            threshold_dict["POSSIBLE_DRIFT"] = self.POSSIBLE_DRIFT
        threshold_dict["DRIFT"] = self.DRIFT
        return threshold_dict


@dataclass
class KSTestConfig:
    quantiles: List[float] = field(default_factory=lambda: list(np.linspace(0, 1, 100)))
    thresholds: DriftThresholds = field(
        default_factory=lambda: DriftThresholds(NO_DRIFT=(0.15, 1), POSSIBLE_DRIFT=(0.05, 0.15), DRIFT=(0, 0.05))
    )


@dataclass
class HellingerConfig:
    max_hist_buckets: int = 30
    hist_avg_number_per_bucket: int = 4
    min_n_buckets: int = 2
    thresholds: DriftThresholds = field(
        default_factory=lambda: DriftThresholds(NO_DRIFT=(0, 0.15), POSSIBLE_DRIFT=(0.15, 0.4), DRIFT=(0.4, 1))
    )


@dataclass
class ChiSquareConfig:
    thresholds: DriftThresholds = field(
        default_factory=lambda: DriftThresholds(NO_DRIFT=(0.15, 1), POSSIBLE_DRIFT=(0.05, 0.15), DRIFT=(0, 0.05))
    )
