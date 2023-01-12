from dataclasses import dataclass, field
from typing import List, Optional
from enum import Enum
from typing_extensions import TypedDict
import numpy as np


class DriftCategory(Enum):
    NO_DRIFT = 0
    POSSIBLE_DRIFT = 1
    DRIFT = 2


@dataclass
class DriftThresholds:
    NO_DRIFT: tuple
    POSSIBLE_DRIFT: Optional[tuple]
    DRIFT: tuple

    def to_dict(self):
        threshold_dict = {
            "NO_DRIFT": self.NO_DRIFT,
            "POSSIBLE_DRIFT": self.POSSIBLE_DRIFT,
            "DRIFT": self.DRIFT,
        }
        return threshold_dict


@dataclass
class KSTestConfig:
    quantiles: List[float] = field(default_factory=lambda: list(np.linspace(0, 1, 100)))
    thresholds: List[float] = field(default_factory=lambda: [0.05, 0.15])
    n_thresholds = DriftThresholds(NO_DRIFT=(0.15, 1.1), POSSIBLE_DRIFT=(0.05, 0.15), DRIFT=(0, 0.05))
    polarity: str = "negative"


@dataclass
class HellingerConfig:
    max_hist_buckets: int = 30
    hist_avg_number_per_bucket: int = 4
    min_n_buckets: int = 2
    thresholds: List[float] = field(default_factory=lambda: [0.2, 0.5])
    n_thresholds = DriftThresholds(NO_DRIFT=(0.15, 1.1), POSSIBLE_DRIFT=(0.05, 0.15), DRIFT=(0, 0.05))
    polarity: str = "positive"


@dataclass
class ChiSquareConfig:
    thresholds: List[float] = field(default_factory=lambda: [0.05, 0.15])
    n_thresholds = DriftThresholds(NO_DRIFT=(0.15, 1.1), POSSIBLE_DRIFT=(0.05, 0.15), DRIFT=(0, 0.05))
    polarity: str = "negative"
