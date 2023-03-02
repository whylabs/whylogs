import logging
from dataclasses import dataclass, field
from itertools import chain
from typing import Any, Callable, Dict, List, Union, Optional

from whylogs.core.metrics.condition_count_metric import Condition
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.validators.validator import Validator
import random
import math
from whylogs.core.metrics.metrics import MetricConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ConditionValidatorConfig(MetricConfig):
    validator_sampling_size: int = 10


@dataclass
class ConditionValidator(Validator):
    conditions: Dict[str, Union[Condition, Callable[[Any], bool]]]
    actions: List[Callable[[str, str, Any], None]]
    name: str
    total: int = 0
    failures: Dict[str, int] = field(default_factory=dict)
    enable_sampling: bool = True
    _samples = []
    _sampler = None

    def __post_init__(self):
        for cond_name in self.conditions.keys():
            if cond_name not in self.failures:
                self.failures[cond_name] = 0

    def columnar_validate(self, data: PreprocessedColumn, sampling_size: int = 10) -> None:
        count = 0
        count_failures = 0
        if not isinstance(sampling_size, int):
            raise ValueError("sampling_size must be an integer")
        if self.enable_sampling:
            self._sampler = ReservoirSampler(sampling_size)
        for x in list(chain.from_iterable(data.raw_iterator())):
            count += 1
            for cond_name, condition in self.conditions.items():
                try:
                    if isinstance(condition, Condition):
                        valid = condition.relation(x)
                    else:
                        valid = condition(x)
                except Exception as e:
                    valid = False
                    logger.exception(e)

                if not valid:
                    self.failures[cond_name] += 1
                    count_failures += 1
                    if self.enable_sampling:
                        self._sampler.add(x)
                    for action in self.actions:
                        action(self.name, cond_name, x)

        self.total = count

    def to_summary_dict(self) -> Dict[str, Any]:
        summary = {"total_evaluations": self.total}
        for cond_name, _ in self.conditions.items():
            summary[cond_name] = self.failures[cond_name]
        return summary

    def sample_failed_conditions(self) -> List[Any]:
        return self._sampler.sample()


class ReservoirSampler:
    def __init__(self, k):
        self.k = k
        self.R = []
        self.i = 0
        self.W = math.exp(math.log(random.random()) / k)
        self.i_to_sample = k + math.floor(math.log(random.random()) / math.log(1 - self.W)) + 1

    def add(self, x):
        if self.i < self.k:
            self.R.append(x)
            self.i += 1
        else:
            if self.i == self.i_to_sample:
                self.R[random.randint(0, self.k - 1)] = x
                self.W = self.W * math.exp(math.log(random.random()) / self.k)
                self.i_to_sample = self.i + math.floor(math.log(random.random()) / math.log(1 - self.W)) + 1
            self.i += 1

    def sample(self):
        return self.R
