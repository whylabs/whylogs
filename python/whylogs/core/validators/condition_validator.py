import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Union, Optional

from whylogs.core.metrics.condition_count_metric import Condition
from whylogs.core.validators.validator import Validator
from whylogs.core.metrics.metrics import MetricConfig
from whylogs.core.stubs import pd
import whylogs_sketching as ds

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ConditionValidatorConfig(MetricConfig):
    validator_sampling_size: int = 10
    identity_column: Optional[str] = None


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

    def columnar_validate(self, data: Any, sampling_size: int = 10, identity_values: Any = None) -> None:
        count = 0
        count_failures = 0
        validate_with_row_id = False
        if not isinstance(sampling_size, int):
            raise ValueError("sampling_size must be an integer")
        if self.enable_sampling:
            self._sampler = ds.var_opt_sketch(k=sampling_size)
        if pd.Series is not None and isinstance(identity_values, pd.Series):
            if len(identity_values) != len(data):
                logger.warning("Identity values and data are not the same length. Skipping identity validation")
            else:
                validate_with_row_id = True
        for index, x in enumerate(data):
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
                    if self.enable_sampling and validate_with_row_id:
                        self._sampler.update(identity_values[index])
                    elif self.enable_sampling:  # if we don't have an identity column, sample the validated values
                        self._sampler.update(x)
                    for action in self.actions:
                        if validate_with_row_id:
                            action(self.name, cond_name, x, identity_values[index])
                        else:
                            action(self.name, cond_name, x)

        self.total = count

    def sample_failed_conditions(self) -> List[Any]:
        if self._sampler is None:
            raise ValueError("Sampling is not enabled for this validator")
        return [sample[0] for sample in self._sampler.get_samples()]

    def to_summary_dict(self) -> Dict[str, Any]:
        summary = {"total_evaluations": self.total}
        for cond_name, _ in self.conditions.items():
            summary[cond_name] = self.failures[cond_name]
        return summary
