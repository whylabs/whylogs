import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Union

import whylogs_sketching as ds

from whylogs.core.metrics.condition_count_metric import Condition
from whylogs.core.metrics.metrics import MetricConfig
from whylogs.core.stubs import pd
from whylogs.core.validators.validator import Validator

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ConditionValidatorConfig(MetricConfig):
    """
    Configuration for a condition validator.

    Args:
        validator_sample_size (int): The number of failed values to sample. Defaults to 10.
        identity_column (Optional[str]): The name of the column to use as the identity column. Defaults to None.
    """

    validator_sample_size: int = 10
    identity_column: Optional[str] = None


@dataclass
class ConditionValidator(Validator):
    """
    A validator that checks if a column satisfies a condition.

    Args:
        name (str): The name of the column to validate
        conditions (Dict[str, Union[Condition, Callable[[Any], bool]]]): A dictionary of conditions to check.
        actions (List[Callable[[str, str, Any, Any], None]]): A list of actions to take when a condition fails.
        enable_sampling (bool): Whether to enable sampling of failed values. Defaults to True.

    """

    conditions: Dict[str, Union[Condition, Callable[[Any], bool]]]
    actions: List[Callable[[str, str, Any, Any], None]]
    name: str
    total: int = 0
    failures: Dict[str, int] = field(default_factory=dict)
    enable_sampling: bool = True
    _samples: List[Any] = field(default_factory=list)
    _sampler: Optional[ds.var_opt_sketch] = None

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
                    if self.enable_sampling and validate_with_row_id and isinstance(self._sampler, ds.var_opt_sketch):
                        self._sampler.update(identity_values[index])
                    # if we don't have an identity column, sample the validated values
                    elif self.enable_sampling and isinstance(self._sampler, ds.var_opt_sketch):
                        self._sampler.update(x)
                    for action in self.actions:
                        if validate_with_row_id:
                            action(self.name, cond_name, x, identity_values[index])
                        else:
                            action(self.name, cond_name, x)  # type: ignore

        self.total = count

    def sample_failed_conditions(self) -> List[Any]:
        """
        Returns a list of samples of failed values.
        The number of samples is determined by the validator's sampling size, defined through the ConditionValidatorConfig.
        If `identity_column` is set, the samples will be the identity values of the failed values. Otherwise, the samples will contain
        the failed values themselves.
        """
        if self._sampler is None:
            raise ValueError("Sampling is not enabled for this validator")
        return [sample[0] for sample in self._sampler.get_samples()]

    def to_summary_dict(self) -> Dict[str, Any]:
        summary = {"total_evaluations": self.total}
        for cond_name, _ in self.conditions.items():
            summary[cond_name] = self.failures[cond_name]
        return summary
