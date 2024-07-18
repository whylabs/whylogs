import logging
from dataclasses import dataclass, field
from itertools import chain
from typing import Any, Dict, List, Optional

import whylogs_sketching as ds

from whylogs.core.metrics.condition_count_metric import Condition
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.validators.validator import Validator

logger = logging.getLogger(__name__)


@dataclass
class ConditionValidator(Validator):
    """
    A validator that checks if a column satisfies a condition.

    Args:
        name (str): The name of the ConditionValidator.
        conditions (Dict[str, Union[Condition, Callable[[Any], bool]]]): A dictionary of conditions to check.
        actions (List[Union[Callable[[str, str, Any], None], Callable[[str, str, Any, Optional[Any]], None]]]):
            A list of actions to take when a condition fails. The action arguments represent, respectively:
            condition validator name, condition name, invalid value, row identity value (if any).
        enable_sampling (bool): Whether to enable sampling of failed values. Defaults to True.

    """

    total: int = 0
    failures: Dict[str, int] = field(default_factory=dict)
    enable_sampling: bool = True
    _samples: List[Any] = field(default_factory=list)
    _sampler: Optional[ds.var_opt_sketch] = None
    sample_size: int = 10

    def __post_init__(self):
        from whylogs.api.usage_stats import emit_usage

        emit_usage("condition_validator")
        for cond_name in self.conditions.keys():
            if cond_name not in self.failures:
                self.failures[cond_name] = 0
        if not isinstance(self.sample_size, int):
            raise ValueError("sample_size must be an integer")
        if self.enable_sampling:
            self._sampler = ds.var_opt_sketch(k=self.sample_size)

    def columnar_validate(self, data: Any, identity_values: Optional[Any] = None) -> None:
        count = 0
        count_failures = 0
        validate_with_row_id = False
        if self.enable_sampling and self._sampler is None:
            self._sampler = ds.var_opt_sketch(k=self.sample_size)
        if isinstance(data, PreprocessedColumn):
            identity_list = (
                list(chain.from_iterable(identity_values.raw_iterator())) if identity_values is not None else None
            )
            data_list = list(chain.from_iterable(data.raw_iterator()))
        else:
            identity_list = identity_values if identity_values is not None else None
            data_list = data
        if identity_list is not None and data_list is not None and len(identity_list) == len(data_list):
            validate_with_row_id = True
        for index, x in enumerate(data_list):
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
                        self._sampler.update(identity_list[index])  # type: ignore
                    # if we don't have an identity column, sample the validated values
                    elif self.enable_sampling and isinstance(self._sampler, ds.var_opt_sketch):
                        self._sampler.update(x)
                    for action in self.actions:
                        if validate_with_row_id:
                            action(self.name, cond_name, x, identity_list[index])  # type: ignore
                        else:
                            action(self.name, cond_name, x)  # type: ignore

        self.total = count

    def get_samples(self) -> List[Any]:
        """
        Returns a list of samples of failed values.
        The number of samples is determined by the validator's sampling size, defined through the MetricConfig.
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
