import logging

from dataclasses import dataclass, field
from itertools import chain
from typing import Any, Callable, Dict, List

from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.relations import Expression
from whylogs.core.validators.validator import Validator

logger = logging.getLogger(__name__)


@dataclass
class ConditionValidator(Validator):
    conditions: Dict[str, Expression]
    actions: List[Callable[[str, str, Any], None]]
    name: str
    total: int = 0
    failures: Dict[str, int] = field(default_factory=dict)

    def __post_init__(self):
        for cond_name in self.conditions.keys():
            if cond_name not in self.failures:
                self.failures[cond_name] = 0

    def columnar_validate(self, data: PreprocessedColumn) -> None:
        count = 0
        count_failures = 0
        for x in list(chain.from_iterable(data.raw_iterator())):
            count += 1
            for cond_name, condition in self.conditions.items():
                try:
                    valid = condition[0](x, condition[1])
                except Exception as e:
                    valid = False
                    logger.exception(e)

                if not valid:
                    self.failures[cond_name] += 1
                    count_failures += 1
                    for action in self.actions:
                        action(self.name, cond_name, x)
                    
        self.total = count

    def to_summary_dict(self) -> Dict[str, Any]:
        summary = {"total_evaluations": self.total}
        for cond_name, _ in self.conditions.items():
            summary[cond_name] = self.failures[cond_name]
        return summary
