from dataclasses import dataclass
from itertools import chain
from typing import Any, Callable, Dict, List

from whylogs.core.metrics.condition_count_metric import Condition
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.validators.validator import Validator


@dataclass
class ConditionValidator(Validator):
    def __init__(
        self, name: str, conditions: Dict[str, Condition], actions=List[Callable[[str, str, Any], None]]
    ) -> None:
        self.conditions = conditions
        self.name = name
        self.actions = actions
        self.total: int = 0
        self.failures: Dict[str, int] = dict()

        if "total" in self.conditions.keys():
            raise ValueError("Condition cannot be named 'total'")

        if "total_failures" in self.conditions.keys():
            raise ValueError("Condition cannot be named 'total_failures'")

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
                    if not condition.relation(x):
                        self.failures[cond_name] += 1
                        count_failures += 1
                        for action in self.actions:
                            action(self.name, cond_name, x)
                except:  # noqa
                    pass
        self.total = count

    def to_summary_dict(self) -> Dict[str, Any]:
        summary = {"total_evaluations": self.total}
        for cond_name, _ in self.conditions.items():
            summary[cond_name] = self.failures[cond_name]
        return summary
