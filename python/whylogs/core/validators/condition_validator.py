from dataclasses import dataclass
from itertools import chain
from typing import Any, Callable, Dict, List

from whylogs.core.metrics.condition_count_metric import Condition
from whylogs.core.metrics.metric_components import IntegralComponent
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.validators.validator import Validator


@dataclass
class ConditionValidator(Validator):
    def __init__(self, name: str, conditions: Dict[str, Condition], actions=List[Callable]) -> None:
        self.conditions = conditions
        self.name = name
        self.actions = actions
        self.total: IntegralComponent = IntegralComponent(0)
        self.failures: Dict[str, IntegralComponent] = dict()

        if "total" in self.conditions.keys():
            raise ValueError("Condition cannot be named 'total'")

        if "total_failures" in self.conditions.keys():
            raise ValueError("Condition cannot be named 'total_failures'")

        self.failures["total"] = IntegralComponent(0)
        for cond_name in self.conditions.keys():
            if cond_name not in self.failures:
                self.failures[cond_name] = IntegralComponent(0)

    def columnar_validate(self, data: PreprocessedColumn) -> None:
        count = 0
        count_failures = 0
        for x in list(chain.from_iterable(data.raw_iterator())):
            count += 1
            for cond_name, condition in self.conditions.items():
                try:
                    if not condition.relation(x):
                        self.failures[cond_name].set(self.failures[cond_name].value + 1)
                        count_failures += 1
                        for action in self.actions:
                            action(self.name, cond_name, x)
                except:  # noqa
                    pass
        self.total.set(count)
        self.failures["total"].set(count_failures)

    def to_summary_dict(self) -> Dict[str, Any]:
        summary = {"total": self.total.value, "total_failures": self.failures["total"].value}
        for cond_name, condition in self.conditions.items():
            summary[cond_name] = self.failures[cond_name].value
        return summary
