from whylogs.core.validators.validator import Validator
from whylogs.core.metrics.condition_count_metric import Condition
from typing import Callable, Dict, List
from whylogs.core.preprocessing import PreprocessedColumn
from itertools import chain


class ConditionValidator(Validator):
    def __init__(self, name: str, conditions: Dict[str, Condition], actions=List[Callable]):
        self.conditions = conditions
        self.name = name
        self.actions = actions

    def columnar_validate(self, data: PreprocessedColumn):
        count = 0
        for x in list(chain.from_iterable(data.raw_iterator())):
            count += 1
            for cond_name, condition in self.conditions.items():
                try:
                    if condition.relation(x):
                        for action in self.actions:
                            action(cond_name, x)
                except:  # noqa
                    pass
