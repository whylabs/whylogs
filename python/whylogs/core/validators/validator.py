from abc import ABC, abstractmethod
from whylogs.core.preprocessing import PreprocessedColumn
from typing import Callable, Dict, List
from whylogs.core.metrics.condition_count_metric import Condition


class Validator(ABC):
    name: str
    conditions: Dict[str, Condition]
    actions: List[Callable]

    @abstractmethod
    def columnar_validate(self, data: PreprocessedColumn):
        raise NotImplementedError
