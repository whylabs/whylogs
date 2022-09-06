from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List

from whylogs.core.metrics.condition_count_metric import Condition
from whylogs.core.preprocessing import PreprocessedColumn


class Validator(ABC):
    name: str
    conditions: Dict[str, Condition]
    actions: List[Callable[[str, str, Any], None]]

    @abstractmethod
    def columnar_validate(self, data: PreprocessedColumn):
        pass

    @abstractmethod
    def to_summary_dict(self) -> Dict[str, Any]:
        pass
