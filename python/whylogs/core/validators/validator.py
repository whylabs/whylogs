import copy
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


def deepcopy_validators(obj):
    if isinstance(obj, dict):
        return {k: deepcopy_validators(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [deepcopy_validators(item) for item in obj]
    elif isinstance(obj, Validator):
        copied_obj = copy.copy(obj)

        if hasattr(obj, "_sampler"):
            copied_obj._sampler = None
        if hasattr(obj, "_samples"):
            copied_obj._samples = []
        if hasattr(obj, "total"):
            copied_obj.total = 0
        if hasattr(obj, "failures"):
            copied_obj.failures = {k: 0 for k in obj.failures.keys()}
        return copied_obj
    else:
        return copy.deepcopy(obj)
