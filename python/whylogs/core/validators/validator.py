import copy
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Union

from whylogs.core.metrics.condition_count_metric import Condition


@dataclass
class Validator(ABC):
    name: str
    conditions: Dict[str, Union[Condition, Callable[[Any], bool]]]
    actions: List[Union[Callable[[str, str, Any], None], Callable[[str, str, Any, Optional[Any]], None]]]

    @abstractmethod
    def columnar_validate(self, data: Any, identity_values: Optional[Any] = None) -> None:
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
