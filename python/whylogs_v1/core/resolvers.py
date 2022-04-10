from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict

from whylogs_v1.core.datatypes import AnyType, DataType, Fractional, Integral, String
from whylogs_v1.core.metrics import get_registry
from whylogs_v1.core.trackers import Tracker


class StandardTrackerNames(str, Enum):
    hist = "hist"
    cnt = "cnt"
    uniq = "uniq"
    fi = "fi"
    null = "null"
    int_max = "int.max"
    int_min = "int.min"


class Resolver(ABC):
    """A resolver maps from a column name and a data type to trackers.

    Note that the key of the result dictionaries defines the namespaces of the metrics in the serialized form."""

    @abstractmethod
    def resolve(self, name: str, why_type: DataType) -> Dict[str, Tracker]:
        raise NotImplementedError


class StandardResolver(Resolver):
    """Standard tracker resolution with built in types."""

    _registry = get_registry()

    def resolve(self, name: str, why_type: DataType) -> Dict[str, Tracker]:
        result: Dict[str, Tracker] = {}
        self._add_tracker(mapping=result, name=StandardTrackerNames.cnt, why_type=AnyType())
        self._add_tracker(mapping=result, name=StandardTrackerNames.null, why_type=AnyType())
        if isinstance(why_type, Integral):
            self._add_tracker(mapping=result, name=StandardTrackerNames.int_min, why_type=Integral())
            self._add_tracker(mapping=result, name=StandardTrackerNames.int_max, why_type=Integral())
            self._add_tracker(mapping=result, name=StandardTrackerNames.hist, why_type=Fractional())
            self._add_tracker(mapping=result, name=StandardTrackerNames.uniq, why_type=AnyType())
            self._add_tracker(mapping=result, name=StandardTrackerNames.fi, why_type=AnyType())
        elif isinstance(why_type, Fractional):
            self._add_tracker(mapping=result, name=StandardTrackerNames.hist, why_type=Fractional())
            self._add_tracker(mapping=result, name=StandardTrackerNames.uniq, why_type=AnyType())
        elif isinstance(why_type, String):
            self._add_tracker(mapping=result, name=StandardTrackerNames.uniq, why_type=AnyType())
            self._add_tracker(mapping=result, name=StandardTrackerNames.fi, why_type=AnyType())
        return result

    def _add_tracker(self, mapping: Dict[str, Tracker], name: str, why_type: DataType) -> None:
        mapping[name] = Tracker(why_type, self._registry.get_updatable_metric(name))
