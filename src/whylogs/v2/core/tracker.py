import logging as test_logging


from abc import ABC, abstractmethod
from typing import Optional, Type


from whylogs.proto import TrackerMap, TrackerMessage, TrackerSummary
from whylogs.proto.summaries_pb2 import UniqueCountSummary
from whylogs.v2.core.column_predicate import ColumnPredicate


class Tracker(ABC):
    """
    Abstract class for trackers.

    """

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    # Cardinality tracking has special handling so surface candidate unique count estimators here.
    @property
    def is_cardinality_tracker(self):
        return False

    def get_unique_count_summary(self) -> UniqueCountSummary:
        return None
    
    def has_unique_count(self) -> bool:
        return False

    @staticmethod
    def type_from_index(type_index: TrackerMap.Type):
        TrackerMap

    @classmethod
    @abstractmethod
    def from_protobuf(cls, message: TrackerMessage) -> "Tracker":
        pass

    @abstractmethod
    def to_protobuf(
        self,
    ) -> TrackerMessage:
        pass

    @abstractmethod
    def track(self, data):
        pass

    @abstractmethod
    def merge(self, other: "Tracker"):
        pass

    @abstractmethod
    def to_summary(self) -> TrackerSummary:
       pass








