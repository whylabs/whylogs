from abc import ABC, abstractmethod

from whylogs.proto import TrackerMessage, TrackerSummary
from whylogs.proto.summaries_pb2 import UniqueCountSummary


class Tracker(ABC):
    """
    Abstract class for trackers. Trackers represent built in statistics and things that build summaries.

    """

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    # Cardinality tracking has special handling, we can remove th
    @property
    def is_cardinality_tracker(self):
        return False

    def get_unique_count_summary(self) -> UniqueCountSummary:
        return None

    def has_unique_count(self) -> bool:
        return False

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
    def track(self, data, data_type):
        pass

    @abstractmethod
    def merge(self, other: "Tracker"):
        pass

    @abstractmethod
    def to_summary(self) -> TrackerSummary:
        pass
