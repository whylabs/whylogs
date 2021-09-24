import warnings

from whylogs.proto import TrackerMessage, TrackerSummary
from whylogs.v2.core.tracker import Tracker

_TRACKER_TYPE = 1


class CountersTracker(Tracker):
    """
    Class to keep track of the counts of various data types

    Parameters
    ----------
    count : int, optional
        Current number of objects
    """

    def __init__(self, count=0):
        self.count = count
        self.name = "Counter"
        self.summary_name = "counters"

    def increment_count(self):
        """
        Add one to the count of total objects
        """
        self.count += 1

    def track(self, value, _):
        self.increment_count()

    def increment_bool(self):
        """
        Add one to the boolean count
        """
        warnings.warn("This call is a No-OP. Use SchemaTracker.boolCount instead", DeprecationWarning)

    def merge(self, other):
        """
        Merge another counter tracker with this one

        Returns
        -------
        new_tracker : CountersTracker
            The merged tracker
        """
        return CountersTracker(count=self.count + other.count)

    def to_protobuf(self) -> TrackerMessage:
        """
        Return the object serialized as a protobuf message
        """
        return TrackerMessage(
            name=self.name,
            type_index=_TRACKER_TYPE,  # todo plugin map or registry
            n=self.count,  # Use 'TrackerMessage.value.n' over embedding Counters message.
        )

    def to_summary(self) -> TrackerSummary:
        """
        Return the object serialized as a protobuf message
        """
        return TrackerSummary(
            name=self.name,
            type_index=_TRACKER_TYPE,  # todo plugin map or registry
            n=self.count,  # Use 'TrackerMessage.value.n' over embedding Counters message.
        )

    @staticmethod
    def from_protobuf(message: TrackerMessage) -> "CountersTracker":
        """
        Load from a protobuf message

        Returns
        -------
        counters : CountersTracker
        """
        if message.type_index != _TRACKER_TYPE:  # TODO plugin map or type registry
            raise ValueError(f"Cannot deserialize a counter tracker named {message.name} using type_index: {message.type_index}")
        return CountersTracker(count=message.n)
