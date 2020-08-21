from google.protobuf.wrappers_pb2 import Int64Value

from whylogs.proto import Counters


class CountersTracker:
    """
    Class to keep track of the counts of various data types

    Parameters
    ----------
    count : int, optional
        Current number of objects
    true_count : int, optional
        Number of boolean values
    null_count : int, optional
        Number of nulls encountered
    """

    def __init__(self, count=0, true_count=0, null_count=0):
        self.count = count
        self.true_count = true_count
        self.null_count = null_count

    def increment_count(self):
        """
        Add one to the count of total objects
        """
        self.count += 1

    def increment_bool(self):
        """
        Add one to the boolean count
        """
        self.true_count += 1

    def increment_null(self):
        """
        Add one to the null count
        """
        self.null_count += 1

    def merge(self, other):
        """
        Merge another counter tracker with this one

        Returns
        -------
        new_tracker : CountersTracker
            The merged tracker
        """
        return CountersTracker(
            count=self.count + other.count,
            true_count=self.true_count + other.true_count,
            null_count=self.null_count + other.null_count,
        )

    def to_protobuf(self):
        """
        Return the object serialized as a protobuf message
        """
        opts = dict(count=self.count)
        if self.true_count > 0:
            opts["true_count"] = Int64Value(value=self.true_count)
        if self.null_count > 0:
            opts["null_count"] = Int64Value(value=self.null_count)
        return Counters(**opts)

    @staticmethod
    def from_protobuf(message: Counters):
        """
        Load from a protobuf message

        Returns
        -------
        counters : CountersTracker
        """
        return CountersTracker(
            count=message.count,
            true_count=message.true_count.value,
            null_count=message.null_count.value,
        )
