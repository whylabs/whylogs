import logging
import math

from datasketches import frequent_strings_sketch

from whylogs.core.statistics.thetasketch import ThetaSketch
from whylogs.core.summaryconverters import from_string_sketch
from whylogs.proto import StringsMessage, StringsSummary
from whylogs.util import dsketch

MAX_ITEMS_SIZE = 128
MAX_SUMMARY_ITEMS = 100

logger = logging.getLogger(__name__)


class StringTracker:
    """
    Track statistics for strings

    Parameters
    ----------
    count : int
        Total number of processed values
    items : frequent_strings_sketch
        Sketch for tracking string counts
    theta_sketch : ThetaSketch
        Sketch for approximate cardinality tracking
    """

    def __init__(
        self,
        count: int = None,
        items: frequent_strings_sketch = None,
        theta_sketch: ThetaSketch = None,
    ):
        if count is None:
            count = 0
        if items is None:
            items = frequent_strings_sketch(round(math.log(MAX_ITEMS_SIZE)))
        if theta_sketch is None:
            theta_sketch = ThetaSketch()
        self.count = count
        self.items = items
        self.theta_sketch = theta_sketch

    def update(self, value: str):
        """
        Add a string to the tracking statistics.

        If `value` is `None`, nothing will be done
        """
        if value is None:
            return

        self.count += 1
        self.theta_sketch.update(value)
        self.items.update(value)

    def merge(self, other):
        """
        Merge the values of this string tracker with another

        Parameters
        ----------
        other : StringTracker
            The other StringTracker

        Returns
        -------
        new : StringTracker
            Merged values
        """
        items_copy = frequent_strings_sketch.deserialize(self.items.serialize())
        items_copy.merge(other.items)

        new_theta = self.theta_sketch.merge(other.theta_sketch)
        count = self.count + other.count
        return StringTracker(count, items_copy, new_theta)

    def to_protobuf(self):
        """
        Return the object serialized as a protobuf message

        Returns
        -------
        message : StringsMessage
        """
        return StringsMessage(
            count=self.count,
            items=self.items.serialize(),
            compact_theta=self.theta_sketch.serialize(),
        )

    @staticmethod
    def from_protobuf(message: StringsMessage):
        """
        Load from a protobuf message

        Returns
        -------
        string_tracker : StringTracker
        """
        theta = None
        if message.compact_theta is not None and len(message.compact_theta) > 0:
            theta = ThetaSketch.deserialize(message.compact_theta)
        elif message.theta is not None and len(message.theta) > 0:
            logger.warning("Possible missing data. Non-compact theta sketches are no longer supported")

        return StringTracker(
            count=message.count,
            items=dsketch.deserialize_frequent_strings_sketch(message.items),
            theta_sketch=theta,
        )

    def to_summary(self):
        """
        Generate a summary of the statistics

        Returns
        -------
        summary : StringsSummary
            Protobuf summary message.
        """
        if self.count == 0:
            return None
        unique_count = self.theta_sketch.to_summary()
        opts = dict(
            unique_count=unique_count,
        )
        if unique_count.estimate < MAX_SUMMARY_ITEMS:
            frequent_strings = from_string_sketch(self.items)
            if frequent_strings is not None:
                opts["frequent"] = frequent_strings

        return StringsSummary(**opts)
