import logging

from whylogs.proto import TrackerMessage
from whylogs.v2.core.statistics import (
    CountersTracker,
    NumberTracker,
    SchemaTracker,
    StringTracker,
)
from whylogs.v2.core.statistics.hllsketch import HllSketch
from whylogs.v2.core.tracker import Tracker
from whylogs.v2.util.dsketch import FrequentItemsSketch

logger = logging.getLogger(__name__)


# TODO: define an abstract base class and then create a default registry and split this up.
class TrackerRegistry:
    """
    Define default tracker registry that resolves TrackerMessage type information into a concrete Tracker type and instance.
    """

    _TYPE_INDEX_MAP = dict(
        [
            (1, CountersTracker),
            (4, SchemaTracker),
            (5, NumberTracker),
            (6, StringTracker),
            (8, FrequentItemsSketch),
            (9, HllSketch),
        ]
    )

    @staticmethod
    def from_protobuf(message: TrackerMessage) -> Tracker:
        if hasattr(message, "type_index"):
            tracker_type = TrackerRegistry._TYPE_INDEX_MAP.get(message.type_index, None)
            if tracker_type:
                return tracker_type.from_protobuf(message)
            else:
                logger.debug(f"No tracker type found for {message.type_index} in {TrackerRegistry._TYPE_INDEX_MAP}")
        else:
            logger.error("Attempt to deserialize a message without a type_index property!")

        return None
