import logging
import math

from datasketches import frequent_strings_sketch

from whylogs.core.statistics.thetasketch import ThetaSketch
from whylogs.core.summaryconverters import from_string_sketch
from whylogs.proto import CharPosMessage, CharPosSummary, StringsMessage, StringsSummary
from whylogs.util import dsketch
from whylogs.util.util_functions import encode_to_integers

from .numbertracker import NumberTracker

MAX_ITEMS_SIZE = 128
MAX_SUMMARY_ITEMS = 100

logger = logging.getLogger(__name__)


class CharPosTracker:
    def __init__(self, character_list: str = "abcdefghijklmnopqrstuvwzyz0123456789-@!#$%^&*()[]{\}"):

        self.character_list = set(character_list)

        self.char_pos_map = {}

    def update(self, value):
        for indx, char in enumerate(value.lower()):

            try: 
                char=char.encode("ascii")
                if char in self.character_list:
                    self.char_pos_map.setdefault(char, NumberTracker())
                    self.char_pos_map[char].track(indx)
                else:
                    self.char_pos_map.setdefault("NITL", NumberTracker())
                    self.char_pos_map["NITL"].track(indx)
            except UnicodeEncodeError:
                self.char_pos_map.setdefault("NITL", NumberTracker())
                self.char_pos_map["NITL"].track(indx)


    def merge(self, other):
        """
        Merges two Char Pos Frequency Maps

        Args:

            other (CharPosTracker): to be merged

        """
        new_character_list = self.character_list.union(other.character_list)

        # initialize merged
        new_char_pos_tracker = CharPosTracker(character_list="".join(list(new_character_list)))

        # merge

        return new_char_pos_tracker

    def to_protobuf(self):
        """
        Return the object serialized as a protobuf message
        """
        opts = dict(
            char_list="".join(list(self.character_list)),
            char_pos_map=[nt.to_protobuf() for key, nt in self.char_pos_map.items()],
        )

        msg = CharPosMessage(**opts)
        return msg

    @staticmethod
    def from_protobuf(self, message: CharPosMessage):
        """
        Load from a protobuf message

        Returns
        -------
        CharPosTracker
        """

        opts = dict(
            character_list=CharPosMessage.char_list,
        )
        char_pos_tracker = CharPosTracker(**opts)
        char_pos_tracker.char_pos_map = [nt.from_protobuf for nt in CharPosMessage.char_pos_map]
        return char_pos_tracker

    def to_summary(
        self,
    ):
        opts = dict(character_list="".join(list(self.character_list)), char_pos_map={key: nt.to_summary() for key, nt in self.char_pos_map.items()})

        return CharPosSummary(**opts)


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
    length : NumberTracker
        tracks the distribution of length of strings
    token_length
    """

    def __init__(
        self,
        count: int = None,
        items: frequent_strings_sketch = None,
        theta_sketch: ThetaSketch = None,
        length: NumberTracker = None,
        token_length: NumberTracker = None,
        char_pos_tracker=None,
        token_method=None,
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

        self.char_pos_tracker = char_pos_tracker if char_pos_tracker else CharPosTracker()

        self.length = length if length else NumberTracker()

        self.token_length = token_length if token_length else NumberTracker()

        self.token_method = token_method if token_method else lambda x: x.split(" ")

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
        self.char_pos_tracker.update(value)
        self.length.track(len(value))
        self.token_length.track(len(self.token_method(value)))

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

        new_length = self.length.merge(other.length)
        new_token_length = self.token_length.merge(other.length)
        new_char_pos_tracker = self.char_pos_tracker.merge(other.char_pos_tracker)

        return StringTracker(count, items_copy, new_theta, new_length, new_token_length, new_char_pos_tracker)

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
            length=self.length.to_protobuf(),
            token_length=self.token_length.to_protobuf(),
            char_pos_tracker=self.char_pos_tracker.to_protobuf(),
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
            length=message.length,
            token_length=message.token_length,
        )

    def to_summary(
        self,
    ):
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
            length=self.length.to_summary(),
            token_length=self.token_length.to_summary(),
            char_pos_tracker=self.char_pos_tracker.to_summary(),
        )
        if unique_count.estimate < MAX_SUMMARY_ITEMS:
            frequent_strings = from_string_sketch(self.items)
            if frequent_strings is not None:
                opts["frequent"] = frequent_strings

        return StringsSummary(**opts)
