import logging
import math
import unicodedata
from collections import defaultdict
from typing import Callable, Dict, List

from datasketches import frequent_strings_sketch

from whylogs.core.statistics.thetasketch import ThetaSketch
from whylogs.core.summaryconverters import from_string_sketch
from whylogs.proto import (
    CharBlockMessage,
    CharBlockSummary,
    CharPosMessage,
    CharPosSummary,
    StringsMessage,
    StringsSummary,
    UnicodeBlockEntry,
)
from whylogs.util import dsketch

from .numbertracker import NumberTracker

MAX_ITEMS_SIZE = 128
MAX_SUMMARY_ITEMS = 100

logger = logging.getLogger(__name__)


class CharPosTracker:
    """
    Track statistics for character positions within a string

    Parameters
    ----------
    character_list : str
        string containing all characters to be tracked
        this list can include specific unicode characters to track.


    """

    def __init__(self, character_list: str = None):

        if character_list is None:
            character_list = "abcdefghijklmnopqrstuvwzyz0123456789-+_@!,./?#$%^&*()[]{}"
        self.character_list = set(character_list)
        self.char_pos_map = {}

    def update(self, value: str, character_list: str = None) -> None:
        """update

        Parameters
        ----------
        value : str
            utf-16 string
        character_list : str, optional
            use a specific character_list for
            the tracked string. Note that modifing
            it from a previous saved choice, will
            reset the character position map, since
            NITL no longer has the same context.
        """
        if character_list:
            char_set = set(character_list)
            if char_set != self.character_list:
                # check if any character were previously tracked
                if not self.char_pos_map:
                    logger.warning("Changing character list, a non-empty character position tracker is being reset to remove ambiguities")
                self.character_list = char_set
                self.char_pos_map = {}

        for indx, char in enumerate(value.lower()):
            try:

                if char in self.character_list:
                    self.char_pos_map.setdefault(char, NumberTracker())
                    # print("track")
                    self.char_pos_map[char].track(indx)
                else:
                    self.char_pos_map.setdefault("NITL", NumberTracker())
                    self.char_pos_map["NITL"].track(indx)
            except UnicodeEncodeError:
                # print("exception")
                self.char_pos_map.setdefault("NITL", NumberTracker())
                self.char_pos_map["NITL"].track(indx)

    def merge(self, other: "CharPosTracker") -> "CharPosTracker":
        """
        Merges two Char Pos Frequency Maps

        Args:

            other (CharPosTracker): to be merged

        """
        if (self.character_list != other.character_list) and (not self.char_pos_map or not other.char_pos_map):
            logger.error("Merging two non-empty Character position tracker with different character lists")

        new_character_list = self.character_list.union(other.character_list)

        # initialize merged
        new_char_pos_tracker = CharPosTracker(character_list=str("".join(list(new_character_list))))

        # merge
        new_char_pos_map = {}
        for character in new_character_list:
            pos_tracker = self.char_pos_map.get(character, None)
            other_tracker = other.char_pos_map.get(character, None)

            if pos_tracker and other_tracker:
                new_char_pos_map[character] = pos_tracker.merge(other_tracker)
            elif pos_tracker:
                new_char_pos_map[character] = pos_tracker
            elif other_tracker:
                new_char_pos_map[character] = other_tracker

        # merge not in the list
        nitl_tracker = self.char_pos_map.get("NITL", None)
        nitl_other_tracker = other.char_pos_map.get("NITL", None)

        if nitl_tracker and nitl_other_tracker:
            new_char_pos_map["NITL"] = nitl_tracker.merge(nitl_other_tracker)
        elif nitl_tracker:
            new_char_pos_map["NITL"] = nitl_tracker
        elif nitl_other_tracker:
            new_char_pos_map["NITL"] = nitl_other_tracker

        new_char_pos_tracker.char_pos_map = new_char_pos_map

        return new_char_pos_tracker

    def to_protobuf(self):
        """
        Return the object serialized as a protobuf message
        """
        character_list = list(self.character_list)
        character_list.sort()
        opts = dict(char_list="".join(character_list), char_pos_map={key: nt.to_protobuf() for key, nt in self.char_pos_map.items()})

        msg = CharPosMessage(**opts)

        return msg

    @staticmethod
    def from_protobuf(message: CharPosMessage):
        """
        Load from a CharPosMessage protobuf message

        Returns
        -------
        CharPosTracker
        """

        opts = dict(character_list=message.char_list)
        char_pos_tracker = CharPosTracker(**opts)

        for each_key, each_value in message.char_pos_map.items():
            char_pos_tracker.char_pos_map[each_key] = NumberTracker.from_protobuf(each_value)

        return char_pos_tracker

    def to_summary(self):
        character_list = list(self.character_list)
        character_list.sort()
        opts = dict(character_list="".join(character_list), char_pos_map={key: nt.to_summary() for key, nt in self.char_pos_map.items()})

        return CharPosSummary(**opts)


class CharBlockTracker:
    """
    Track statistics for character blocks and properties within a string

    Parameters
    ----------
    block_list : Dict[str, List[int]]
        Map from block name to block range to track


    """

    def __init__(self, block_list: Dict[str, List[int]] = None):

        if block_list is None:
            # might want to make this a multimap or make each value a list of ranges
            # maybe make the defualt {} so nothing's tracked to match old StringTracker behaviour
            block_list = {
                "emoji": [0x1F600, 0x1F64F],
                "basic-latin": [0x0000, 0x007F],
                "extended-latin": [0x0080, 0x02AF],
            }
        # validate list length is (multiple of) 2
        self.block_list = block_list
        self.char_block_map = defaultdict(NumberTracker)

    def update(self, value: str) -> None:  # pass in block_list?
        # value must be unicodedata.normalize()-able, not necessarily utf-16
        """update

        Parameters
        ----------
        value : str
            utf-16 string
        """
        block_counter = defaultdict(int)
        # need to transform to utf-32 or handle surrogates
        for char in unicodedata.normalize("NFD", value).lower():
            for block_name, block_limits in self.block_list.items():
                if block_limits[0] <= ord(char) <= block_limits[1]:
                    block_counter[block_name] += 1
                # do we want to track chars not in any block?
        for block_name, block_count in block_counter.items():
            self.char_block_map[block_name].track(block_count)

    def merge(self, other: "CharBlockTracker") -> "CharBlockTracker":
        """
        Merges two Char Block Maps

        Args:

            other (CharBlockTracker): to be merged

        """
        # probably need lots of validation here :)
        new_block_list = {**self.block_list, **other.block_list}

        # initialize merged
        new_char_block_tracker = CharBlockTracker(new_block_list)

        # merge
        new_char_block_map = defaultdict(NumberTracker)
        for block_name in new_block_list.keys():
            block_tracker = self.char_block_map.get(block_name, None)
            other_tracker = other.char_block_map.get(block_name, None)
            if block_tracker and other_tracker:
                new_char_block_map[block_name] = block_tracker.merge(other_tracker)
            elif block_tracker:
                new_char_block_map[block_name] = block_tracker
            elif other_tracker:
                new_char_block_map[block_name] = other_tracker

        # merge not in the list?

        new_char_block_tracker.char_block_map = new_char_block_map
        return new_char_block_tracker

    def to_protobuf(self):
        """
        Return the object serialized as a protobuf message
        """
        block_list = {
            block_name: UnicodeBlockEntry(block_start=block_range[0], block_end=block_range[1]) for block_name, block_range in self.block_list.items()
        }

        opts = dict(block_list=block_list, char_block_map={key: nt.to_protobuf() for key, nt in self.char_block_map.items()})

        msg = CharBlockMessage(**opts)

        return msg

    @staticmethod
    def from_protobuf(message: CharBlockMessage):
        """
        Load from a CharBlockMessage protobuf message

        Returns
        -------
        CharBlockTracker
        """

        char_block_tracker = CharBlockTracker(dict())
        for each_key, each_value in message.block_list.items():
            char_block_tracker.block_list[each_key] = [each_value.block_start, each_value.block_end]

        for each_key, each_value in message.char_block_map.items():
            char_block_tracker.char_block_map[each_key] = NumberTracker.from_protobuf(each_value)

        return char_block_tracker

    def to_summary(self):
        block_list = {
            block_name: UnicodeBlockEntry(block_start=block_range[0], block_end=block_range[1]) for block_name, block_range in self.block_list.items()
        }

        opts = dict(block_list=block_list, char_block_map={key: nt.to_summary() for key, nt in self.char_block_map.items()})

        msg = CharBlockSummary(**opts)

        return msg


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
    token_length :  NumberTracker
        counts token per sentence
    token_method : funtion
        method used to turn string into tokens
    char_pos_tracker: CharPosTracker
    char_block_tracker: CharBlockTracker
    """

    def __init__(
        self,
        count: int = None,
        items: frequent_strings_sketch = None,
        theta_sketch: ThetaSketch = None,
        length: NumberTracker = None,
        token_length: NumberTracker = None,
        char_pos_tracker: CharPosTracker = None,
        token_method: Callable[[], List[str]] = None,
        char_block_tracker: CharBlockTracker = None,
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

        self.char_block_tracker = char_block_tracker if char_block_tracker else CharBlockTracker()

    def update(self, value: str, character_list=None, token_method=None):
        """
        Add a string to the tracking statistics.

        If `value` is `None`, nothing will be done
        """
        if value is None:
            return

        self.count += 1
        self.theta_sketch.update(value)
        self.items.update(value)

        self.char_pos_tracker.update(value, character_list)
        self.char_block_tracker.update(value)  # block map?
        if token_method:
            self.token_method = token_method
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
        new_token_length = self.token_length.merge(other.token_length)
        new_char_pos_tracker = self.char_pos_tracker.merge(other.char_pos_tracker)
        new_char_block_tracker = self.char_block_tracker.merge(other.char_block_tracker)  # block map?

        return StringTracker(count, items_copy, new_theta, new_length, new_token_length, new_char_pos_tracker, self.token_method, new_char_block_tracker)

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
            length=self.length.to_protobuf() if self.length else None,
            token_length=self.token_length.to_protobuf() if self.token_length else None,
            char_pos_tracker=self.char_pos_tracker.to_protobuf() if self.char_pos_tracker else None,
            char_block_tracker=self.char_block_tracker.to_protobuf() if self.char_block_tracker else None,
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
            length=NumberTracker.from_protobuf(message.length),
            token_length=NumberTracker.from_protobuf(message.token_length),
            char_pos_tracker=CharPosTracker.from_protobuf(message.char_pos_tracker),
            char_block_tracker=CharBlockTracker.from_protobuf(message.char_block_tracker),
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
            length=self.length.to_summary(),
            token_length=self.token_length.to_summary(),
            char_pos_tracker=self.char_pos_tracker.to_summary(),
            char_block_tracker=self.char_block_tracker.to_summary(),
        )
        if unique_count.estimate < MAX_SUMMARY_ITEMS:
            frequent_strings = from_string_sketch(self.items)
            if frequent_strings is not None:
                opts["frequent"] = frequent_strings

        return StringsSummary(**opts)
