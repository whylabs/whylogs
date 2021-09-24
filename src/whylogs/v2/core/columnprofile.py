"""
Defines the ColumnProfile class for tracking per-column statistics
"""
import logging
from typing import List, Optional

import pandas as pd

from whylogs.proto import (
    ColumnMessageV2,
    ColumnSummaryV2,
    InferredType,
    Op,
    UniqueCountSummary,
)
from whylogs.v2.core.statistics import (
    CountersTracker,
    NumberTracker,
    SchemaTracker,
    StringTracker,
)
from whylogs.v2.core.statistics.constraints import (
    SummaryConstraint,
    SummaryConstraints,
    ValueConstraints,
)
from whylogs.v2.core.statistics.hllsketch import HllSketch
from whylogs.v2.core.tracker import Tracker
from whylogs.v2.core.tracker_registry import TrackerRegistry
from whylogs.v2.util.dsketch import FrequentItemsSketch

_TYPES = InferredType.Type
logger = logging.getLogger(__name__)


class ColumnProfile:
    """
    Statistics tracking for a column (i.e. a feature)

    The primary method for

    Parameters
    ----------
    name : str (required)
        Name of the column profile
    trackers : list of trackers configured for this column
    constraints : ValueConstraints
        Static assertions to be applied to numeric data tracked in this column

    TODO:
        * Proper TypedDataConverter type checking
        * Multi-threading/parallelism
    """

    def __init__(self, name: str, trackers: List[Tracker] = None, strict_typing: bool = False, constraints: ValueConstraints = None, is_merge: bool = False):
        if constraints is None:
            constraints = ValueConstraints()
        # TODO simplify this API, for now store trackers by name for merging
        self._tracker_map = {}
        # Handle default trackers
        if trackers is None:
            schema_tracker = SchemaTracker()
            string_tracker = StringTracker()
            string_tracker.register_unique_count_for_string(schema_tracker)
            default_trackers = [
                HllSketch(),
                NumberTracker(),
                string_tracker,
                schema_tracker,
                CountersTracker(),
                FrequentItemsSketch(),
            ]
            self._initialize_trackers(default_trackers)
        else:
            self._initialize_trackers(trackers)

        # TODO: review if we need to preserve existing behavior around cardinality tracking
        self.unique_count_estimators = [tracker for tracker in self._tracker_map.values() if tracker.is_cardinality_tracker]

        # Assign values
        self.column_name = name
        self.strict_typing = strict_typing
        self.column_type = _TYPES.UNKNOWN
        self.constraints = constraints

        if not is_merge and trackers is not None:
            logger.info(f"Using custom set of trackers {self.get_tracker_names()} on column {self.column_name}")

    def _initialize_trackers(self, trackers: List[Tracker]):
        for tracker in trackers:
            if tracker:
                self._tracker_map[tracker.name] = tracker
            else:
                logger.warning(f"empty tracker was passed in for {self.name}")

    def track(self, value):
        """
        Add `value` to tracking statistics.
        """
        # TODO: factor into better structure with functions
        if self.strict_typing:
            if self.column_type == _TYPES.UNKNOWN and not pd.isnull([value]).all():
                self.column_type = type(value)
            for tracker in self._tracker_map.values():
                tracker.track(value, self.column_type)
        else:
            data_type = InferredType.Type.NULL if pd.isnull([value]).all() else type(value)
            for tracker in self._tracker_map.values():
                tracker.track(value, data_type)

        self.constraints.update(value)

    def _unique_count_summary(self) -> Optional[UniqueCountSummary]:
        cardinality_estimator = None
        if self.unique_count_estimators:
            cardinality_estimator = next(filter(lambda tracker: tracker.has_unique_count(), self.unique_count_estimators), None)
        if cardinality_estimator:
            return cardinality_estimator.get_unique_count_summary()
        else:
            logger.error(f"Possible missing data. No unique count estimators configured for this column: {self.column_name}")
        return cardinality_estimator

    def to_summary(self) -> ColumnSummaryV2:
        """
        Generate a summary of the statistics

        Returns
        -------
        summary : ColumnSummaryV2
            Protobuf summary message.
        """
        tracker_summaries = {}
        for tracker in self._tracker_map.values():
            tracker_summary = tracker.to_summary()
            if not tracker_summary:
                logger.debug(f"{tracker.name} produced no summary")
            else:
                tracker_summaries[tracker.name] = tracker_summary

        return ColumnSummaryV2(name=self.column_name, trackers=tracker_summaries)

    def generate_constraints(self) -> SummaryConstraints:
        items = []
        if self.number_tracker is not None and self.number_tracker.count > 0:
            summ = self.number_tracker.to_summary()
            if summ.min > 0:
                items = [SummaryConstraint(op=Op.GT, first_field="min", value=0)]
            # generate additional constraints here
            if len(items) > 0:
                return SummaryConstraints(items)

        return None

    def add_tracker(self, tracker: Tracker):
        name = tracker.name
        if name in self._tracker_map:
            raise ValueError(f"A tracker with name: {name} already exists on this column profile.")
        else:
            self._tracker_map[name] = tracker

    def get_tracker(self, tracker_name: str) -> Optional[Tracker]:
        return self._tracker_map.get(tracker_name, None)

    def get_tracker_names(self) -> List[str]:
        if self._tracker_map is None:
            return list()
        return list(self._tracker_map.keys())

    def merge(self, other, strict: bool = True):
        """
        Merge this columnprofile with another.

        Parameters
        ----------
        other : ColumnProfile

        Returns
        -------
        merged : ColumnProfile
            A new, merged column profile.
        """
        assert self.column_name == other.column_name
        merged_trackers = []
        if strict:
            assert len(self._tracker_map.values()) == len(
                other._tracker_map.values()
            ), f"trying to merge column profiles with {len(self._tracker_map.values())} and {len(other._tracker_map.values())} trackers."

        for name in self._tracker_map.keys():
            tracker = self.get_tracker(name)
            other_tracker = other.get_tracker(name)
            if strict:
                assert other.get_tracker(name) is not None, f"trying to merge column profiles but missing {name} from one of the profiles."
            merged_trackers.append(tracker.merge(other_tracker))

        return ColumnProfile(self.column_name, trackers=merged_trackers, is_merge=True)

    def to_protobuf(self) -> ColumnMessageV2:
        """
        Return the object serialized as a protobuf message

        Returns
        -------
        message : ColumnMessageV2
        """
        tracker_messages = {tracker.name: tracker.to_protobuf() for tracker in self._tracker_map.values()}
        if len(tracker_messages.values()) == 0:
            logger.warning(f"There are no trackers on this column profile, the ColumnMessageV2 will be mostly empty for {self.column_name}")

        return ColumnMessageV2(name=self.column_name, trackers=tracker_messages)

    @staticmethod
    def from_protobuf(message: ColumnMessageV2) -> "ColumnProfile":
        """
        Load from a protobuf message

        Returns
        -------
        column_profile : ColumnProfile
        """
        trackers = []
        column_name = None
        if not message.name:
            logger.error(f"Attempt to deserialize a column profile message without name property: {message}")
        else:
            column_name = message.name

        if not message.trackers:
            logger.warning(f"There are no trackers in this message, the column profile will not track anything: {message}")
        else:
            # TODO: consider passing in a registry to resolve protobuf messages
            trackers = [TrackerRegistry.from_protobuf(tracker_message) for tracker_message in message.trackers.values()]
        return ColumnProfile(
            name=column_name,
            trackers=trackers,
        )
