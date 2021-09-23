"""
Defines the ColumnProfile class for tracking per-column statistics
"""
from whylogs.src.whylogs.v2.core.statistics.stringtracker import _STRING_TRACKER_TYPE
import pandas as pd

from typing import List

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
from whylogs.proto import (
    ColumnMessage,
    ColumnSummary,
    InferredType,
    Op,
    UniqueCountSummary,
)
from whylogs.util.dsketch import FrequentItemsSketch

_TYPES = InferredType.Type

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

    def __init__(
        self,
        name: str,
        trackers: List[Tracker] = None,
        strict_typing: bool = False,
        constraints: ValueConstraints = None,
    ):

        # Handle default trackers
        if trackers is None:
            trackers = [
                HllSketch(),
                NumberTracker(),
                StringTracker(),
                SchemaTracker(),
                CountersTracker(),
                FrequentItemsSketch(),
            ]

        # TODO: simplify this further, for now preserve existing behavior around cardinality tracking
        self.unique_count_estimators = [tracker for tracker in trackers if tracker.is_cardinality_tracker]

        if constraints is None:
            constraints = ValueConstraints()

        # Assign values
        self.column_name = name
        self.strict_typing = strict_typing
        self.column_type = _TYPES.UNKNOWN
        self.constraints = constraints
        self.unique_count_estimators = [tracker for tracker in self.trackers if isinstance(tracker, (HllSketch, StringTracker, NumberTracker))]

    def track(self, value, character_list=None, token_method=None):
        """
        Add `value` to tracking statistics.
        """
        # TODO: factor into better structure with functions
        if self.strict_typing:
            if self.column_type == _TYPES.UNKNOWN and not pd.isnull([value]).all():
                self.column_type = type(value)
            for tracker in self.trackers:
                tracker.track(value, self.column_type)
        else: 
            data_type = InferredType.Type.NULL if pd.isnull([value]).all() else type(value)
            for tracker in self.trackers:
                tracker.track(value, data_type)

        self.constraints.update(value, data_type)

    def _unique_count_summary(self) -> UniqueCountSummary:
        if self.trackers
        cardinality_summary = self.cardinality_tracker.to_summary()
        if cardinality_summary:
            return cardinality_summary

        inferred_type = self.schema_tracker.infer_type()
        if inferred_type.type == _TYPES.STRING:
            cardinality_summary = self.string_tracker.theta_sketch.to_summary()
        else:  # default is number summary
            cardinality_summary = self.number_tracker.theta_sketch.to_summary()
        return cardinality_summary

    def to_summary(self):
        """
        Generate a summary of the statistics

        Returns
        -------
        summary : ColumnSummary
            Protobuf summary message.
        """
        schema = None
        if self.schema_tracker is not None:
            schema = self.schema_tracker.to_summary()
        # TODO: implement the real schema/type checking
        null_count = self.schema_tracker.get_count(InferredType.Type.NULL)
        opts = dict(
            counters=self.counters.to_protobuf(null_count=null_count),
            frequent_items=self.frequent_items.to_summary(),
            unique_count=self._unique_count_summary(),
        )
        if self.string_tracker is not None and self.string_tracker.count > 0:
            opts["string_summary"] = self.string_tracker.to_summary()
        if self.number_tracker is not None and self.number_tracker.count > 0:
            opts["number_summary"] = self.number_tracker.to_summary()

        if schema is not None:
            opts["schema"] = schema

        return ColumnSummary(**opts)

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

    def merge(self, other):
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
        return ColumnProfile(
            self.column_name,
            number_tracker=self.number_tracker.merge(other.number_tracker),
            string_tracker=self.string_tracker.merge(other.string_tracker),
            schema_tracker=self.schema_tracker.merge(other.schema_tracker),
            counters=self.counters.merge(other.counters),
            frequent_items=self.frequent_items.merge(other.frequent_items),
            cardinality_tracker=self.cardinality_tracker.merge(other.cardinality_tracker),
        )

    def to_protobuf(self):
        """
        Return the object serialized as a protobuf message

        Returns
        -------
        message : ColumnMessage
        """

        return ColumnMessage(
            name=self.column_name,
            counters=self.counters.to_protobuf(),
            schema=self.schema_tracker.to_protobuf(),
            numbers=self.number_tracker.to_protobuf(),
            strings=self.string_tracker.to_protobuf(),
            frequent_items=self.frequent_items.to_protobuf(),
            cardinality_tracker=self.cardinality_tracker.to_protobuf(),
        )

    @staticmethod
    def from_protobuf(message):
        """
        Load from a protobuf message

        Returns
        -------
        column_profile : ColumnProfile
        """
        schema_tracker = SchemaTracker.from_protobuf(message.schema, legacy_null_count=message.counters.null_count.value)
        return ColumnProfile(
            message.name,
            counters=(CountersTracker.from_protobuf(message.counters)),
            schema_tracker=schema_tracker,
            number_tracker=NumberTracker.from_protobuf(message.numbers),
            string_tracker=StringTracker.from_protobuf(message.strings),
            frequent_items=FrequentItemsSketch.from_protobuf(message.frequent_items),
            cardinality_tracker=HllSketch.from_protobuf(message.cardinality_tracker),
        )
