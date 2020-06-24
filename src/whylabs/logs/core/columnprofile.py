#!/usr/bin/env python3
"""
created 5/7/20 by ibackus
"""
from whylabs.logs.core.types import TypedDataConverter
from whylabs.logs.core.statistics import NumberTracker
from whylabs.logs.core.statistics import from_number_tracker
from whylabs.logs.proto import ColumnSummary, ColumnMessage, InferredType
from whylabs.logs.core.statistics import CountersTracker, SchemaTracker
from whylabs.logs.core.statistics.datatypes import StringTracker
_TYPES = InferredType.Type
_NUMERIC_TYPES = {_TYPES.FRACTIONAL, _TYPES.INTEGRAL}


class ColumnProfile:
    """
    Statistics tracking for a column

    Parameters
    ----------
    name : str (required)
        Name of the column profile
    number_tracker : NumberTracker
        Implements numeric data statisics tracking
    string_tracker : StringTracker
        Implements string data-type statistics tracking
    schema_tracker : SchemaTracker
        Implements tracking of schema-related information
    counters : CountersTracker
        Keep count of various things

    TODO:
        * Proper TypedDataConverter type checking
        * Multi-threading/parallelism
    """

    def __init__(self, name, number_tracker=None, string_tracker=None,
                 schema_tracker=None, counters=None):
        # Handle default values
        if counters is None:
            counters = CountersTracker()
        if number_tracker is None:
            number_tracker = NumberTracker()
        if string_tracker is None:
            string_tracker = StringTracker()
        if schema_tracker is None:
            schema_tracker = SchemaTracker()
        # Assign values
        self.column_name = name
        self.number_tracker = number_tracker
        self.string_tracker = string_tracker
        self.schema_tracker = schema_tracker
        self.counters = counters

    def track(self, value):
        """
        Add `value` to tracking statistics.
        """
        self.counters.increment_count()
        if value is None:
            self.counters.increment_null()
            return

        # TODO: ignore this if we already know the data type
        if isinstance(value, str):
            self.string_tracker.update(value)

        # TODO: Implement real typed data conversion
        typed_data = TypedDataConverter.convert(value)
        dtype = TypedDataConverter.get_type(typed_data)
        self.schema_tracker.track(dtype)

        if isinstance(typed_data, bool):
            # Note: bools are sub-classes of ints in python, so we should check
            # for bool type first
            self.counters.increment_bool()
        elif isinstance(typed_data, (float, int)):
            self.number_tracker.track(typed_data)

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
        opts = dict(
            counters=self.counters.to_protobuf()
        )
        if schema is not None:
            opts['schema'] = schema
            dtype = schema.inferred_type.type
            if dtype == _TYPES.STRING:
                if self.string_tracker is not None:
                    string_summary = self.string_tracker.to_summary()
                    opts['string_summary'] = string_summary

            elif dtype in _NUMERIC_TYPES:
                numbers_summary = from_number_tracker(self.number_tracker)
                if numbers_summary is not None:
                    opts['number_summary'] = numbers_summary
        return ColumnSummary(**opts)

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
        )

    @staticmethod
    def from_protobuf(message):
        """
        Load from a protobuf message

        Returns
        -------
        column_profile : ColumnProfile
        """
        return ColumnProfile(
            message.name,
            counters=CountersTracker.from_protobuf(message.counters),
            schema_tracker=SchemaTracker.from_protobuf(message.schema),
            number_tracker=NumberTracker.from_protobuf(message.numbers),
            string_tracker=StringTracker.from_protobuf(message.strings),
        )
