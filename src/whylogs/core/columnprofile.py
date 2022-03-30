"""
Defines the ColumnProfile class for tracking per-column statistics
"""

from whylogs.core.statistics import (
    CountersTracker,
    NumberTracker,
    SchemaTracker,
    StringTracker,
)
from whylogs.core.statistics.constraints import (
    MultiColumnValueConstraints,
    SummaryConstraints,
    ValueConstraints,
    columnMostCommonValueInSetConstraint,
    columnUniqueValueCountBetweenConstraint,
    columnValuesTypeEqualsConstraint,
    maxLessThanEqualConstraint,
    meanBetweenConstraint,
    minGreaterThanEqualConstraint,
)
from whylogs.core.statistics.hllsketch import HllSketch
from whylogs.core.types import TypedDataConverter
from whylogs.proto import ColumnMessage, ColumnSummary, InferredType, UniqueCountSummary
from whylogs.util.dsketch import FrequentItemsSketch

_TYPES = InferredType.Type
_NUMERIC_TYPES = {_TYPES.FRACTIONAL, _TYPES.INTEGRAL}
_UNIQUE_COUNT_BOUNDS_STD = 1


class ColumnProfile:
    """
    Statistics tracking for a column (i.e. a feature)

    The primary method for

    Parameters
    ----------
    name : str (required)
        Name of the column profile
    number_tracker : NumberTracker
        Implements numeric data statistics tracking
    string_tracker : StringTracker
        Implements string data-type statistics tracking
    schema_tracker : SchemaTracker
        Implements tracking of schema-related information
    counters : CountersTracker
        Keep count of various things
    frequent_items : FrequentItemsSketch
        Keep track of all frequent items, even for mixed datatype features
    cardinality_tracker : HllSketch
        Track feature cardinality (even for mixed data types)
    constraints : ValueConstraints
        Static assertions to be applied to numeric data tracked in this column

    TODO:
        * Proper TypedDataConverter type checking
        * Multi-threading/parallelism
    """

    def __init__(
        self,
        name: str,
        number_tracker: NumberTracker = None,
        string_tracker: StringTracker = None,
        schema_tracker: SchemaTracker = None,
        counters: CountersTracker = None,
        frequent_items: FrequentItemsSketch = None,
        cardinality_tracker: HllSketch = None,
        constraints: ValueConstraints = None,
    ):
        # Handle default values
        if counters is None:
            counters = CountersTracker()
        if number_tracker is None:
            number_tracker = NumberTracker()
        if string_tracker is None:
            string_tracker = StringTracker()
        if schema_tracker is None:
            schema_tracker = SchemaTracker()
        if frequent_items is None:
            frequent_items = FrequentItemsSketch()
        if cardinality_tracker is None:
            cardinality_tracker = HllSketch()
        if constraints is None:
            constraints = ValueConstraints()

        # Assign values
        self.column_name = name
        self.number_tracker = number_tracker
        self.string_tracker = string_tracker
        self.schema_tracker = schema_tracker
        self.counters = counters
        self.frequent_items = frequent_items
        self.cardinality_tracker = cardinality_tracker
        self.constraints = constraints

    def track(self, value, character_list=None, token_method=None):
        """
        Add `value` to tracking statistics.
        """
        self.counters.increment_count()
        if TypedDataConverter._are_nulls(value):
            self.schema_tracker.track(InferredType.Type.NULL)
            return

        if TypedDataConverter._is_array_like(value):
            return

        # TODO: ignore this if we already know the data type
        if isinstance(value, str):
            self.string_tracker.update(value, character_list=character_list, token_method=token_method)
        # TODO: Implement real typed data conversion

        self.constraints.update(value)

        typed_data = TypedDataConverter.convert(value)

        if not TypedDataConverter._are_nulls(typed_data):
            self.cardinality_tracker.update(typed_data)
            self.frequent_items.update(typed_data)
        dtype = TypedDataConverter.get_type(typed_data)
        self.schema_tracker.track(dtype)

        if isinstance(typed_data, bool):
            # Note: bools are sub-classes of ints in python, so we should check
            # for bool type first
            self.counters.increment_bool()

        # prevent errors in number tracker on unknown types
        if TypedDataConverter._is_array_like(typed_data) or dtype == _TYPES.UNKNOWN:
            return

        self.number_tracker.track(typed_data)

        self.constraints.update_typed(typed_data)

    def _unique_count_summary(self) -> UniqueCountSummary:
        cardinality_summary = self.cardinality_tracker.to_summary(_UNIQUE_COUNT_BOUNDS_STD)
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

            if summ.min >= 0:
                items.append(minGreaterThanEqualConstraint(value=0))

            mean_lower = summ.mean - summ.stddev
            mean_upper = summ.mean + summ.stddev

            if mean_lower != mean_upper:
                items.append(
                    meanBetweenConstraint(
                        lower_value=mean_lower,
                        upper_value=mean_upper,
                    )
                )

            if summ.max <= 0:
                items.append(maxLessThanEqualConstraint(value=0))

        schema_summary = self.schema_tracker.to_summary()
        inferred_type = schema_summary.inferred_type.type
        if inferred_type not in (InferredType.UNKNOWN, InferredType.NULL):
            items.append(columnValuesTypeEqualsConstraint(expected_type=inferred_type))

        if self.cardinality_tracker and inferred_type != InferredType.FRACTIONAL:
            unique_count = self.cardinality_tracker.to_summary()
            if unique_count and unique_count.estimate > 0:
                low = int(max(0, unique_count.lower - 1))
                up = int(unique_count.upper + 1)
                items.append(
                    columnUniqueValueCountBetweenConstraint(
                        lower_value=low,
                        upper_value=up,
                    )
                )

        frequent_items_summary = self.frequent_items.to_summary(max_items=5)
        if frequent_items_summary and len(frequent_items_summary.items) > 0:
            most_common_value_set = {val.json_value for val in frequent_items_summary.items}
            items.append(columnMostCommonValueInSetConstraint(value_set=most_common_value_set))

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


class MultiColumnProfile:
    """
    Statistics tracking for a multiple columns (i.e. a features)

    The primary method for

    Parameters
    ----------
    constraints : MultiColumnValueConstraints
        Static assertions to be applied to data tracked between all columns

    """

    def __init__(
        self,
        constraints: MultiColumnValueConstraints = None,
    ):

        self.constraints = constraints or MultiColumnValueConstraints()

    def track(self, column_dict, character_list=None, token_method=None):
        """
        TODO: Add `column_dict` to tracking statistics.
        """

        # update the MultiColumnTrackers code

        self.constraints.update(column_dict)
        self.constraints.update_typed(column_dict)

    def to_summary(self):
        """
        Generate a summary of the statistics

        Returns
        -------
        summary : (Multi)ColumnSummary
            Protobuf summary message.
        """

        # TODO: summaries for the multi column trackers and statistics

        raise NotImplementedError()

    def merge(self, other) -> "MultiColumnProfile":
        """
        Merge this columnprofile with another.

        Parameters
        ----------
        other : MultiColumnProfile

        Returns
        -------
        merged : MultiColumnProfile
            A new, merged multi column profile.
        """
        return MultiColumnProfile(self.constraints.merge(other.constraints))

    def to_protobuf(self):
        """
        Return the object serialized as a protobuf message

        Returns
        -------
        message : ColumnMessage
        """

        # TODO: implement new type of multicolumn message
        raise NotImplementedError()

    @staticmethod
    def from_protobuf(message):
        """
        Load from a protobuf message

        Returns
        -------
        column_profile : MultiColumnProfile
        """
        # TODO: implement new type of multicolumn message

        raise NotImplementedError()
