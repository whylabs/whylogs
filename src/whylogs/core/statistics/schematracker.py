import copy

from whylogs.proto import InferredType, SchemaMessage, SchemaSummary

Type = InferredType.Type


class SchemaTracker:
    """
    Track information about a column's schema and present datatypes

    Paramters
    ---------
    type_counts : dict
        If specified, a dictionary containing information about the counts of
        all data types.
    """

    UNKNOWN_TYPE = InferredType(type=Type.UNKNOWN)
    NULL_TYPE = InferredType(type=Type.NULL, ratio=1.0)
    CANDIDATE_MIN_FRAC = 0.7

    def __init__(self, type_counts: dict = None):
        if type_counts is None:
            type_counts = {}
        if not isinstance(type_counts, dict):
            # Assume we have a protobuf object
            type_counts = {k: v for k, v in type_counts.items()}
        self.type_counts = type_counts

    def _non_null_type_counts(self):
        type_counts = self.type_counts.copy()
        if Type.NULL in type_counts:
            type_counts.pop(Type.NULL)
        return type_counts

    def track(self, item_type):
        """
        Track an item type
        """
        try:
            self.type_counts[item_type] += 1
        except KeyError:
            self.type_counts[item_type] = 1

    def get_count(self, item_type):
        """
        Return the count of a given item type
        """
        return self.type_counts.get(item_type, 0)

    def infer_type(self):
        """
        Generate a guess at what type the tracked values are.

        Returns
        -------
        type_guess : object
            The guess tome.  See `InferredType.Type` for candidates
        """
        null_count = self.get_count(Type.NULL)
        type_counts = self._non_null_type_counts()
        total_count = sum(type_counts.values())
        if total_count == 0:
            if null_count > 0:
                return SchemaTracker.NULL_TYPE
            else:
                return SchemaTracker.UNKNOWN_TYPE

        candidate = self._get_most_popular_type(total_count)
        if candidate.ratio > SchemaTracker.CANDIDATE_MIN_FRAC:
            return candidate

        # Integral is considered a subset of fractional here
        fractional_count = sum([type_counts.get(k, 0) for k in (Type.INTEGRAL, Type.FRACTIONAL)])

        if candidate.type == Type.STRING and type_counts.get(Type.STRING, 0) > fractional_count:
            # treat everything else as "String" except UNKNOWN
            coerced_count = sum([type_counts.get(k, 0) for k in (Type.INTEGRAL, Type.FRACTIONAL, Type.STRING, Type.BOOLEAN)])
            actual_ratio = float(coerced_count) / total_count
            return InferredType(type=Type.STRING, ratio=actual_ratio)

        if candidate.ratio >= 0.5:
            # Not a string, but something else with a majority
            actual_count = type_counts[candidate.type]
            if candidate.type == Type.FRACTIONAL:
                actual_count = fractional_count
            return InferredType(type=candidate.type, ratio=float(actual_count) / total_count)

        fractional_ratio = float(fractional_count) / total_count
        if fractional_ratio >= 0.5:
            return InferredType(type=Type.FRACTIONAL, ratio=fractional_ratio)

        # Otherwise, assume everything is the candidate type
        return InferredType(type=candidate.type, ratio=1.0)

    def _get_most_popular_type(self, total_count):
        item_type = Type.UNKNOWN
        type_counts = self._non_null_type_counts()
        count = -1
        for candidate_type, candidate_count in type_counts.items():
            if candidate_count > count:
                item_type = candidate_type
                count = candidate_count

        ratio = float(count) / total_count
        return InferredType(type=item_type, ratio=ratio)

    def merge(self, other):
        """
        Merge another schema tracker with this and return a new one.
        Does not alter this object.

        Parameters
        ----------
        other : SchemaTracker

        Returns
        -------
        merged : SchemaTracker
            Merged tracker
        """
        this_copy = self.copy()
        all_types = Type.values()
        for t in all_types:
            if (t in self.type_counts) or (t in other.type_counts):
                this_copy.type_counts[t] = this_copy.type_counts.get(t, 0) + other.type_counts.get(t, 0)
        return this_copy

    def copy(self):
        """
        Return a copy of this tracker
        """
        type_counts = copy.copy(self.type_counts)
        return SchemaTracker(type_counts)

    def to_protobuf(self):
        """
        Return the object serialized as a protobuf message

        Returns
        -------
        message : SchemaMessage
        """
        return SchemaMessage(typeCounts=self.type_counts)

    @staticmethod
    def from_protobuf(message):
        """
        Load from a protobuf message

        Returns
        -------
        schema_tracker : SchemaTracker
        """
        return SchemaTracker(type_counts=message.typeCounts)

    def to_summary(self):
        """
        Generate a summary of the statistics

        Returns
        -------
        summary : SchemaSummary
            Protobuf summary message.
        """
        type_counts = self.type_counts
        # Convert the integer keys to their corresponding string names
        type_counts_with_names = {Type.Name(k): v for k, v in type_counts.items()}
        return SchemaSummary(
            inferred_type=self.infer_type(),
            type_counts=type_counts_with_names,
        )
