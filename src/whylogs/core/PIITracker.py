from whylogs.core.statistics.constraints import (
    ValueConstraints,
    containsCreditCardConstraint,
    containsEmailConstraint,
    containsSSNConstraint,
)
from whylogs.proto import PIIMessage


class PIITracker:
    def __init__(self, column_name, reporting_threshold=1):
        """
        Tracker for generating PII insights for a ColumnProfile

        Parameters
        ----------
        column_name: str
            Name of the column which the tracker corresponds to

        reporting_threshold: int
            Minimal number of observations required to make the PII conclusion

        """

        self.column_name = column_name
        self.reporting_threshold = reporting_threshold
        self.pii_constraints = ValueConstraints(
            [
                containsEmailConstraint(name=f"The feature \'{self.column_name}\' contains some values identified as e-mail addresses"),
                containsCreditCardConstraint(name=f"The feature \'{self.column_name}\' contains some values identified as credit card numbers"),
                containsSSNConstraint(name=f"The feature \'{self.column_name}\' contains some values identified as social security numbers"),
            ]
        )
        self._tracking = True

    def update(self, value: str):
        """
        Updates the pii constraints

        Parameters
        ----------
        value: str
            A string value for updating the pii constraints
        """

        if self._tracking:
            self.pii_constraints.update(value)
            for name, constraint in self.pii_constraints.raw_value_constraints.items():
                if (constraint.total - constraint.failures) >= self.reporting_threshold:
                    self._tracking = False

    def generate_pii_insights(self):
        """
        Generates PII insights for the current ColumnProfile

        Returns
        -------
            pii_report: list
                List of pii conclusions
        """

        pii_report = []
        for name, constraint in self.pii_constraints.raw_value_constraints.items():
            if (constraint.total - constraint.failures) >= self.reporting_threshold:
                pii_report.append(constraint.name)

        if len(pii_report) > 0:
            return pii_report
        else:
            return f"There was no information identified as PII in the column \'{self.column_name}\'"

    def merge(self, other):
        """
        Merge another counter tracker with this one

        Parameters
        -------
        other: PIITracker
            other PIITracker object to be merged with this

        Returns
        -------
        pii_tracker : PIITracker
            The merged tracker

        """

        if self.column_name != other.column_name:
            raise ValueError(f"Cannot merge two PIITracker objects for different columns:"
                             f" {self.column_name} and {other.column_name}")

        if self.reporting_threshold != other.reporting_threshold:
            raise ValueError(f"Cannot merge two PIITracker objects with different reporting thresholds: "
                             f"{self.reporting_threshold} and {other.reporting_threshold}")

        constraints = self.pii_constraints.merge(other.pii_constraints)

        pii_tracker = PIITracker(
            column_name=self.column_name,
            reporting_threshold=self.reporting_threshold
        )
        pii_tracker.pii_constraints = constraints
        pii_tracker._tracking = self._tracking if self._tracking is False else other._tracking
        return pii_tracker

    def to_protobuf(self):
        """
        Return the object serialized as a protobuf message
        """

        return PIIMessage(
            column_name=self.column_name,
            reporting_threshold=self.reporting_threshold,
            constraints=self.pii_constraints.to_protobuf(),
            tracking=self._tracking
        )


    @staticmethod
    def from_protobuf(message: PIIMessage):
        """
        Load from a protobuf message

        Returns
        -------
        pii_tracker : PIITracker
        """

        pii_tracker = PIITracker(
            column_name=message.column_name,
            reporting_threshold=message.reporting_threshold
        )

        constraints = ValueConstraints.from_protobuf(message.constraints)
        if constraints:
            pii_tracker.pii_constraints = constraints

        if message.tracking:
            pii_tracker._tracking = message.tracking

        return pii_tracker
