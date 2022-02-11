from whylogs.core.statistics.constraints import (
    ValueConstraints,
    containsCreditCardConstraint,
    containsEmailConstraint,
    containsSSNConstraint,
)


class PIITracker:
    def __init__(self, column_name, reporting_threshold=1):
        self.column_name = column_name
        self.reporting_threshold = reporting_threshold
        self.pii_constraints = ValueConstraints(
            [
                containsEmailConstraint(name=f"The feature \'{self.column_name}\' contains some values identified as e-mail addresses"),
                containsCreditCardConstraint(name=f"The feature \'{self.column_name}\' contains some values identified as credit card numbers"),
                containsSSNConstraint(name=f"The feature \'{self.column_name}\' contains some values identified as social security numbers"),
            ]
        )
        self.tracking = True

    def update(self, value):
        if self.tracking:
            self.pii_constraints.update(value)
            for name, constraint in self.pii_constraints.raw_value_constraints.items():
                if (constraint.total - constraint.failures) >= self.reporting_threshold:
                    self.tracking = False

    def give_pii_insights(self):
        pii_report = []
        for name, constraint in self.pii_constraints.raw_value_constraints.items():
            if (constraint.total - constraint.failures) >= self.reporting_threshold:
                pii_report.append(constraint.name)

        if len(pii_report) > 0:
            return pii_report
        else:
            return f"There was no information identified as PII in the column \'{self.column_name}\'"
