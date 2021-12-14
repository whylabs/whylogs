import logging
import re
from typing import List, Mapping, Optional

from google.protobuf.json_format import Parse

from whylogs.proto import (
    DatasetConstraintMsg,
    DatasetProperties,
    NumberSummary,
    Op,
    SummaryBetweenConstraintMsg,
    SummaryConstraintMsg,
    SummaryConstraintMsgs,
    ValueConstraintMsg,
    ValueConstraintMsgs,
)
from whylogs.util.protobuf import message_to_json

logger = logging.getLogger(__name__)

"""
Dict indexed by constraint operator.

These help translate from constraint schema to language-specific functions that are faster to evaluate.
This is just a form of currying, and I chose to bind the boolean comparison operator first.
"""
_value_funcs = {
    # functions that compare an incoming feature value to a literal value.
    Op.LT: lambda x: lambda v: v < x,  # assert incoming value 'v' is less than some fixed value 'x'
    Op.LE: lambda x: lambda v: v <= x,
    Op.EQ: lambda x: lambda v: v == x,
    Op.NE: lambda x: lambda v: v != x,
    Op.GE: lambda x: lambda v: v >= x,
    Op.GT: lambda x: lambda v: v > x,  # assert incoming value 'v' is greater than some fixed value 'x'
    Op.MATCH: lambda x: lambda v: x.match(v) is not None,
    Op.NOMATCH: lambda x: lambda v: x.match(v) is None,
}

_summary_funcs1 = {
    # functions that compare a summary field to a literal value.
    Op.LT: lambda f, v: lambda s: getattr(s, f) < v,
    Op.LE: lambda f, v: lambda s: getattr(s, f) <= v,
    Op.EQ: lambda f, v: lambda s: getattr(s, f) == v,
    Op.NE: lambda f, v: lambda s: getattr(s, f) != v,
    Op.GE: lambda f, v: lambda s: getattr(s, f) >= v,
    Op.GT: lambda f, v: lambda s: getattr(s, f) > v,
    Op.BTWN: lambda f, v1, v2: lambda s: v1 <= getattr(s, f) <= v2,
}

_summary_funcs2 = {
    # functions that compare two summary fields.
    Op.LT: lambda f, f2: lambda s: getattr(s, f) < getattr(s, f2),
    Op.LE: lambda f, f2: lambda s: getattr(s, f) <= getattr(s, f2),
    Op.EQ: lambda f, f2: lambda s: getattr(s, f) == getattr(s, f2),
    Op.NE: lambda f, f2: lambda s: getattr(s, f) != getattr(s, f2),
    Op.GE: lambda f, f2: lambda s: getattr(s, f) >= getattr(s, f2),
    Op.GT: lambda f, f2: lambda s: getattr(s, f) > getattr(s, f2),
    Op.BTWN: lambda f, f2, f3: lambda s: getattr(s, f2) <= getattr(s, f) <= getattr(s, f3),
}


class ValueConstraint:
    """
    ValueConstraints express a binary boolean relationship between an implied numeric value and a literal.
    When associated with a ColumnProfile, the relation is evaluated for every incoming value that is processed by whylogs.

    Parameters
    ----------
    op : whylogs.proto.Op (required)
        Enumeration of binary comparison operator applied between static value and incoming stream.
        Enum values are mapped to operators like '==', '<', and '<=', etc.
    value :  (required)
        Static value to compare against incoming stream using operator specified in `op`.
    name : str
        Name of the constraint used for reporting
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    """

    def __init__(self, op: Op, value=None, regex_pattern: str = None, name: str = None, verbose=False):
        self._name = name
        self._verbose = verbose
        self.op = op
        self.total = 0
        self.failures = 0
        if value is not None and regex_pattern is None:
            # numeric value
            self.value = value
            self.func = _value_funcs[op](value)

        elif regex_pattern is not None and value is None:
            # Regex pattern
            self.regex_pattern = regex_pattern
            self.func = _value_funcs[op](re.compile(self.regex_pattern))

        else:
            raise ValueError("Value constraint must specify a numeric value or regex pattern, but not both")

    @property
    def name(self):
        if getattr(self, "value", None):
            return self._name if self._name is not None else f"value {Op.Name(self.op)} {self.value}"
        else:
            return self._name if self._name is not None else f"value {Op.Name(self.op)} {self.regex_pattern}"

    def update(self, v) -> bool:
        self.total += 1
        if self.op in [Op.MATCH, Op.NOMATCH] and not isinstance(v, str):
            self.failures += 1
            if self._verbose:
                logger.info(f"value constraint {self.name} failed: value {v} not a string")
        elif not self.func(v):
            self.failures += 1
            if self._verbose:
                logger.info(f"value constraint {self.name} failed on value {v}")

    def merge(self, other) -> "ValueConstraint":
        if not other:
            return self
        assert self.name == other.name, f"Cannot merge constraints with different names: ({self.name}) and ({other.name})"
        assert self.op == other.op, f"Cannot merge constraints with different ops: {self.op} and {other.op}"
        assert self.value == other.value, f"Cannot merge value constraints with different values: {self.value} and {other.value}"
        merged_value_constraint = ValueConstraint(op=self.op, value=self.value, name=self.name, verbose=self._verbose)
        merged_value_constraint.total = self.total + other.total
        merged_value_constraint.failures = self.failures + other.failures
        return merged_value_constraint

    @staticmethod
    def from_protobuf(msg: ValueConstraintMsg) -> "ValueConstraint":
        return ValueConstraint(msg.op, msg.value, name=msg.name, verbose=msg.verbose)

    def to_protobuf(self) -> ValueConstraintMsg:
        if hasattr(self, "value"):
            return ValueConstraintMsg(
                name=self.name,
                op=self.op,
                value=self.value,
                verbose=self._verbose,
            )
        else:
            return ValueConstraintMsg(
                name=self.name,
                op=self.op,
                regex_pattern=self.regex_pattern,
                verbose=self._verbose,
            )

    def report(self):
        return (self.name, self.total, self.failures)


class SummaryConstraint:
    """
    Summary constraints specify a relationship between a summary field and a static value,
    or between two summary fields.
    e.g.     'min' < 6
             'std_dev' < 2.17
             'min' > 'avg'

    Parameters
    ----------
    first_field : str
        Name of field in NumberSummary that will be compared against either a second field or a static value.
    op : whylogs.proto.Op (required)
        Enumeration of binary comparison operator applied between summary values.
        Enum values are mapped to operators like '==', '<', and '<=', etc.
    value :  (one-of)
        Static value to be compared against summary field specified in `first_field`.
        Only one of `value` or `second_field` should be supplied.
    upper_value :  (one-of)
        Only to be supplied when using Op.BTWN.
        Static upper boundary value to be compared against summary field specified in `first_field`.
        Only one of `upper_value` or `third_field` should be supplied.
    second_field :  (one-of)
        Name of second field in NumberSummary to be compared against summary field specified in `first_field`.
        Only one of `value` or `second_field` should be supplied.
    third_field :  (one-of)
        Only to be supplied when op == Op.BTWN. Name of third field in NumberSummary, used as an upper boundary,
         to be compared against summary field specified in `first_field`.
        Only one of `upper_value` or `third_field` should be supplied.
    name : str
        Name of the constraint used for reporting
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.


    """

    def __init__(
        self,
        first_field: str,
        op: Op,
        value=None,
        upper_value=None,
        second_field: str = None,
        third_field: str = None,
        name: str = None,
        verbose=False,
    ):
        self._verbose = verbose
        self._name = name
        self.op = op
        self.first_field = first_field
        self.second_field = second_field
        self.third_field = third_field
        self.total = 0
        self.failures = 0

        self.value = value
        self.upper_value = upper_value

        if self.op == Op.BTWN:
            if value is not None and upper_value is not None and (second_field, third_field) == (None, None):
                # field-value summary comparison
                if not isinstance(value, (int, float)) or not isinstance(upper_value, (int, float)):
                    raise TypeError("When creating Summary constraint with BETWEEN operation, upper and lower value must be of type (int, float)")
                if value >= upper_value:
                    raise ValueError("Summary constraint with BETWEEN operation must specify lower value to be less than upper value")

                self.func = _summary_funcs1[self.op](first_field, value, upper_value)

            elif second_field is not None and third_field is not None and (value, upper_value) == (None, None):
                # field-field summary comparison
                if not isinstance(second_field, str) or not isinstance(third_field, str):
                    raise TypeError("When creating Summary constraint with BETWEEN operation, upper and lower field must be of type string")

                self.func = _summary_funcs2[self.op](first_field, second_field, third_field)
            else:
                raise ValueError("Summary constraint with BETWEEN operation must specify lower and upper value OR lower and third field name, but not both")
        else:
            if upper_value is not None or third_field is not None:
                raise ValueError("Summary constraint with other than BETWEEN operation must NOT specify upper value NOR third field name")

            if value is not None and second_field is None:
                # field-value summary comparison

                self.func = _summary_funcs1[op](first_field, value)
            elif second_field is not None and value is None:
                # field-field summary comparison

                self.func = _summary_funcs2[op](first_field, second_field)
            else:
                raise ValueError("Summary constraint must specify a second value or field name, but not both")

    @property
    def name(self):
        if self.op == Op.BTWN:
            lower_target = self.value if self.value is not None else self.second_field
            upper_target = self.upper_value if self.upper_value is not None else self.third_field
            return self._name if self._name is not None else f"summary {self.first_field} {Op.Name(self.op)} {lower_target} and {upper_target}"

        return self._name if self._name is not None else f"summary {self.first_field} {Op.Name(self.op)} {self.value}/{self.second_field}"

    def update(self, summ: NumberSummary) -> bool:
        self.total += 1
        if not self.func(summ):
            self.failures += 1
            if self._verbose:
                logger.info(f"summary constraint {self.name} failed")

    def merge(self, other) -> "SummaryConstraint":
        if not other:
            return self

        assert self.name == other.name, f"Cannot merge constraints with different names: ({self.name}) and ({other.name})"
        assert self.op == other.op, f"Cannot merge constraints with different ops: {self.op} and {other.op}"
        assert self.value == other.value, f"Cannot merge constraints with different values: {self.value} and {other.value}"
        assert self.first_field == other.first_field, f"Cannot merge constraints with different first_field: {self.first_field} and {other.first_field}"
        assert self.second_field == other.second_field, f"Cannot merge constraints with different second_field: {self.second_field} and {other.second_field}"

        if self.op == Op.BTWN:
            assert self.upper_value == other.upper_value, f"Cannot merge constraints with different upper values: {self.upper_value} and {other.upper_value}"
            assert self.third_field == other.third_field, f"Cannot merge constraints with different third_field: {self.third_field} and {other.third_field}"
            merged_constraint = SummaryConstraint(
                first_field=self.first_field,
                op=self.op,
                value=self.value,
                upper_value=self.upper_value,
                second_field=self.second_field,
                third_field=self.third_field,
                name=self.name,
                verbose=self._verbose,
            )
        else:
            merged_constraint = SummaryConstraint(
                first_field=self.first_field, op=self.op, value=self.value, second_field=self.second_field, name=self.name, verbose=self._verbose
            )

        merged_constraint.total = self.total + other.total
        merged_constraint.failures = self.failures + other.failures
        return merged_constraint

    @staticmethod
    def from_protobuf(msg: SummaryConstraintMsg) -> "SummaryConstraint":

        if msg.HasField("value") and not msg.HasField("second_field") and not msg.HasField("between"):
            return SummaryConstraint(
                msg.first_field,
                msg.op,
                value=msg.value,
                name=msg.name,
                verbose=msg.verbose,
            )
        elif msg.HasField("second_field") and not msg.HasField("value") and not msg.HasField("between"):
            return SummaryConstraint(
                msg.first_field,
                msg.op,
                second_field=msg.second_field,
                name=msg.name,
                verbose=msg.verbose,
            )
        elif msg.HasField("between") and not msg.HasField("value") and not msg.HasField("second_field"):
            if (
                msg.between.HasField("lower_value")
                and msg.between.HasField("upper_value")
                and not msg.between.HasField("second_field")
                and not msg.between.HasField("third_field")
            ):
                return SummaryConstraint(
                    msg.first_field,
                    msg.op,
                    value=msg.between.lower_value,
                    upper_value=msg.between.upper_value,
                    name=msg.name,
                    verbose=msg.verbose,
                )
            elif (
                msg.between.HasField("second_field")
                and msg.between.HasField("third_field")
                and not msg.between.HasField("lower_value")
                and not msg.between.HasField("upper_value")
            ):
                return SummaryConstraint(
                    msg.first_field,
                    msg.op,
                    second_field=msg.between.second_field,
                    third_field=msg.between.third_field,
                    name=msg.name,
                    verbose=msg.verbose,
                )
        else:
            raise ValueError("SummaryConstraintMsg must specify a value OR second field name OR SummaryBetweenConstraintMsg, but only one of them")

    def to_protobuf(self) -> SummaryConstraintMsg:
        if self.op == Op.BTWN:

            summary_between_constraint_msg = None
            if self.second_field is None and self.third_field is None:
                summary_between_constraint_msg = SummaryBetweenConstraintMsg(lower_value=self.value, upper_value=self.upper_value)
            else:
                summary_between_constraint_msg = SummaryBetweenConstraintMsg(second_field=self.second_field, third_field=self.third_field)

            msg = SummaryConstraintMsg(
                name=self.name,
                first_field=self.first_field,
                op=self.op,
                between=summary_between_constraint_msg,
                verbose=self._verbose,
            )

        elif self.second_field is None:
            msg = SummaryConstraintMsg(
                name=self.name,
                first_field=self.first_field,
                op=self.op,
                value=self.value,
                verbose=self._verbose,
            )
        else:
            msg = SummaryConstraintMsg(
                name=self.name,
                first_field=self.first_field,
                op=self.op,
                second_field=self.second_field,
                verbose=self._verbose,
            )
        return msg

    def report(self):
        return (self.name, self.total, self.failures)


class ValueConstraints:
    def __init__(self, constraints: Mapping[str, ValueConstraint] = None):
        if constraints is None:
            constraints = dict()

        if isinstance(constraints, list):
            self.constraints = {constraint.name: constraint for constraint in constraints}
        else:
            self.constraints = constraints

    @staticmethod
    def from_protobuf(msg: ValueConstraintMsgs) -> "ValueConstraints":
        value_constraints = [ValueConstraint.from_protobuf(c) for c in msg.constraints]
        if len(value_constraints) > 0:
            return ValueConstraints({v.name: v for v in value_constraints})
        return None

    def __getitem__(self, name: str) -> Optional[ValueConstraint]:
        if self.contraints:
            return self.constraints.get(name)
        return None

    def to_protobuf(self) -> ValueConstraintMsgs:
        v = [c.to_protobuf() for c in self.constraints.values()]
        if len(v) > 0:
            vcmsg = ValueConstraintMsgs()
            vcmsg.constraints.extend(v)
            return vcmsg
        return None

    def update(self, v):
        for c in self.constraints.values():
            c.update(v)

    def merge(self, other) -> "ValueConstraints":
        if not other or not other.constraints:
            return self

        merged_constraints = other.constraints.copy()
        for name, constraint in self.constraints:
            merged_constraints[name] = constraint.merge(other.constraints.get(name))

        return ValueConstraints(merged_constraints)

    def report(self) -> List[tuple]:
        v = [c.report() for c in self.constraints.values()]
        if len(v) > 0:
            return v
        return None


class SummaryConstraints:
    def __init__(self, constraints: Mapping[str, SummaryConstraint] = None):
        if constraints is None:
            constraints = dict()

        # Support list of constraints for back compat with previous version.
        if isinstance(constraints, list):
            self.constraints = {constraint.name: constraint for constraint in constraints}
        else:
            self.constraints = constraints

    @staticmethod
    def from_protobuf(msg: SummaryConstraintMsgs) -> "SummaryConstraints":
        constraints = [SummaryConstraint.from_protobuf(c) for c in msg.constraints]
        if len(constraints) > 0:
            return SummaryConstraints({v.name: v for v in constraints})
        return None

    def __getitem__(self, name: str) -> Optional[SummaryConstraint]:
        if self.contraints:
            return self.constraints.get(name)
        return None

    def to_protobuf(self) -> SummaryConstraintMsgs:
        v = [c.to_protobuf() for c in self.constraints.values()]
        if len(v) > 0:
            scmsg = SummaryConstraintMsgs()
            scmsg.constraints.extend(v)
            return scmsg
        return None

    def update(self, v):
        for c in self.constraints.values():
            c.update(v)

    def merge(self, other) -> "SummaryConstraints":

        if not other or not other.constraints:
            return self

        merged_constraints = other.constraints.copy()
        for name, constraint in self.constraints:
            merged_constraints[name] = constraint.merge(other.constraints.get(name))

        return SummaryConstraints(merged_constraints)

    def report(self) -> List[tuple]:
        v = [c.report() for c in self.constraints.values()]
        if len(v) > 0:
            return v
        return None


class DatasetConstraints:
    def __init__(
        self,
        props: DatasetProperties,
        value_constraints: Optional[ValueConstraints] = None,
        summary_constraints: Optional[SummaryConstraints] = None,
    ):
        self.dataset_properties = props
        # repackage lists of constraints if necessary
        if value_constraints is None:
            value_constraints = dict()
        for k, v in value_constraints.items():
            if isinstance(v, list):
                value_constraints[k] = ValueConstraints(v)
        self.value_constraint_map = value_constraints
        if summary_constraints is None:
            summary_constraints = dict()
        for k, v in summary_constraints.items():
            if isinstance(v, list):
                summary_constraints[k] = SummaryConstraints(v)
        self.summary_constraint_map = summary_constraints

    def __getitem__(self, key):
        if key in self.value_constraint_map:
            return self.value_constraint_map[key]
        return None

    @staticmethod
    def from_protobuf(msg: DatasetConstraintMsg) -> "DatasetConstraints":
        vm = dict([(k, ValueConstraints.from_protobuf(v)) for k, v in msg.value_constraints.items()])
        sm = dict([(k, SummaryConstraints.from_protobuf(v)) for k, v in msg.summary_constraints.items()])
        return DatasetConstraints(msg.properties, vm, sm)

    @staticmethod
    def from_json(data: str) -> "DatasetConstraints":
        msg = Parse(data, DatasetConstraintMsg())
        return DatasetConstraints.from_protobuf(msg)

    def to_protobuf(self) -> DatasetConstraintMsg:
        # construct tuple for each column, (name, [constraints,...])
        # turn that into a map indexed by column name
        vm = dict([(k, v.to_protobuf()) for k, v in self.value_constraint_map.items()])
        sm = dict([(k, s.to_protobuf()) for k, s in self.summary_constraint_map.items()])
        return DatasetConstraintMsg(
            properties=self.dataset_properties,
            value_constraints=vm,
            summary_constraints=sm,
        )

    def to_json(self) -> str:
        return message_to_json(self.to_protobuf())

    def report(self):
        l1 = [(k, v.report()) for k, v in self.value_constraint_map.items()]
        l2 = [(k, s.report()) for k, s in self.summary_constraint_map.items()]
        return l1 + l2


def stddevBetweenConstraint(lower_value=None, upper_value=None, lower_field=None, upper_field=None, verbose=False):
    return SummaryConstraint("stddev", Op.BTWN, value=lower_value, upper_value=upper_value, second_field=lower_field, third_field=upper_field, verbose=verbose)


def meanBetweenConstraint(lower_value=None, upper_value=None, lower_field=None, upper_field=None, verbose=False):
    return SummaryConstraint("mean", Op.BTWN, value=lower_value, upper_value=upper_value, second_field=lower_field, third_field=upper_field, verbose=verbose)


def minBetweenConstraint(lower_value=None, upper_value=None, lower_field=None, upper_field=None, verbose=False):
    return SummaryConstraint("min", Op.BTWN, value=lower_value, upper_value=upper_value, second_field=lower_field, third_field=upper_field, verbose=verbose)


def minGreaterThanEqualConstraint(value=None, field=None, verbose=False):
    return SummaryConstraint("min", Op.GE, value=value, second_field=field, verbose=verbose)


def maxBetweenConstraint(lower_value=None, upper_value=None, lower_field=None, upper_field=None, verbose=False):
    return SummaryConstraint("max", Op.BTWN, value=lower_value, upper_value=upper_value, second_field=lower_field, third_field=upper_field, verbose=verbose)


def maxLessThanEqualConstraint(value=None, field=None, verbose=False):
    return SummaryConstraint("max", Op.LE, value=value, second_field=field, verbose=verbose)
