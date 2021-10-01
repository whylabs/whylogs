import logging
import re
from typing import List, Mapping, Optional

from google.protobuf.json_format import Parse

from whylogs.proto import (
    DatasetConstraintMsg,
    DatasetProperties,
    NumberSummary,
    Op,
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
    Op.MATCH: lambda x: lambda v: re.match(x, v) is not None,
    Op.NOMATCH: lambda x: lambda v: re.match(x, v) is None,
}

_summary_funcs1 = {
    # functions that compare a summary field to a literal value.
    Op.LT: lambda f, v: lambda s: getattr(s, f) < v,
    Op.LE: lambda f, v: lambda s: getattr(s, f) <= v,
    Op.EQ: lambda f, v: lambda s: getattr(s, f) == v,
    Op.NE: lambda f, v: lambda s: getattr(s, f) != v,
    Op.GE: lambda f, v: lambda s: getattr(s, f) >= v,
    Op.GT: lambda f, v: lambda s: getattr(s, f) > v,
}

_summary_funcs2 = {
    # functions that compare two summary fields.
    Op.LT: lambda f, f2: lambda s: getattr(s, f) < getattr(s, f2),
    Op.LE: lambda f, f2: lambda s: getattr(s, f) <= getattr(s, f2),
    Op.EQ: lambda f, f2: lambda s: getattr(s, f) == getattr(s, f2),
    Op.NE: lambda f, f2: lambda s: getattr(s, f) != getattr(s, f2),
    Op.GE: lambda f, f2: lambda s: getattr(s, f) >= getattr(s, f2),
    Op.GT: lambda f, f2: lambda s: getattr(s, f) > getattr(s, f2),
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
            self.func = _value_funcs[op](regex_pattern)

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
    second_field :  (one-of)
        Name of second field in NumberSummary to be compared against summary field specified in `first_field`.
        Only one of `value` or `second_field` should be supplied.
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
        second_field: str = None,
        name: str = None,
        verbose=False,
    ):
        self._verbose = verbose
        self._name = name
        self.op = op
        self.first_field = first_field
        self.second_field = second_field
        self.total = 0
        self.failures = 0
        if value is not None and second_field is None:
            # field-value summary comparison
            self.value = value
            self.func = _summary_funcs1[op](first_field, value)
        elif second_field is not None and value is None:
            # field-field summary comparison
            self.second_field = second_field
            self.func = _summary_funcs2[op](first_field, second_field)
        else:
            raise ValueError("Summary constraint must specify a second value or field name, but not both")

    @property
    def name(self):
        return self._name if self._name is not None else f"summary {self.first_field} {Op.Name(self.op)} {self.value}/{self.second_field}"

    def update(self, summ: NumberSummary) -> bool:
        self.total += 1
        if not self.func(summ):
            self.failures += 1
            if self._verbose:
                logger.info(f"summary constraint {self.name} failed")

    @staticmethod
    def from_protobuf(msg: SummaryConstraintMsg) -> "SummaryConstraint":
        if msg.HasField("value") and not msg.HasField("second_field"):
            return SummaryConstraint(
                msg.first_field,
                msg.op,
                value=msg.value,
                name=msg.name,
                verbose=msg.verbose,
            )
        elif msg.HasField("second_field") and not msg.HasField("value"):
            return SummaryConstraint(
                msg.first_field,
                msg.op,
                second_field=msg.second_field,
                name=msg.name,
                verbose=msg.verbose,
            )
        else:
            raise ValueError("SummaryConstraintMsg must specify a value or second field name, but not both")

    def to_protobuf(self) -> SummaryConstraintMsg:
        if self.second_field is None:
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
    def __init__(self, constraints: List[ValueConstraint] = []):
        self.constraints = constraints

    @staticmethod
    def from_protobuf(msg: ValueConstraintMsgs) -> "ValueConstraints":
        v = [ValueConstraint.from_protobuf(c) for c in msg.constraints]
        if len(v) > 0:
            return ValueConstraints(v)
        return None

    def to_protobuf(self) -> ValueConstraintMsgs:
        v = [c.to_protobuf() for c in self.constraints]
        if len(v) > 0:
            vcmsg = ValueConstraintMsgs()
            vcmsg.constraints.extend(v)
            return vcmsg
        return None

    def update(self, v):
        for c in self.constraints:
            c.update(v)

    def report(self) -> List[tuple]:
        v = [c.report() for c in self.constraints]
        if len(v) > 0:
            return v
        return None


class SummaryConstraints:
    def __init__(self, constraints: List[SummaryConstraint]):
        self.constraints = constraints

    @staticmethod
    def from_protobuf(msg: SummaryConstraintMsgs) -> "SummaryConstraints":
        v = [SummaryConstraint.from_protobuf(c) for c in msg.constraints]
        if len(v) > 0:
            return SummaryConstraints(v)
        return None

    def to_protobuf(self) -> SummaryConstraintMsgs:
        v = [c.to_protobuf() for c in self.constraints]
        if len(v) > 0:
            scmsg = SummaryConstraintMsgs()
            scmsg.constraints.extend(v)
            return scmsg
        return None

    def update(self, v):
        for c in self.constraints:
            c.update(v)

    def report(self) -> List[tuple]:
        v = [c.report() for c in self.constraints]
        if len(v) > 0:
            return v
        return None


class DatasetConstraints:
    def __init__(
        self,
        props: DatasetProperties,
        value_constraints: Optional[Mapping[str, ValueConstraints]] = None,
        summary_constraints: Optional[Mapping[str, SummaryConstraints]] = None,
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
