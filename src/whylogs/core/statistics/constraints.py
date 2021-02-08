import logging
from typing import List, Mapping, Optional

from whylogs.proto import (
    DatasetConstraintMsg,
    DatasetProperties,
    Op,
    SummaryConstraintMsg,
    SummaryConstraintMsgs,
    ValueConstraintMsg,
    ValueConstraintMsgs,
    NumberSummary,
)

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
    These are applied to every incoming value that is processed by whylogs.
    """

    def __init__(self, op: Op, value, name: str = None, verbose=False):
        self._name = name
        self._verbose = verbose
        self.op = op
        self.value = value
        self.func = _value_funcs[op](value)
        self.total = 0
        self.failures = 0

    @property
    def name(self):
        return self._name if self._name is not None else f'value {Op.Name(self.op)} {self.value}'

    def track(self, v) -> bool:
        self.total += 1
        if not self.func(v):
            self.failures += 1
            if self._verbose:
                logger.info(f'value constraint {self.name} failed on value {v}')

    @staticmethod
    def from_protobuf(msg: ValueConstraintMsg) -> 'ValueConstraint':
        return ValueConstraint(msg.op, msg.value, name=msg.name)

    def to_protobuf(self) -> ValueConstraintMsg:
        return ValueConstraintMsg(
            name=self.name,
            op=self.op,
            value=self.value,
        )

    def report(self):
        return (self.name, self.total, self.failures)


class SummaryConstraint:
    """
    Summary constraints specify a relationship between a summary field and a literal value,
    or between two summary fields.
    e.g.     'min' < 6
             'std_dev' < 2.17
             'min' > 'avg'
    """

    def __init__(self, field: str, op: Op, value=None, field2: str = None, name: str = None, verbose=False):
        self._verbose = verbose
        self._name = name
        self.op = op
        self.field = field
        self.field2 = field2
        self.total = 0
        self.failures = 0
        if value is not None and field2 is None:
            # field-value summary comparison
            self.value = value
            self.func = _summary_funcs1[op](field, value)
        elif field2 is not None and value is None:
            # field-field summary comparison
            self.field2 = field2
            self.func = _summary_funcs2[op](field, field2)
        else:
            raise ValueError("Summary constraint must specify a second value or field name, but not both")

    @property
    def name(self):
        return self._name if self._name is not None else f'summary {self.field} {Op.Name(self.op)} {self.value}/{self.field2}'

    def track(self, summ: NumberSummary) -> bool:
        self.total += 1
        if not self.func(summ):
            self.failures += 1
            if self._verbose:
                logger.info(f'summary constraint {self.name} failed')

    @staticmethod
    def from_protobuf(msg: SummaryConstraintMsg) -> 'SummaryConstraint':
        return ValueConstraint(msg.op, msg.value, name=msg.name)

    def to_protobuf(self) -> SummaryConstraintMsg:
        if self.field2 is None:
            msg = SummaryConstraintMsg(
                name=self.name,
                field=self.field,
                op=self.op,
                value=self.value,
            )
        else:
            msg = SummaryConstraintMsg(
                name=self.name,
                field=self.field,
                op=self.op,
                field2=self.field2,
            )
        return msg

    def report(self):
        return (self.name, self.total, self.failures)


class ValueConstraints:
    def __init__(self, constraints: List[ValueConstraint] = []):
        self.constraints = constraints

    @staticmethod
    def from_protobuf(msg: ValueConstraintMsgs) -> 'ValueConstraints':
        v = [ValueConstraint.from_proto(c) for c in msg.constraints]
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

    def add_constraint(self, c: ValueConstraint):
        self.constraints.append(c)

    def track(self, v):
        for c in self.constraints:
            c.track(v)

    def report(self) -> List[tuple]:
        v = [c.report() for c in self.constraints]
        if len(v) > 0:
            return v
        return None


class SummaryConstraints:
    def __init__(self, constraints: List[SummaryConstraint]):
        self.constraints = constraints

    @staticmethod
    def from_protobuf(msg: SummaryConstraintMsgs) -> 'SummaryConstraints':
        v = [SummaryConstraint.from_proto(c) for c in msg.constraints]
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

    def add_constraint(self, c: SummaryConstraint):
        self.constraints.append(c)

    def track(self, v):
        for c in self.constraints:
            c.track(v)

    def report(self) -> List[tuple]:
        v = [c.report() for c in self.constraints]
        if len(v) > 0:
            return v
        return None


class DatasetConstraints:
    def __init__(self,
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
    def from_protobuf(msg: DatasetConstraintMsg) -> 'DatasetConstraints':
        vm = dict([(k, ValueConstraints.from_protobuf(v)) for k, v in msg.value_constraints.items()])
        sm = dict([(k, SummaryConstraints.from_protobuf(v)) for k, v in msg.summary_constraints.items()])
        return DatasetConstraints(msg.properties, vm, sm)

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

    def report(self):
        l1 = [(k, v.report()) for k, v in self.value_constraint_map.items()]
        l2 = [(k, s.report()) for k, s in self.summary_constraint_map.items()]
        l1.extend(l2)
        return l1
