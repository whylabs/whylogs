import datetime
import json
import logging
import numbers
import re
from typing import Any, List, Mapping, Optional, Set, Union

import jsonschema
from datasketches import theta_a_not_b, update_theta_sketch
from dateutil.parser import parse
from google.protobuf.json_format import Parse
from google.protobuf.struct_pb2 import ListValue
from jsonschema import validate

from whylogs.core.summaryconverters import single_quantile_from_sketch
from whylogs.proto import (
    ApplyFunctionMsg,
    DatasetConstraintMsg,
    DatasetProperties,
    Op,
    SummaryBetweenConstraintMsg,
    SummaryConstraintMsg,
    SummaryConstraintMsgs,
    ValueConstraintMsg,
    ValueConstraintMsgs,
)
from whylogs.util.protobuf import message_to_json

logger = logging.getLogger(__name__)


def _try_parse_strftime_format(strftime_val: str, format: str) -> Optional[datetime.datetime]:
    """
    Return whether the string is in a strftime format.

    :param strftime_val: str, string to check for date
    :param format: format to check if strftime_val can be parsed
    :return None if not parseable, otherwise the parsed datetime.datetime object

    """
    parsed = None
    try:
        parsed = datetime.datetime.strptime(strftime_val, format)
    except (ValueError, TypeError):
        pass
    return parsed


def _try_parse_dateutil(dateutil_val: str, ref_val=None) -> Optional[datetime.datetime]:
    """
    Return whether the string can be interpreted as a date.

    :param dateutil_val: str, string to check for date
    :param ref_val: any, not used, interface design requirement
    :return None if not parseable, otherwise the parsed datetime.datetime object

    """
    parsed = None
    try:
        parsed = parse(dateutil_val)
    except (ValueError, TypeError):
        pass
    return parsed


def _try_parse_json(json_string: str, ref_val=None) -> Optional[dict]:
    """
    Return whether the string can be interpreted as json.

    :param json_string: str, string to check for json
    :param ref_val: any, not used, interface design requirement
    :return None if not parseable, otherwise the parsed json object
    """
    parsed = None
    try:
        parsed = json.loads(json_string)
    except (ValueError, TypeError):
        pass
    return parsed


def _matches_json_schema(json_data: Union[str, dict], json_schema: Union[str, dict]) -> bool:
    """
    Return whether the provided json matches the provided schema.

    :param json_data: json object to check
    :param json_schema: schema to check if the json object matches it
    :return True if the json data matches the schema, False otherwise
    """
    if isinstance(json_schema, str):
        try:
            json_schema = json.loads(json_schema)
        except (ValueError, TypeError):
            return False

    if isinstance(json_data, str):
        try:
            json_data = json.loads(json_data)
        except (ValueError, TypeError):
            return False

    try:
        validate(instance=json_data, schema=json_schema)
    except (jsonschema.exceptions.ValidationError, jsonschema.exceptions.SchemaError):
        return False
    return True


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
    Op.IN_SET: lambda x: lambda v: v in x,
    Op.APPLY_FUNC: lambda apply_function, reference_value: lambda v: apply_function(v, reference_value),
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
    Op.IN_SET: lambda f, ref_str_sketch, ref_num_sketch: lambda update_obj: round(
        theta_a_not_b().compute(getattr(update_obj, f)["string_theta"], ref_str_sketch).get_estimate(), 1
    )
    == round(theta_a_not_b().compute(getattr(update_obj, f)["number_theta"], ref_num_sketch).get_estimate(), 1)
    == 0.0,
    Op.CONTAIN_SET: lambda f, ref_str_sketch, ref_num_sketch: lambda update_obj: round(
        theta_a_not_b().compute(ref_str_sketch, getattr(update_obj, f)["string_theta"]).get_estimate(), 1
    )
    == round(theta_a_not_b().compute(ref_num_sketch, getattr(update_obj, f)["number_theta"]).get_estimate(), 1)
    == 0.0,
    Op.EQ_SET: lambda f, ref_str_sketch, ref_num_sketch: lambda update_obj: round(
        theta_a_not_b().compute(getattr(update_obj, f)["string_theta"], ref_str_sketch).get_estimate(), 1
    )
    == round(theta_a_not_b().compute(getattr(update_obj, f)["number_theta"], ref_num_sketch).get_estimate(), 1)
    == round(theta_a_not_b().compute(ref_str_sketch, getattr(update_obj, f)["string_theta"]).get_estimate(), 1)
    == round(theta_a_not_b().compute(ref_num_sketch, getattr(update_obj, f)["number_theta"]).get_estimate(), 1)
    == 0.0,
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
    value : (one-of)
        When value is provided, regex_pattern must be None.
        Static value to compare against incoming stream using operator specified in `op`.
    regex_pattern : (one-of)
        When regex_pattern is provided, value must be None.
        Regex pattern to use when MATCH or NOMATCH operations are used.
    apply_function:
        To be supplied only when using APPLY_FUNC operation.
        In case when the apply_function requires argument, to be supplied in the value param.
    name : str
        Name of the constraint used for reporting
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    """

    def __init__(self, op: Op, value=None, regex_pattern: str = None, apply_function=None, name: str = None, verbose=False):
        self._name = name
        self._verbose = verbose
        self.op = op
        self.apply_function = apply_function
        self.total = 0
        self.failures = 0

        if (apply_function is not None) != (self.op == Op.APPLY_FUNC):
            raise ValueError("A function must be provided if and only if using the APPLY_FUNC operator")

        if (isinstance(value, set) and op != Op.IN_SET) or (not isinstance(value, set) and op == Op.IN_SET):
            raise ValueError("Value constraint must provide a set of values for using the IN operator")

        if self.op == Op.APPLY_FUNC:
            if value is not None:
                value = self.apply_func_validate(value)
                self.value = value
            self.func = _value_funcs[op](apply_function, value)

        elif value is not None and regex_pattern is None:
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
        if self.op == Op.APPLY_FUNC:
            return self._name if self._name is not None else f"value {Op.Name(self.op)} {self.apply_function.__name__}"
        elif getattr(self, "value", None) is not None:
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

    def apply_func_validate(self, value) -> str:
        if not isinstance(value, str):
            if self.apply_function == _matches_json_schema:
                try:
                    value = json.dumps(value)
                except (ValueError, TypeError):
                    raise ValueError("Json schema invalid. When matching json schema, the schema provided must be valid.")
            else:
                value = str(value)
        return value

    def merge(self, other) -> "ValueConstraint":
        if not other:
            return self
        val = None
        pattern = None
        assert self.name == other.name, f"Cannot merge constraints with different names: ({self.name}) and ({other.name})"
        assert self.op == other.op, f"Cannot merge constraints with different ops: {self.op} and {other.op}"
        assert (
            self.apply_function == other.apply_function
        ), f"Cannot merge constraints with different apply_function: {self.apply_function} and {other.apply_function}"
        if self.apply_function is not None:
            if hasattr(self, "value") != hasattr(other, "value"):
                raise TypeError("Cannot merge one constraint with provided value and one without")
            elif hasattr(self, "value") and hasattr(other, "value"):
                val = self.value
                assert self.value == other.value, f"Cannot merge value constraints with different values: {self.value} and {other.value}"
        elif all([getattr(v, "value", None) is not None for v in (self, other)]):
            val = self.value
            assert self.value == other.value, f"Cannot merge value constraints with different values: {self.value} and {other.value}"
        elif all([getattr(v, "regex_pattern", None) for v in (self, other)]):
            pattern = self.regex_pattern
            assert (
                self.regex_pattern == other.regex_pattern
            ), f"Cannot merge value constraints with different values: {self.regex_pattern} and {other.regex_pattern}"
        else:
            raise TypeError("Cannot merge a numeric value constraint with a string value constraint")

        merged_value_constraint = ValueConstraint(
            op=self.op, value=val, regex_pattern=pattern, apply_function=self.apply_function, name=self.name, verbose=self._verbose
        )
        merged_value_constraint.total = self.total + other.total
        merged_value_constraint.failures = self.failures + other.failures
        return merged_value_constraint

    @staticmethod
    def from_protobuf(msg: ValueConstraintMsg) -> "ValueConstraint":
        if msg.HasField("function"):
            val = None if msg.function.reference_value == "" else msg.function.reference_value
            return ValueConstraint(msg.op, value=val, apply_function=globals()[msg.function.function], name=msg.name, verbose=msg.verbose)
        elif msg.regex_pattern != "":
            return ValueConstraint(msg.op, regex_pattern=msg.regex_pattern, name=msg.name, verbose=msg.verbose)
        elif len(msg.value_set.values) != 0:
            val_set = set(msg.value_set.values[0].list_value)
            return ValueConstraint(msg.op, value=val_set, name=msg.name, verbose=msg.verbose)
        else:
            return ValueConstraint(msg.op, msg.value, name=msg.name, verbose=msg.verbose)

    def to_protobuf(self) -> ValueConstraintMsg:
        if self.op == Op.APPLY_FUNC:
            func_msg = ApplyFunctionMsg(function=self.apply_function.__name__)
            if hasattr(self, "value"):
                func_msg = ApplyFunctionMsg(function=self.apply_function.__name__, reference_value=self.value)
            return ValueConstraintMsg(
                name=self.name,
                op=self.op,
                function=func_msg,
                verbose=self._verbose,
            )
        elif hasattr(self, "value"):
            if isinstance(self.value, set):
                set_vals_message = ListValue()
                set_vals_message.append(list(self.value))
                return ValueConstraintMsg(
                    name=self.name,
                    op=self.op,
                    value_set=set_vals_message,
                    verbose=self._verbose,
                )
            else:
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
    reference_set : (one-of)
        Only to be supplied when using set operations. Used as a reference set to be compared with the column
        distinct values.
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
        quantile_value: Union[int, float] = None,
        second_field: str = None,
        third_field: str = None,
        reference_set=None,
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
        self.quantile_value = quantile_value

        if self.first_field == "quantile" and not self.quantile_value:
            raise ValueError("Summary quantile constraint must specify quantile value")

        if self.first_field != "quantile" and self.quantile_value is not None:
            raise ValueError("Summary constraint applied on non-quantile field should not specify quantile value")

        if self.op in (Op.IN_SET, Op.CONTAIN_SET, Op.EQ_SET):
            if any([value, upper_value, second_field, third_field, not reference_set]):
                raise ValueError("When using set operations only set should be provided and not values or field names!")

            self.reference_set = reference_set
            reference_set = self.try_cast_set()

            self.ref_string_set, self.ref_numbers_set = self.get_string_and_numbers_sets()

            self.reference_theta_sketch = self.create_theta_sketch()
            self.string_theta_sketch = self.create_theta_sketch(self.ref_string_set)
            self.numbers_theta_sketch = self.create_theta_sketch(self.ref_numbers_set)

            self.func = _summary_funcs1[self.op](first_field, self.string_theta_sketch, self.numbers_theta_sketch)

        elif self.op == Op.BTWN:
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
        if self.first_field == "quantile":
            field_name = f"{self.first_field} {self.quantile_value}"
        else:
            field_name = self.first_field
        if self.op in (Op.IN_SET, Op.CONTAIN_SET, Op.EQ_SET):
            reference_set_str = ""
            if len(self.reference_set) > 20:
                tmp_set = set(list(self.reference_set)[:20])
                reference_set_str = f"{str(tmp_set)[:-1]}, ...}}"
            else:
                reference_set_str = str(self.reference_set)
            return self._name if self._name is not None else f"summary {self.first_field} {Op.Name(self.op)} {reference_set_str}"
        elif self.op == Op.BTWN:
            lower_target = self.value if self.value is not None else self.second_field
            upper_target = self.upper_value if self.upper_value is not None else self.third_field
            return self._name if self._name is not None else f"summary {field_name} {Op.Name(self.op)} {lower_target} and {upper_target}"

        return self._name if self._name is not None else f"summary {field_name} {Op.Name(self.op)} {self.value}/{self.second_field}"

    def try_cast_set(self) -> Set[Any]:
        if not isinstance(self.reference_set, set):
            try:
                logger.warning(f"Trying to cast provided value of {type(self.reference_set)} to type set!")
                self.reference_set = set(self.reference_set)
            except TypeError:
                provided_type_name = self.reference_set.__class__.__name__
                raise TypeError(f"When using set operations, provided value must be set or set castable, instead type: '{provided_type_name}' was provided!")
        return self.reference_set

    def get_string_and_numbers_sets(self):
        string_set = set()
        numbers_set = set()
        for item in self.reference_set:
            if isinstance(item, str):
                string_set.add(item)
            elif isinstance(item, numbers.Real) and not isinstance(item, bool):
                numbers_set.add(item)

        return string_set, numbers_set

    def create_theta_sketch(self, ref_set: set = None):
        theta = update_theta_sketch()
        target_set = self.reference_set if ref_set is None else ref_set

        for item in target_set:
            theta.update(item)
        return theta

    def update(self, update_dict: dict) -> bool:
        self.total += 1

        if self.first_field == "quantile":
            kll_sketch = getattr(update_dict, self.first_field)
            quantile_value = single_quantile_from_sketch(kll_sketch, self.quantile_value)
            result = self.func(quantile_value)
        else:
            result = self.func(update_dict)

        if not result:
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
        assert (
            self.quantile_value == other.quantile_value
        ), f"Cannot merge constraints with different quantile_value: {self.quantile_value} and {other.quantile_value}"

        if self.op in (Op.IN_SET, Op.CONTAIN_SET, Op.EQ_SET):
            assert self.reference_set == other.reference_set
            merged_constraint = SummaryConstraint(
                first_field=self.first_field, op=self.op, reference_set=self.reference_set, name=self.name, verbose=self._verbose
            )
        elif self.op == Op.BTWN:
            assert self.upper_value == other.upper_value, f"Cannot merge constraints with different upper values: {self.upper_value} and {other.upper_value}"
            assert self.third_field == other.third_field, f"Cannot merge constraints with different third_field: {self.third_field} and {other.third_field}"
            merged_constraint = SummaryConstraint(
                first_field=self.first_field,
                op=self.op,
                value=self.value,
                upper_value=self.upper_value,
                quantile_value=self.quantile_value,
                second_field=self.second_field,
                third_field=self.third_field,
                name=self.name,
                verbose=self._verbose,
            )
        else:
            merged_constraint = SummaryConstraint(
                first_field=self.first_field,
                op=self.op,
                value=self.value,
                quantile_value=self.quantile_value,
                second_field=self.second_field,
                name=self.name,
                verbose=self._verbose,
            )

        merged_constraint.total = self.total + other.total
        merged_constraint.failures = self.failures + other.failures
        return merged_constraint

    @staticmethod
    def from_protobuf(msg: SummaryConstraintMsg) -> "SummaryConstraint":
        if msg.first_field == "quantile":
            quantile_val = msg.quantile_value
        else:
            quantile_val = None

        if msg.HasField("reference_set") and not msg.HasField("value") and not msg.HasField("second_field") and not msg.HasField("between"):
            return SummaryConstraint(
                msg.first_field,
                msg.op,
                reference_set=set(msg.reference_set),
                name=msg.name,
                verbose=msg.verbose,
            )
        elif msg.HasField("value") and not msg.HasField("second_field") and not msg.HasField("between") and not msg.HasField("reference_set"):
            return SummaryConstraint(
                msg.first_field,
                msg.op,
                value=msg.value,
                quantile_value=quantile_val,
                name=msg.name,
                verbose=msg.verbose,
            )
        elif msg.HasField("second_field") and not msg.HasField("value") and not msg.HasField("between") and not msg.HasField("reference_set"):
            return SummaryConstraint(
                msg.first_field,
                msg.op,
                second_field=msg.second_field,
                name=msg.name,
                quantile_value=quantile_val,
                verbose=msg.verbose,
            )
        elif msg.HasField("between") and not msg.HasField("value") and not msg.HasField("second_field") and not msg.HasField("reference_set"):
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
                    quantile_value=quantile_val,
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
                    quantile_value=quantile_val,
                    name=msg.name,
                    verbose=msg.verbose,
                )
        else:
            raise ValueError(
                "SummaryConstraintMsg must specify a value OR second field name OR SummaryBetweenConstraintMsg OR reference set, but only one of them"
            )

    def to_protobuf(self) -> SummaryConstraintMsg:
        if self.op in (Op.IN_SET, Op.CONTAIN_SET, Op.EQ_SET):
            reference_set_msg = ListValue()
            reference_set_msg.extend(self.reference_set)

            msg = SummaryConstraintMsg(
                name=self.name,
                first_field=self.first_field,
                op=self.op,
                reference_set=reference_set_msg,
                verbose=self._verbose,
            )
        elif self.op == Op.BTWN:

            summary_between_constraint_msg = None
            if self.second_field is None and self.third_field is None:
                summary_between_constraint_msg = SummaryBetweenConstraintMsg(lower_value=self.value, upper_value=self.upper_value)
            else:
                summary_between_constraint_msg = SummaryBetweenConstraintMsg(second_field=self.second_field, third_field=self.third_field)

            msg = SummaryConstraintMsg(
                name=self.name,
                first_field=self.first_field,
                op=self.op,
                quantile_value=self.quantile_value,
                between=summary_between_constraint_msg,
                verbose=self._verbose,
            )

        elif self.second_field is None:
            msg = SummaryConstraintMsg(
                name=self.name,
                first_field=self.first_field,
                op=self.op,
                value=self.value,
                quantile_value=self.quantile_value,
                verbose=self._verbose,
            )
        else:
            msg = SummaryConstraintMsg(
                name=self.name,
                first_field=self.first_field,
                op=self.op,
                quantile_value=self.quantile_value,
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

        raw_values_operators = (Op.MATCH, Op.NOMATCH, Op.APPLY_FUNC)
        self.raw_value_constraints = {}
        self.coerced_type_constraints = {}

        if isinstance(constraints, list):
            constraints = {constraint.name: constraint for constraint in constraints}

        for name, constraint in constraints.items():
            if constraint.op in raw_values_operators:
                self.raw_value_constraints.update({name: constraint})
            else:
                self.coerced_type_constraints.update({name: constraint})

    @staticmethod
    def from_protobuf(msg: ValueConstraintMsgs) -> "ValueConstraints":
        value_constraints = [ValueConstraint.from_protobuf(c) for c in msg.constraints]
        if len(value_constraints) > 0:
            return ValueConstraints({v.name: v for v in value_constraints})
        return None

    def __getitem__(self, name: str) -> Optional[ValueConstraint]:
        if self.raw_value_constraints:
            constraint = self.raw_value_constraints.get(name)
            if constraint:
                return constraint
        if self.coerced_type_constraints:
            return self.coerced_type_constraints.get(name)
        return None

    def to_protobuf(self) -> ValueConstraintMsgs:
        v = [c.to_protobuf() for c in self.raw_value_constraints.values()]
        v.extend([c.to_protobuf() for c in self.coerced_type_constraints.values()])
        if len(v) > 0:
            vcmsg = ValueConstraintMsgs()
            vcmsg.constraints.extend(v)
            return vcmsg
        return None

    def update(self, v):
        for c in self.raw_value_constraints.values():
            c.update(v)

    def update_typed(self, v):
        for c in self.coerced_type_constraints.values():
            c.update(v)

    def merge(self, other) -> "ValueConstraints":
        if not other or not other.raw_value_constraints and not other.coerced_type_constraints:
            return self

        merged_constraints = other.raw_value_constraints.copy()
        merged_constraints.update(other.coerced_type_constraints.copy())
        for name, constraint in self.raw_value_constraints.items():
            merged_constraints[name] = constraint.merge(other.raw_value_constraints.get(name))
        for name, constraint in self.coerced_type_constraints.items():
            merged_constraints[name] = constraint.merge(other.coerced_type_constraints.get(name))

        return ValueConstraints(merged_constraints)

    def report(self) -> List[tuple]:
        v = [c.report() for c in self.raw_value_constraints.values()]
        v.extend([c.report() for c in self.coerced_type_constraints.values()])
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


def distinctValuesInSetConstraint(reference_set: Set[Any], name=None, verbose=False):
    return SummaryConstraint("distinct_column_values", Op.IN_SET, reference_set=reference_set, name=name, verbose=False)


def distinctValuesEqualSetConstraint(reference_set: Set[Any], name=None, verbose=False):
    return SummaryConstraint("distinct_column_values", Op.EQ_SET, reference_set=reference_set, name=name, verbose=False)


def distinctValuesContainSetConstraint(reference_set: Set[Any], name=None, verbose=False):
    return SummaryConstraint("distinct_column_values", Op.CONTAIN_SET, reference_set=reference_set, name=name, verbose=False)


def columnValuesInSetConstraint(value_set: Set[Any], verbose=False):
    try:
        value_set = set(value_set)
    except Exception:
        raise TypeError("The value set should be an iterable data type")

    return ValueConstraint(Op.IN_SET, value=value_set, verbose=verbose)


def containsEmailConstraint(regex_pattern: "str" = None, verbose=False):
    if regex_pattern is not None:
        logger.warning("Warning: supplying your own regex pattern might cause slower evaluation of the containsEmailConstraint, depending on its complexity.")
        email_pattern = regex_pattern
    else:
        email_pattern = (
            r"^(?i)(?:[a-z0-9!#$%&\'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&\'*+/=?^_`{|}~-]+)*"
            r'|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|[\x01-\x09\x0b\x0c\x0e-\x7f])*")'
            r"@"
            r"(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)$"
        )

    return ValueConstraint(Op.MATCH, regex_pattern=email_pattern, verbose=verbose)


def containsCreditCardConstraint(regex_pattern: "str" = None, verbose=False):
    if regex_pattern is not None:
        logger.warning(
            "Warning: supplying your own regex pattern might cause slower evaluation of the containsCreditCardConstraint, depending on its complexity."
        )
        credit_card_pattern = regex_pattern
    else:
        credit_card_pattern = (
            r"^(?:(4[0-9]{3}([\s-]?[0-9]{4}){2}[\s-]?[0-9]{1,4})"
            r"|(?:(5[1-5][0-9]{2}([\s-]?[0-9]{4}){3}))"
            r"|(?:(6(?:011|5[0-9]{2})([\s-]?[0-9]{4}){3}))"
            r"|(?:(3[47][0-9]{2}[\s-]?[0-9]{6}[\s-]?[0-9]{5}))"
            r"|(?:(3(?:0[0-5]|[68][0-9])[0-9][\s-]?[0-9]{6}[\s-]?[0-9]{4}))"
            r"|(?:2131|1800|35[0-9]{2,3}([\s-]?[0-9]{4}){3}))$"
        )

    return ValueConstraint(Op.MATCH, regex_pattern=credit_card_pattern, verbose=verbose)


def dateUtilParseableConstraint(verbose=False):
    return ValueConstraint(Op.APPLY_FUNC, apply_function=_try_parse_dateutil, verbose=verbose)


def jsonParseableConstraint(verbose=False):
    return ValueConstraint(Op.APPLY_FUNC, apply_function=_try_parse_json, verbose=verbose)


def matchesJsonSchemaConstraint(json_schema, verbose=False):
    return ValueConstraint(Op.APPLY_FUNC, json_schema, apply_function=_matches_json_schema, verbose=verbose)


def strftimeFormatConstraint(format, verbose=False):
    return ValueConstraint(Op.APPLY_FUNC, format, apply_function=_try_parse_strftime_format, verbose=verbose)


def containsSSNConstraint(regex_pattern: "str" = None, verbose=False):
    if regex_pattern is not None:
        logger.warning("Warning: supplying your own regex pattern might cause slower evaluation of the containsSSNConstraint, depending on its complexity.")
        ssn_pattern = regex_pattern
    else:
        ssn_pattern = r"^(?!000|666|9[0-9]{2})[0-9]{3}[\s-]?(?!00)[0-9]{2}[\s-]?(?!0000)[0-9]{4}$"

    return ValueConstraint(Op.MATCH, regex_pattern=ssn_pattern, verbose=verbose)


def containsURLConstraint(regex_pattern: "str" = None, verbose=False):
    if regex_pattern is not None:
        logger.warning("Warning: supplying your own regex pattern might cause slower evaluation of the containsURLConstraint, depending on its complexity.")
        url_pattern = regex_pattern
    else:
        url_pattern = (
            r"^(?:http(s)?:\/\/)?((www)|(?:[a-zA-z0-9-]+)\.)"
            r"(?:[-a-zA-Z0-9@:%._\+~#=]{1,256}\."
            r"(?:[a-zA-Z0-9]{1,6})\b"
            r"(?:[-a-zA-Z0-9@:%_\+.~#?&//=]*))$"
        )

    return ValueConstraint(Op.MATCH, regex_pattern=url_pattern, verbose=verbose)


def stringLengthEqualConstraint(length: int, verbose=False):
    length_pattern = f"^.{{{length}}}$"
    return ValueConstraint(Op.MATCH, regex_pattern=length_pattern, verbose=verbose)


def stringLengthBetweenConstraint(lower_value: int, upper_value: int, verbose=False):
    length_pattern = rf"^.{{{lower_value},{upper_value}}}$"
    return ValueConstraint(Op.MATCH, regex_pattern=length_pattern, verbose=verbose)


def quantileBetweenConstraint(quantile_value: Union[int, float], lower_value: Union[int, float], upper_value: Union[int, float], verbose: "bool" = False):
    if not all([isinstance(v, (int, float)) for v in (quantile_value, upper_value, lower_value)]):
        raise TypeError("The quantile, lower and upper values must be of type int or float")

    if lower_value > upper_value:
        raise ValueError("The lower value must be less than or equal to the upper value")

    return SummaryConstraint("quantile", value=lower_value, upper_value=upper_value, quantile_value=quantile_value, op=Op.BTWN, verbose=verbose)


def columnUniqueValueCountBetweenConstraint(lower_value: int, upper_value: int, verbose: bool = False):
    if not all([isinstance(v, int) and v >= 0 for v in (lower_value, upper_value)]):
        raise ValueError("The lower and upper values should be non-negative integers")

    if lower_value > upper_value:
        raise ValueError("The lower value should be less than or equal to the upper value")

    return SummaryConstraint("unique_count", op=Op.BTWN, value=lower_value, upper_value=upper_value, verbose=verbose)


def columnUniqueValueProportionBetweenConstraint(lower_fraction: float, upper_fraction: float, verbose: bool = False):
    if not all([isinstance(v, float) and 0 <= v <= 1 for v in (lower_fraction, upper_fraction)]):
        raise ValueError("The lower and upper fractions should be between 0 and 1")

    if lower_fraction > upper_fraction:
        raise ValueError("The lower fraction should be decimal values less than or equal to the upper fraction")

    return SummaryConstraint("unique_proportion", op=Op.BTWN, value=lower_fraction, upper_value=upper_fraction, verbose=verbose)
