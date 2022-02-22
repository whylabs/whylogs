import copy
import datetime
import json
import logging
import numbers
import re
from typing import Any, List, Mapping, Optional, Set, Tuple, Union

import datasketches
import jsonschema
import numpy as np
import pandas as pd
from datasketches import theta_a_not_b, update_theta_sketch
from dateutil.parser import parse
from google.protobuf.json_format import Parse
from google.protobuf.struct_pb2 import ListValue
from jsonschema import validate

from whylogs.core.statistics.hllsketch import HllSketch
from whylogs.core.statistics.numbertracker import DEFAULT_HIST_K
from whylogs.core.summaryconverters import (
    compute_chi_squared_test_p_value,
    compute_kl_divergence,
    ks_test_compute_p_value,
    single_quantile_from_sketch,
)
from whylogs.core.types import TypedDataConverter
from whylogs.proto import (
    ApplyFunctionMsg,
    DatasetConstraintMsg,
    DatasetProperties,
    InferredType,
    KllFloatsSketchMessage,
    MultiColumnValueConstraintMsg,
    Op,
    ReferenceDistributionContinuousMessage,
    ReferenceDistributionDiscreteMessage,
    SummaryBetweenConstraintMsg,
    SummaryConstraintMsg,
    SummaryConstraintMsgs,
    ValueConstraintMsg,
    ValueConstraintMsgs,
)
from whylogs.util.dsketch import FrequentItemsSketch
from whylogs.util.protobuf import message_to_json

TYPES = InferredType.Type

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


# restrict the set length for printing the name of the constraint which contains a reference set
MAX_SET_DISPLAY_MESSAGE_LENGTH = 20


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
    Op.IN: lambda x: lambda v: v in x,
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
    Op.IN: lambda f, v: lambda s: getattr(s, f) in v,
    Op.CONTAIN: lambda f, v: lambda s: v in getattr(s, f),
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

_multi_column_value_funcs = {
    Op.LT: lambda v2: lambda v1: v1 < v2,
    Op.LE: lambda v2: lambda v1: v1 <= v2,
    Op.EQ: lambda v2: lambda v1: v1 == v2,
    Op.NE: lambda v2: lambda v1: v1 != v2,
    Op.GE: lambda v2: lambda v1: v1 >= v2,
    Op.GT: lambda v2: lambda v1: v1 > v2,  # assert incoming value 'v' is greater than some fixed value 'x'
    Op.IN: lambda v2: lambda v1: v1 in v2,
    Op.NOT_IN: lambda v2: lambda v1: v1 not in v2,
    Op.SUM: lambda v: sum(v),
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

        if isinstance(value, set) != (op == Op.IN):
            raise ValueError("Value constraint must provide a set of values for using the IN operator!")

        if self.op == Op.APPLY_FUNC:
            if apply_function.__name__ not in globals() or "lambda" in apply_function.__name__:
                raise ValueError("Cannot initialize constraint with APPLY_FUNC using an unknown function!")
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
        if self._name:
            return self._name
        if self.op == Op.APPLY_FUNC:
            val_or_funct = self.apply_function.__name__
        elif hasattr(self, "value"):
            val_or_funct = self.value
        else:
            val_or_funct = self.regex_pattern

        return f"value {Op.Name(self.op)} {val_or_funct}"

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
        elif all([hasattr(v, "value") for v in (self, other)]):
            val = self.value
            assert self.value == other.value, f"Cannot merge value constraints with different values: {self.value} and {other.value}"
        elif all([hasattr(v, "regex_pattern") for v in (self, other)]):
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
        val = None
        regex_pattern = None
        apply_function = None
        name = msg.name if msg.name else None

        if msg.HasField("function"):
            val = None if msg.function.reference_value == "" else msg.function.reference_value
            apply_function = globals()[msg.function.function]
        elif msg.regex_pattern != "":
            regex_pattern = msg.regex_pattern
        elif len(msg.value_set.values) != 0:
            val = set(msg.value_set.values[0].list_value)
        else:
            val = msg.value

        constraint = ValueConstraint(msg.op, value=val, regex_pattern=regex_pattern, apply_function=apply_function, name=name, verbose=msg.verbose)
        constraint.total = msg.total
        constraint.failures = msg.failures

        return constraint

    def to_protobuf(self) -> ValueConstraintMsg:
        set_vals_message = None
        regex_pattern = None
        value = None
        apply_func = None

        if self.op == Op.APPLY_FUNC:
            if hasattr(self, "value"):
                apply_func = ApplyFunctionMsg(function=self.apply_function.__name__, reference_value=self.value)
            else:
                apply_func = ApplyFunctionMsg(function=self.apply_function.__name__)

        elif hasattr(self, "value"):
            if isinstance(self.value, set):
                set_vals_message = ListValue()
                set_vals_message.append(list(self.value))
            else:
                value = self.value

        elif hasattr(self, "regex_pattern"):
            regex_pattern = self.regex_pattern

        return ValueConstraintMsg(
            name=self.name,
            op=self.op,
            value=value,
            value_set=set_vals_message,
            regex_pattern=regex_pattern,
            function=apply_func,
            verbose=self._verbose,
            total=self.total,
            failures=self.failures,
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
        Only to be supplied when using set operations or distributional measures.
        Used as a reference set to be compared with the column distinct values.
        Or is instance of datasketches.kll_floats_sketch or ReferenceDistributionDiscreteMessage.
        Only to be supplied for constraints on distributional measures, such as KS test, KL divergence and Chi-Squared test.
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
        reference_set: Union[List[Any], Set[Any], datasketches.kll_floats_sketch, ReferenceDistributionDiscreteMessage] = None,
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

        table_shape_constraint = self._check_and_init_table_shape_constraint(reference_set)
        set_constraint = False
        distributional_measure_constraint = False
        between_constraint = False

        if not table_shape_constraint:
            set_constraint = self._check_and_init_valid_set_constraint(reference_set)
        if not any([table_shape_constraint, set_constraint]):
            distributional_measure_constraint = self._check_and_init_distributional_measure_constraint(reference_set)
        if not any([table_shape_constraint, set_constraint, distributional_measure_constraint]):
            between_constraint = self._check_and_init_between_constraint()
        if not any([table_shape_constraint, set_constraint, distributional_measure_constraint, between_constraint]):
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
        if self._name:
            return self._name
        constraint_type_str = self._get_constraint_type()
        field_name = self._get_field_name()
        value_or_field = self._get_value_or_field()

        return f"{constraint_type_str} {field_name} {Op.Name(self.op)} {value_or_field}"

    def _get_field_name(self):
        if self.first_field == "quantile":
            return f"{self.first_field} {self.quantile_value}"
        elif hasattr(self, "reference_distribution"):
            if self.first_field == "kl_divergence":
                return f"{self.first_field} threshold"
            else:
                return f"{self.first_field} p-value"
        else:
            return self.first_field

    def _get_value_or_field(self):
        if self.first_field == "column_values_type":
            if self.value is not None:
                return InferredType.Type.Name(self.value)
            else:
                return {InferredType.Type.Name(element) for element in list(self.reference_set)[:MAX_SET_DISPLAY_MESSAGE_LENGTH]}
        elif hasattr(self, "reference_set"):
            return self._get_str_from_ref_set()
        elif self.op == Op.BTWN:
            lower_target = self.value if self.value is not None else self.second_field
            upper_target = self.upper_value if self.upper_value is not None else self.third_field
            return f"{lower_target} and {upper_target}"
        elif self.first_field in ("columns", "total_row_number"):
            return str(self.value)
        else:
            return f"{self.value if self.value is not None else self.second_field}"

    def _get_constraint_type(self):
        return "table" if self.first_field in ("columns", "total_row_number") else "summary"

    def _check_and_init_table_shape_constraint(self, reference_set):
        if self.first_field in ("columns", "total_row_number"):  # table shape constraint

            if self.first_field == "columns":
                if self.op == Op.EQ:
                    if any([self.value, self.upper_value, self.second_field, self.third_field, not reference_set]):
                        raise ValueError("When using set operations only set should be provided and not values or field names!")
                    self.reference_set = reference_set
                    reference_set = self._try_cast_set()
                else:
                    if any([not self.value, self.upper_value, self.second_field, self.third_field, reference_set]):
                        raise ValueError("When using table shape columns constraint only value should be provided and no fields or reference set!")

            if isinstance(self.value, (float)):
                self.value = int(self.value)

            if (self.op == Op.CONTAIN and not isinstance(self.value, str)) or all(
                [self.op == Op.EQ, not isinstance(self.value, int), not isinstance(reference_set, set)]
            ):

                raise ValueError("Table shape constraints require value of type string or string set for columns and type int for number of rows!")

            target_val = self.value if self.value is not None else self.reference_set
            self.func = _summary_funcs1[self.op](self.first_field, target_val)
            return True
        return False

    def _check_and_init_valid_set_constraint(self, reference_set):
        if self.op in (Op.IN_SET, Op.CONTAIN_SET, Op.EQ_SET, Op.IN):
            if any([self.value, self.upper_value, self.second_field, self.third_field, not reference_set]):
                raise ValueError("When using set operations only set should be provided and not values or field names!")

            self.reference_set = reference_set
            reference_set = self._try_cast_set()

            if self.op != Op.IN:
                self.ref_string_set, self.ref_numbers_set = self._get_string_and_numbers_sets()
                self.string_theta_sketch = self._create_theta_sketch(self.ref_string_set)
                self.numbers_theta_sketch = self._create_theta_sketch(self.ref_numbers_set)

                self.func = _summary_funcs1[self.op](self.first_field, self.string_theta_sketch, self.numbers_theta_sketch)
            else:
                self.func = _summary_funcs1[self.op](self.first_field, reference_set)
            return True
        return False

    def _check_and_init_distributional_measure_constraint(self, reference_set):
        if reference_set and self.op not in (Op.IN_SET, Op.CONTAIN_SET, Op.EQ_SET, Op.IN):
            if not isinstance(reference_set, (datasketches.kll_floats_sketch, ReferenceDistributionDiscreteMessage)):
                raise TypeError("The reference distribution should be an object of type datasketches.kll_floats_sketch or ReferenceDistributionDiscreteMessage")
            if self.value is None or any([v is not None for v in (self.upper_value, self.second_field, self.third_field)]):
                raise ValueError(
                    "Summary constraint with reference_distribution must specify value for comparing with the p_value or threshold,"
                    " and must not specify lower_value, second_field or third_field"
                )
            self.reference_distribution = reference_set
            self.func = _summary_funcs1[self.op](self.first_field, self.value)
            return True
        return False

    def _check_and_init_between_constraint(self):
        if self.op == Op.BTWN:
            if all([v is not None for v in (self.value, self.upper_value)]) and all([v is None for v in (self.second_field, self.third_field)]):
                # field-value summary comparison
                if not isinstance(self.value, (int, float)) or not isinstance(self.upper_value, (int, float)):
                    raise TypeError("When creating Summary constraint with BETWEEN operation, upper and lower value must be of type (int, float)")
                if self.value >= self.upper_value:
                    raise ValueError("Summary constraint with BETWEEN operation must specify lower value to be less than upper value")

                self.func = _summary_funcs1[self.op](self.first_field, self.value, self.upper_value)
                return True
            elif all([v is not None for v in (self.second_field, self.third_field)]) and all([v is None for v in (self.value, self.upper_value)]):
                # field-field summary comparison
                if not isinstance(self.second_field, str) or not isinstance(self.third_field, str):
                    raise TypeError("When creating Summary constraint with BETWEEN operation, upper and lower field must be of type string")
                self.func = _summary_funcs2[self.op](self.first_field, self.second_field, self.third_field)
                return True
            else:
                raise ValueError("Summary constraint with BETWEEN operation must specify lower and upper value OR lower and third field name, but not both")
        return False

    def _get_str_from_ref_set(self) -> str:
        reference_set_str = ""
        if len(self.reference_set) > MAX_SET_DISPLAY_MESSAGE_LENGTH:
            tmp_set = set(list(self.reference_set)[:MAX_SET_DISPLAY_MESSAGE_LENGTH])
            reference_set_str = f"{str(tmp_set)[:-1]}, ...}}"
        else:
            reference_set_str = str(self.reference_set)

        return reference_set_str

    def _try_cast_set(self) -> Set[Any]:
        if not isinstance(self.reference_set, set):
            try:
                logger.warning(f"Trying to cast provided value of {type(self.reference_set)} to type set!")
                self.reference_set = set(self.reference_set)
            except TypeError:
                provided_type_name = self.reference_set.__class__.__name__
                raise TypeError(f"When using set operations, provided value must be set or set castable, instead type: '{provided_type_name}' was provided!")
        return self.reference_set

    def _get_string_and_numbers_sets(self):
        string_set = set()
        numbers_set = set()
        for item in self.reference_set:
            if isinstance(item, str):
                string_set.add(item)
            elif isinstance(item, numbers.Real) and not isinstance(item, bool):
                numbers_set.add(item)

        return string_set, numbers_set

    def _create_theta_sketch(self, ref_set: set = None):
        theta = update_theta_sketch()
        target_set = self.reference_set if ref_set is None else ref_set

        for item in target_set:
            theta.update(item)
        return theta

    def update(self, update_summary: object) -> bool:
        constraint_type_str = "table shape" if self.first_field in ("columns", "total_row_number") else "summary"

        self.total += 1

        if self.first_field == "quantile":
            kll_sketch = update_summary.quantile
            update_summary = single_quantile_from_sketch(kll_sketch, self.quantile_value)
        elif self.first_field == "ks_test":
            update_summary = ks_test_compute_p_value(update_summary.ks_test, self.reference_distribution)
        elif self.first_field == "kl_divergence":
            update_summary = compute_kl_divergence(update_summary.kl_divergence, self.reference_distribution)
        elif self.first_field == "chi_squared_test":
            update_summary = compute_chi_squared_test_p_value(update_summary.chi_squared_test, self.reference_distribution)

        if not self.func(update_summary):
            self.failures += 1
            if self._verbose:
                logger.info(f"{constraint_type_str} constraint {self.name} failed")

    def merge(self, other) -> "SummaryConstraint":
        if not other:
            return self

        second_field = None
        third_field = None
        upper_value = None
        quantile = None
        reference_dist = None

        assert self.name == other.name, f"Cannot merge constraints with different names: ({self.name}) and ({other.name})"
        assert self.op == other.op, f"Cannot merge constraints with different ops: {self.op} and {other.op}"
        assert self.value == other.value, f"Cannot merge constraints with different values: {self.value} and {other.value}"
        assert self.first_field == other.first_field, f"Cannot merge constraints with different first_field: {self.first_field} and {other.first_field}"
        assert self.second_field == other.second_field, f"Cannot merge constraints with different second_field: {self.second_field} and {other.second_field}"
        assert (
            self.quantile_value == other.quantile_value
        ), f"Cannot merge constraints with different quantile_value: {self.quantile_value} and {other.quantile_value}"
        if self.quantile_value is not None:
            quantile = self.quantile_value
        elif hasattr(self, "reference_distribution") and hasattr(other, "reference_distribution"):
            reference_dist = self.reference_distribution
            if all([isinstance(dist, ReferenceDistributionDiscreteMessage) for dist in (self.reference_distribution, other.reference_distribution)]):
                assert self.reference_distribution == other.reference_distribution, "Cannot merge constraints with different reference_distribution"
            elif all([isinstance(dist, datasketches.kll_floats_sketch)] for dist in (self.reference_distribution, other.reference_distribution)):
                assert (
                    self.reference_distribution.serialize() == other.reference_distribution.serialize()
                ), "Cannot merge constraints with different reference_distribution"
            else:
                raise AssertionError("Cannot merge constraints with different reference_distribution")

        if hasattr(self, "reference_set"):
            assert hasattr(other, "reference_set"), "Cannot merge constraint that doesn't have reference set with one that does."
            assert (
                self.reference_set == other.reference_set
            ), f"Cannot merge constraints with different reference sets: {self._get_str_from_ref_set()} and {other._get_str_from_ref_set()}"
            reference_dist = self.reference_set
        elif self.op == Op.BTWN:
            assert self.upper_value == other.upper_value, f"Cannot merge constraints with different upper values: {self.upper_value} and {other.upper_value}"
            assert self.third_field == other.third_field, f"Cannot merge constraints with different third_field: {self.third_field} and {other.third_field}"
            third_field = self.third_field
            upper_value = self.upper_value

        merged_constraint = SummaryConstraint(
            first_field=self.first_field,
            op=self.op,
            value=self.value,
            upper_value=upper_value,
            second_field=second_field,
            third_field=third_field,
            reference_set=reference_dist,
            quantile_value=quantile,
            name=self.name,
            verbose=self._verbose,
        )

        merged_constraint.total = self.total + other.total
        merged_constraint.failures = self.failures + other.failures
        return merged_constraint

    def _check_if_summary_constraint_message_is_valid(msg: SummaryConstraintMsg):
        if msg.HasField("reference_set") and not any([msg.HasField(f) for f in ("value", "value_str", "second_field", "between")]):
            return True
        elif msg.HasField("value") and not any([msg.HasField(f) for f in ("value_str", "second_field", "between", "reference_set")]):
            return True
        elif msg.HasField("value_str") and not any([msg.HasField(f) for f in ("second_field", "between", "reference_set")]):
            return True
        elif msg.HasField("second_field") and not any([msg.HasField(f) for f in ("value", "value_str", "between", "reference_set")]):
            return True
        elif msg.HasField("between") and not any([msg.HasField(f) for f in ("value", "value_str", "second_field", "reference_set")]):
            if all([msg.between.HasField(f) for f in ("lower_value", "upper_value")]) and not any(
                [msg.between.HasField(f) for f in ("second_field", "third_field")]
            ):
                return True
        elif all([msg.between.HasField(f) for f in ("second_field", "third_field")]) and not any(
            [msg.between.HasField(f) for f in ("lower_value", "upper_value")]
        ):
            return True
        elif (
            ((msg.HasField("continuous_distribution") and msg.continuous_distribution.HasField("sketch")) or msg.HasField("discrete_distribution"))
            and msg.HasField("value")
            and not any([msg.HasField(f) for f in ("second_field", "between")])
        ):
            return True

        return False

    @staticmethod
    def from_protobuf(msg: SummaryConstraintMsg) -> "SummaryConstraint":
        if not SummaryConstraint._check_if_summary_constraint_message_is_valid(msg):
            raise ValueError("SummaryConstraintMsg must specify a value OR second field name OR SummaryBetweenConstraintMsg, but only one of them")

        value = None
        second_field = None
        lower_value = None
        upper_value = None
        third_field = None
        quantile_value = None
        ref_distribution = None
        name = msg.name if msg.name else None

        if msg.first_field == "quantile":
            quantile_value = msg.quantile_value

        if msg.HasField("continuous_distribution") and msg.continuous_distribution.HasField("sketch"):
            ref_distribution = datasketches.kll_floats_sketch.deserialize(msg.continuous_distribution.sketch.sketch)
        elif msg.HasField("discrete_distribution"):
            ref_distribution = msg.discrete_distribution

        if msg.HasField("reference_set"):
            ref_distribution = set(msg.reference_set)
        elif msg.HasField("value"):
            value = msg.value
        elif msg.HasField("value_str"):
            value = msg.value_str
        elif msg.HasField("second_field"):
            second_field = msg.second_field
        elif msg.HasField("between"):
            if all([msg.between.HasField(f) for f in ("lower_value", "upper_value")]):
                lower_value = msg.between.lower_value
                upper_value = msg.between.upper_value
            elif all([msg.between.HasField(f) for f in ("second_field", "third_field")]):
                second_field = msg.between.second_field
                third_field = msg.between.third_field

        return SummaryConstraint(
            msg.first_field,
            msg.op,
            value=value if value is not None else lower_value,
            upper_value=upper_value,
            second_field=second_field,
            quantile_value=quantile_value,
            third_field=third_field,
            reference_set=ref_distribution,
            name=name,
            verbose=msg.verbose,
        )

    def to_protobuf(self) -> SummaryConstraintMsg:
        reference_set_msg = None
        summary_between_constraint_msg = None
        quantile_value = None
        value = None
        value_str = None
        second_field = None
        continuous_dist = None
        discrete_dist = None

        if hasattr(self, "reference_distribution"):
            if isinstance(self.reference_distribution, datasketches.kll_floats_sketch):
                kll_floats_sketch = KllFloatsSketchMessage(sketch=self.reference_distribution.serialize())
                continuous_dist = ReferenceDistributionContinuousMessage(sketch=kll_floats_sketch)
            elif isinstance(self.reference_distribution, ReferenceDistributionDiscreteMessage):
                discrete_dist = self.reference_distribution

        elif self.quantile_value is not None:
            quantile_value = self.quantile_value

        if hasattr(self, "reference_set"):
            reference_set_msg = ListValue()
            reference_set_msg.extend(self.reference_set)

        elif self.op == Op.BTWN:
            if self.second_field is None and self.third_field is None:
                summary_between_constraint_msg = SummaryBetweenConstraintMsg(lower_value=self.value, upper_value=self.upper_value)
            else:
                summary_between_constraint_msg = SummaryBetweenConstraintMsg(second_field=self.second_field, third_field=self.third_field)
        elif self.second_field:
            second_field = self.second_field
        elif self.value is not None:
            if isinstance(self.value, str):
                value_str = self.value
            else:
                value = self.value

        return SummaryConstraintMsg(
            name=self.name,
            first_field=self.first_field,
            second_field=second_field,
            value=value,
            value_str=value_str,
            between=summary_between_constraint_msg,
            reference_set=reference_set_msg,
            quantile_value=quantile_value,
            continuous_distribution=continuous_dist,
            discrete_distribution=discrete_dist,
            op=self.op,
            verbose=self._verbose,
        )

    def report(self):
        return (self.name, self.total, self.failures)


class ValueConstraints:
    def __init__(self, constraints: Mapping[str, ValueConstraint] = None):
        """
        ValueConstraints is a container for multiple value constraints,
        generally associated with a single ColumnProfile.

        Parameters
        ----------
        constraints : Mapping[str, ValueConstraint]
            A dictionary of value constraints with their names as keys.
            Can also accept a list of value constraints.

        """

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
        """
        SummaryConstraints is a container for multiple summary constraints,
        generally associated with a single ColumnProfile.

        Parameters
        ----------
        constraints : Mapping[str, SummaryConstrain]
            A dictionary of summary constraints with their names as keys.
            Can also accept a list of summary constraints.

        """

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


class MultiColumnValueConstraint(ValueConstraint):
    def __init__(
        self,
        dependent_columns: Union[str, List[str], Tuple[str], np.ndarray],
        op: Op,
        reference_columns: Union[str, List[str], Tuple[str], np.ndarray] = None,
        internal_dependent_cols_op: Op = None,
        value=None,
        name: str = None,
        verbose: bool = False,
    ):
        """
        MultiColumnValueConstraint defines a dependency relationship between multiple columns,
        they can be relationships between column pairs and value pairs, between a single or multiple dependent and
        a single or multiple reference columns, or between multiple columns and a single value.


        Parameters
        ----------
        dependent_columns : Union[str, List[str], Tuple[str], np.ndarray] (required)
            The dependent column(s), can be a single string representing one dependent column, or
            a list, tuple or numpy array of strings representing the names of a set of dependent columns.
        op - whylogs.proto.Op (required)
            Enumeration of binary comparison operator applied between multiple columns, or multiple columns and values.
            Enum values are mapped to operators like '==', '<', and '<=', etc.
        reference_columns : Union[str, List[str], Tuple[str], np.ndarray] (one-of)
            The reference column(s), can be a single string representing one reference column, or
            A list, tuple or numpy array of strings representing the names of a set of reference columns.
            If provided, the reference column(s) will be compared against
            the dependent column(s) specified in `dependent_columns`, using the given operator.
            Only one of `reference_columns` or `value` should be supplied.
        internal_dependent_cols_op : whylogs.proto.Op
            Enumeration of arithmetic operators applied to the dependent columns, if a transformation is necessary.
            Enum values are mapped to operators like '+', '-', etc.
            e.g.    dependent_columns = ['A', 'B', 'C'], internal_dependent_cols_op = Op.SUM
                    A sum operation is performed over each value of column 'A', 'B', and 'C', in each row of the table
        value : Any
            Static value to be compared against the dependent columns specified in `dependent_columns`.
            Only one of `value` or `reference_columns` should be supplied.
        name : str
            Name of the constraint used for reporting
        verbose : bool
            If true, log every application of this constraint that fails.
            Useful to identify specific streaming values that fail the constraint.

        """

        self._name = name
        self._verbose = verbose
        self.op = op
        self.total = 0
        self.failures = 0

        if dependent_columns is None:
            raise ValueError("The dependent_columns attribute must be provided when creating a MultiColumnValueConstraint")
        if not isinstance(dependent_columns, (str, list, tuple, np.ndarray)):
            raise TypeError(
                "The dependent_columns attribute should be a str indicating a column name in the dataframe, or an array-like object of column names"
            )

        self.dependent_columns = dependent_columns
        self.func = _multi_column_value_funcs[op]

        if (reference_columns is None) == (value is None):
            raise ValueError("One of value and reference_columns attributes should be provided when creating a MultiColumnValueConstraint," " but not both")

        if reference_columns:
            if not isinstance(reference_columns, (str, list, tuple, np.ndarray)):
                raise TypeError("The reference_columns attribute should be a str indicating a column name in the dataframe," " or a list of column names")
            else:
                self.reference_columns = reference_columns
        elif value is not None:
            self.value = value

        if internal_dependent_cols_op:
            self.internal_op = internal_dependent_cols_op
            self.internal_dependent_cols_func = _multi_column_value_funcs[internal_dependent_cols_op]

    @property
    def name(self):
        if self._name:
            return self._name

        dependent_cols = str(self.dependent_columns)
        if hasattr(self, "value"):
            val_or_ref_columns = self.value
        else:
            val_or_ref_columns = self.reference_columns
        if hasattr(self, "internal_op"):
            dependent_cols = Op.Name(self.internal_op) + " " + dependent_cols
        return f"multi column value {dependent_cols} {Op.Name(self.op)} {val_or_ref_columns}"

    def update(self, column_values_dictionary):
        columns = copy.deepcopy(column_values_dictionary)
        self.total += 1
        if isinstance(self.dependent_columns, str):
            v1 = columns[self.dependent_columns]
        else:
            if hasattr(self, "internal_dependent_cols_func"):
                v1 = self.internal_dependent_cols_func([columns[col] for col in self.dependent_columns])
            else:
                v1 = tuple([columns[col] for col in self.dependent_columns])
        if hasattr(self, "reference_columns"):
            if isinstance(self.reference_columns, str):
                if self.reference_columns == "all":
                    columns.pop(self.dependent_columns)
                    v2 = columns.values()
                else:
                    v2 = columns[self.reference_columns]
            else:
                v2 = tuple([columns[col] for col in self.reference_columns])
        else:
            v2 = self.value

        if not self.func(v2)(v1):
            self.failures += 1
            if self._verbose:
                logger.info(f"multi column value constraint {self.name} failed on values: {v1}, {v2}")

    def merge(self, other) -> "MultiColumnValueConstraint":
        if not other:
            return self

        val = None
        reference_columns = None
        internal_op = None

        assert (
            self.dependent_columns == other.dependent_columns
        ), f"Cannot merge multicolumn constraints with different dependent columns: ({self.dependent_columns}) and ({other.dependent_columns})"
        assert self.name == other.name, f"Cannot merge multicolumn constraints with different names: ({self.name}) and ({other.name})"
        assert self.op == other.op, f"Cannot merge multicolumn constraints with different ops: {self.op} and {other.op}"

        if all([hasattr(obj, "value") for obj in (self, other)]):
            val = self.value
            assert self.value == other.value, f"Cannot merge multicolumn value constraints with different values: {self.value} and {other.value}"
        elif all([hasattr(obj, "reference_columns") for obj in (self, other)]):
            reference_columns = self.reference_columns
            assert (
                self.reference_columns == other.reference_columns
            ), f"Cannot merge multicolumn value constraints with different reference_columns: {self.reference_columns} and {other.reference_columns}"
        else:
            raise AssertionError(
                "Cannot merge multicolumn value constraints from which one has a value attribute and the other has a reference_columns attribute"
            )
        if hasattr(self, "internal_op") != hasattr(other, "internal_op"):
            raise AssertionError("Cannot merge multicolumn value constraint that has an internal op, with one that does not")
        elif hasattr(self, "internal_op"):
            assert self.internal_op == other.internal_op, "Cannot merge multicolumn value constraints with different internal ops"
            internal_op = self.internal_op

        merged_value_constraint = MultiColumnValueConstraint(
            dependent_columns=self.dependent_columns,
            op=self.op,
            value=val,
            name=self.name,
            reference_columns=reference_columns,
            internal_dependent_cols_op=internal_op,
            verbose=self._verbose,
        )

        merged_value_constraint.total = self.total + other.total
        merged_value_constraint.failures = self.failures + other.failures
        return merged_value_constraint

    @staticmethod
    def from_protobuf(msg: MultiColumnValueConstraintMsg) -> "MultiColumnValueConstraint":
        internal_op = None
        value = None
        ref_cols = None
        name = msg.name if msg.name else None

        if msg.HasField("dependent_columns"):
            dependent_cols = list(msg.dependent_columns)
        else:
            dependent_cols = msg.dependent_column

        if msg.internal_dependent_columns_op:
            internal_op = msg.internal_dependent_columns_op

        if len(msg.value_set.values) != 0:
            value = {TypedDataConverter.convert(tuple(v)) if hasattr(v, "values") else TypedDataConverter.convert(v) for v in msg.value_set}

        elif msg.value:
            value = msg.value

        elif msg.reference_columns:
            ref_cols = list(msg.reference_columns)
            if ref_cols == ["all"]:
                ref_cols = "all"
        else:
            raise ValueError("MultiColumnValueConstraintMsg should contain one of the attributes: value_set, value or reference_columns, but none were found")
        mcv_constraint = MultiColumnValueConstraint(
            dependent_cols, msg.op, value=value, reference_columns=ref_cols, name=name, internal_dependent_cols_op=internal_op, verbose=msg.verbose
        )
        mcv_constraint.total = msg.total
        mcv_constraint.failures = msg.failures

        return mcv_constraint

    def to_protobuf(self) -> MultiColumnValueConstraintMsg:
        value = None
        set_vals_message = None
        ref_cols = None
        dependent_single_col = None
        dependent_multiple_cols = None
        internal_op = None

        if isinstance(self.dependent_columns, str):
            dependent_single_col = self.dependent_columns
        else:
            dependent_multiple_cols = ListValue()
            dependent_multiple_cols.extend(self.dependent_columns)

        if hasattr(self, "value"):
            if isinstance(self.value, (set, list, np.ndarray, pd.Series)):
                set_vals_message = ListValue()
                for val in self.value:
                    if isinstance(val, (set, list, tuple)):
                        internal_list_value = ListValue()
                        internal_list_value.extend(list(val))
                        set_vals_message.append(internal_list_value)
                    else:
                        set_vals_message.append(val)
            else:
                value = self.value
        else:
            ref_cols = ListValue()
            if isinstance(self.reference_columns, str):
                ref_cols.append(self.reference_columns)
            else:
                ref_cols.extend(self.reference_columns)

        if hasattr(self, "internal_op"):
            internal_op = self.internal_op

        return MultiColumnValueConstraintMsg(
            dependent_columns=dependent_multiple_cols,
            dependent_column=dependent_single_col,
            name=self.name,
            op=self.op,
            value=value,
            value_set=set_vals_message,
            reference_columns=ref_cols,
            internal_dependent_columns_op=internal_op,
            verbose=self._verbose,
            total=self.total,
            failures=self.failures,
        )


class MultiColumnValueConstraints(ValueConstraints):
    def __init__(self, constraints: Mapping[str, MultiColumnValueConstraint] = None):
        """
        MultiColumnValueConstraints is a container for multiple MultiColumnValueConstraint objects

        Parameters
        ----------
        constraints : Mapping[str, MultiColumnValueConstraint]
            A dictionary of multi column vale constraints with their names as keys.
            Can also accept a list of multi column value constraints.

        """

        super().__init__(constraints=constraints)

    @staticmethod
    def from_protobuf(msg: ValueConstraintMsgs) -> "MultiColumnValueConstraints":
        value_constraints = [MultiColumnValueConstraint.from_protobuf(c) for c in msg.multi_column_constraints]
        if len(value_constraints) > 0:
            return MultiColumnValueConstraints({v.name: v for v in value_constraints})
        return None

    def to_protobuf(self) -> ValueConstraintMsgs:
        v = [c.to_protobuf() for c in self.raw_value_constraints.values()]
        v.extend([c.to_protobuf() for c in self.coerced_type_constraints.values()])
        if len(v) > 0:
            vcmsg = ValueConstraintMsgs()
            vcmsg.multi_column_constraints.extend(v)
            return vcmsg
        return None


class DatasetConstraints:
    def __init__(
        self,
        props: DatasetProperties,
        value_constraints: Mapping[str, ValueConstraints] = None,
        summary_constraints: Mapping[str, SummaryConstraints] = None,
        table_shape_constraints: Mapping[str, SummaryConstraints] = None,
        multi_column_value_constraints: Optional[MultiColumnValueConstraints] = None,
    ):
        """
        DatasetConstraints is a container for multiple types of constraint containers, such as ValueConstraints,
        SummaryConstraints, and MultiColumnValueConstraints.
        Used for wrapping constraints that should be applied on a single data set.

        Parameters
        ----------
        props : whylogs.proto.DatasetProperties
            Specifies the properties of the data set such as schema major and minor versions, session and
            data timestamps, tags and other metadata
        value_constraints : Mapping[str, ValueConstraints]
            A dictionary where the keys correspond to the name of the feature for which the supplied value
            represents the ValueConstraints to be executed
        summary_constraints : Mapping[str, SummaryConstraints]
            A dictionary where the keys correspond to the name of the feature for which the supplied value
            represents the SummaryConstraints to be executed
        table_shape_constraints : Mapping[str, SummaryConstraints]
            A dictionary where the keys correspond to the name of the feature for which the supplied value
            represents the table-shape constraints to be executed
        multi_column_value_constraints : Mapping[str, MultiColumnValueConstraints]
            A list of MultiColumnValueConstraint, or a container of MultiColumnValueConstraints representing
            the multi-column value constraints to be executed over the predefined features in the constraints.

        """

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

        if table_shape_constraints is None:
            table_shape_constraints = SummaryConstraints()
        if isinstance(table_shape_constraints, list):
            table_shape_constraints = SummaryConstraints(table_shape_constraints)

        self.table_shape_constraints = table_shape_constraints

        if multi_column_value_constraints is None:
            multi_column_value_constraints = MultiColumnValueConstraints()

        if isinstance(multi_column_value_constraints, list):
            multi_column_value_constraints = MultiColumnValueConstraints(multi_column_value_constraints)

        self.multi_column_value_constraints = multi_column_value_constraints

    def __getitem__(self, key):
        if key in self.value_constraint_map:
            return self.value_constraint_map[key]
        return None

    @staticmethod
    def from_protobuf(msg: DatasetConstraintMsg) -> "DatasetConstraints":
        vm = dict([(k, ValueConstraints.from_protobuf(v)) for k, v in msg.value_constraints.items()])
        sm = dict([(k, SummaryConstraints.from_protobuf(v)) for k, v in msg.summary_constraints.items()])
        table_shape_m = SummaryConstraints.from_protobuf(msg.table_shape_constraints)
        multi_column_value_m = MultiColumnValueConstraints.from_protobuf(msg.multi_column_value_constraints)
        return DatasetConstraints(msg.properties, vm, sm, table_shape_m, multi_column_value_m)

    @staticmethod
    def from_json(data: str) -> "DatasetConstraints":
        msg = Parse(data, DatasetConstraintMsg())
        return DatasetConstraints.from_protobuf(msg)

    def to_protobuf(self) -> DatasetConstraintMsg:
        # construct tuple for each column, (name, [constraints,...])
        # turn that into a map indexed by column name
        vm = dict([(k, v.to_protobuf()) for k, v in self.value_constraint_map.items()])
        sm = dict([(k, s.to_protobuf()) for k, s in self.summary_constraint_map.items()])
        table_shape_constraints_message = self.table_shape_constraints.to_protobuf()
        multi_column_value_m = self.multi_column_value_constraints.to_protobuf()
        return DatasetConstraintMsg(
            properties=self.dataset_properties,
            value_constraints=vm,
            summary_constraints=sm,
            table_shape_constraints=table_shape_constraints_message,
            multi_column_value_constraints=multi_column_value_m,
        )

    def to_json(self) -> str:
        return message_to_json(self.to_protobuf())

    def report(self):
        l1 = [(k, v.report()) for k, v in self.value_constraint_map.items()]
        l2 = [(k, s.report()) for k, s in self.summary_constraint_map.items()]
        l3 = self.table_shape_constraints.report() if self.table_shape_constraints.report() else []
        l4 = self.multi_column_value_constraints.report() if self.multi_column_value_constraints.report() else []
        return l1 + l2 + l3 + l4


def _check_between_constraint_valid_initialization(lower_value, upper_value, lower_field, upper_field):
    if (
        (lower_value is not None and upper_field is not None)
        or (lower_value is None and upper_value is not None)
        or (upper_value is None and lower_field is None)
        or (lower_field is not None and upper_field is None)
    ):
        raise ValueError("Summary constraint with BETWEEN operation must specify lower and upper value OR lower and upper field name, but not both")


def _set_between_constraint_default_name(field, lower_value, upper_value, lower_field, upper_field):
    if all([v is not None for v in (lower_value, upper_value)]):
        return f"{field} is between {lower_value} and {upper_value}"
    else:
        return f"{field} is between {lower_field} and {upper_field}"


def _format_set_values_for_display(reference_set):
    if len(reference_set) > MAX_SET_DISPLAY_MESSAGE_LENGTH:
        tmp_set = set(list(reference_set)[:MAX_SET_DISPLAY_MESSAGE_LENGTH])
        return f"{str(tmp_set)[:-1]}, ...}}"
    else:
        return str(reference_set)


def stddevBetweenConstraint(lower_value=None, upper_value=None, lower_field=None, upper_field=None, name=None, verbose=False):
    """
    Defines a summary constraint on the standard deviation of a feature. The standard deviation can be defined to be
    between two values, or between the values of two other summary fields of the same feature,
    such as the minimum and the maximum.
    The defined interval is a closed interval, which includes both of its limit points.

    Parameters
    ----------
    lower_value : numeric (one-of)
        Represents the lower value limit of the interval for the standard deviation.
        If `lower_value` is supplied, then `upper_value` must also be supplied,
        and none of `lower_field` and `upper_field` should be provided.
    upper_value : numeric (one-of)
        Represents the upper value limit of the interval for the standard deviation.
        If `upper_value` is supplied, then `lower_value` must also be supplied,
        and none of `lower_field` and `upper_field` should be provided.
    lower_field : str (one-of)
        Represents the lower field limit of the interval for the standard deviation.
        The lower field is a string representing a summary field
        e.g. `min`, `mean`, `max`, `stddev`, etc., for which the value will be used as a lower bound.
        If `lower_field` is supplied, then `upper_field` must also be supplied,
        and none of `lower_value` and `upper_value` should be provided.
    upper_field : str (one-of)
        Represents the upper field limit of the interval for the standard deviation.
        The upper field is a string representing a summary field
        e.g. `min`, `mean`, `max`, `stddev`, etc., for which the value will be used as an upper bound.
        If `upper_field` is supplied, then `lower_field` must also be supplied,
        and none of `lower_value` and `upper_value` should be provided.
    name : str
        Name of the constraint used for reporting
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        SummaryConstraint -  a summary constraint defining an interval of values for the standard deviation of a feature

    """

    _check_between_constraint_valid_initialization(lower_value, upper_value, lower_field, upper_field)
    if name is None:
        name = _set_between_constraint_default_name("standard deviation", lower_value, upper_value, lower_field, upper_field)
    return SummaryConstraint(
        "stddev", Op.BTWN, value=lower_value, upper_value=upper_value, second_field=lower_field, third_field=upper_field, name=name, verbose=verbose
    )


def meanBetweenConstraint(lower_value=None, upper_value=None, lower_field=None, upper_field=None, name=None, verbose=False):
    """
    Defines a summary constraint on the mean (average) of a feature. The mean can be defined to be
    between two values, or between the values of two other summary fields of the same feature,
    such as the minimum and the maximum.
    The defined interval is a closed interval, which includes both of its limit points.

    Parameters
    ----------
    lower_value : numeric (one-of)
        Represents the lower value limit of the interval for the mean.
        If `lower_value` is supplied, then `upper_value` must also be supplied,
        and none of `lower_field` and `upper_field` should be provided.
    upper_value : numeric (one-of)
        Represents the upper value limit of the interval for the mean.
        If `upper_value` is supplied, then `lower_value` must also be supplied,
        and none of `lower_field` and `upper_field` should be provided.
    lower_field : str (one-of)
        Represents the lower field limit of the interval for the mean.
        The lower field is a string representing a summary field
        e.g. `min`, `mean`, `max`, `stddev`, etc., for which the value will be used as a lower bound.
        If `lower_field` is supplied, then `upper_field` must also be supplied,
        and none of `lower_value` and `upper_value` should be provided.
    upper_field : str (one-of)
        Represents the upper field limit of the interval for the mean.
        The upper field is a string representing a summary field
        e.g. `min`, `mean`, `max`, `stddev`, etc., for which the value will be used as an upper bound.
        If `upper_field` is supplied, then `lower_field` must also be supplied,
        and none of `lower_value` and `upper_value` should be provided.
    name : str
        Name of the constraint used for reporting
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        SummaryConstraint -  a summary constraint defining an interval of values for the mean of a feature

    """

    _check_between_constraint_valid_initialization(lower_value, upper_value, lower_field, upper_field)
    if name is None:
        name = _set_between_constraint_default_name("mean", lower_value, upper_value, lower_field, upper_field)

    return SummaryConstraint(
        "mean", Op.BTWN, value=lower_value, upper_value=upper_value, second_field=lower_field, third_field=upper_field, name=name, verbose=verbose
    )


def minBetweenConstraint(lower_value=None, upper_value=None, lower_field=None, upper_field=None, name=None, verbose=False):
    """
    Defines a summary constraint on the minimum value of a feature. The minimum can be defined to be
    between two values, or between the values of two other summary fields of the same feature,
    such as the minimum and the maximum.
    The defined interval is a closed interval, which includes both of its limit points.

    Parameters
    ----------
    lower_value : numeric (one-of)
        Represents the lower value limit of the interval for the minimum.
        If `lower_value` is supplied, then `upper_value` must also be supplied,
        and none of `lower_field` and `upper_field` should be provided.
    upper_value : numeric (one-of)
        Represents the upper value limit of the interval for the minimum.
        If `upper_value` is supplied, then `lower_value` must also be supplied,
        and none of `lower_field` and `upper_field` should be provided.
    lower_field : str (one-of)
        Represents the lower field limit of the interval for the minimum.
        The lower field is a string representing a summary field
        e.g. `min`, `mean`, `max`, `stddev`, etc., for which the value will be used as a lower bound.
        If `lower_field` is supplied, then `upper_field` must also be supplied,
        and none of `lower_value` and `upper_value` should be provided.
    upper_field : str (one-of)
        Represents the upper field limit of the interval for the minimum.
        The upper field is a string representing a summary field
        e.g. `min`, `mean`, `max`, `stddev`, etc., for which the value will be used as an upper bound.
        If `upper_field` is supplied, then `lower_field` must also be supplied,
        and none of `lower_value` and `upper_value` should be provided.
    name : str
        Name of the constraint used for reporting
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        SummaryConstraint -  a summary constraint defining an interval of values for the minimum value of a feature

    """

    _check_between_constraint_valid_initialization(lower_value, upper_value, lower_field, upper_field)
    if name is None:
        name = _set_between_constraint_default_name("minimum", lower_value, upper_value, lower_field, upper_field)

    return SummaryConstraint(
        "min", Op.BTWN, value=lower_value, upper_value=upper_value, second_field=lower_field, third_field=upper_field, name=name, verbose=verbose
    )


def minGreaterThanEqualConstraint(value=None, field=None, name=None, verbose=False):
    """
    Defines a summary constraint on the minimum value of a feature. The minimum can be defined to be
    greater than or equal to some value,
    or greater than or equal to the values of another summary field of the same feature, such as the mean (average).

    Parameters
    ----------
    value : numeric (one-of)
        Represents the value which should be compared to the minimum value of the specified feature,
        for checking the greater than or equal to constraint.
        Only one of `value` and `field` should be supplied.
    field : str (one-of)
        The field is a string representing a summary field
        e.g. `min`, `mean`, `max`, `stddev`, etc., for which the value will be used for
        checking the greater than or equal to constraint.
        Only one of `field` and `value` should be supplied.
    name : str
        Name of the constraint used for reporting
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        SummaryConstraint -  a summary constraint defining a constraint on the minimum value to be greater than
        or equal to some value / summary field

    """

    if name is None:
        name = f"minimum is greater than or equal to {value}"

    return SummaryConstraint("min", Op.GE, value=value, second_field=field, name=name, verbose=verbose)


def maxBetweenConstraint(lower_value=None, upper_value=None, lower_field=None, upper_field=None, name=None, verbose=False):
    """
    Defines a summary constraint on the maximum value of a feature. The maximum can be defined to be
    between two values, or between the values of two other summary fields of the same feature,
    such as the minimum and the maximum.
    The defined interval is a closed interval, which includes both of its limit points.

    Parameters
    ----------
    lower_value : numeric (one-of)
        Represents the lower value limit of the interval for the maximum.
        If `lower_value` is supplied, then `upper_value` must also be supplied,
        and none of `lower_field` and `upper_field` should be provided.
    upper_value : numeric (one-of)
        Represents the upper value limit of the interval for the maximum.
        If `upper_value` is supplied, then `lower_value` must also be supplied,
        and none of `lower_field` and `upper_field` should be provided.
    lower_field : str (one-of)
        Represents the lower field limit of the interval for the maximum.
        The lower field is a string representing a summary field
        e.g. `min`, `mean`, `max`, `stddev`, etc., for which the value will be used as a lower bound.
        If `lower_field` is supplied, then `upper_field` must also be supplied,
        and none of `lower_value` and `upper_value` should be provided.
    upper_field : str (one-of)
        Represents the upper field limit of the interval for the maximum.
        The upper field is a string representing a summary field
        e.g. `min`, `mean`, `max`, `stddev`, etc., for which the value will be used as an upper bound.
        If `upper_field` is supplied, then `lower_field` must also be supplied,
        and none of `lower_value` and `upper_value` should be provided.
    name : str
        Name of the constraint used for reporting
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        SummaryConstraint -  a summary constraint defining an interval of values for the maximum value of a feature

    """

    _check_between_constraint_valid_initialization(lower_value, upper_value, lower_field, upper_field)
    if name is None:
        name = _set_between_constraint_default_name("maximum", lower_value, upper_value, lower_field, upper_field)

    return SummaryConstraint("max", Op.BTWN, value=lower_value, upper_value=upper_value, second_field=lower_field, third_field=upper_field, verbose=verbose)


def maxLessThanEqualConstraint(value=None, field=None, name=None, verbose=False):
    """
    Defines a summary constraint on the maximum value of a feature. The maximum can be defined to be
    less than or equal to some value,
    or less than or equal to the values of another summary field of the same feature, such as the mean (average).

    Parameters
    ----------
    value : numeric (one-of)
        Represents the value which should be compared to the maximum value of the specified feature,
        for checking the less than or equal to constraint.
        Only one of `value` and `field` should be supplied.
    field : str (one-of)
        The field is a string representing a summary field
        e.g. `min`, `mean`, `max`, `stddev`, etc., for which the value will be used for
        checking the less than or equal to constraint.
        Only one of `field` and `value` should be supplied.
    name : str
        Name of the constraint used for reporting
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        SummaryConstraint -  a summary constraint defining a constraint on the maximum value to be less than
        or equal to some value / summary field

    """
    if name is None:
        name = f"maximum is less than or equal to {value}"

    return SummaryConstraint("max", Op.LE, value=value, second_field=field, name=name, verbose=verbose)


def distinctValuesInSetConstraint(reference_set: Set[Any], name=None, verbose=False):
    """
    Defines a summary constraint on the distinct values of a feature. All of the distinct values should
    belong in the user-provided set or reference values `reference_set`.
    Useful for categorical features, for checking if the set of values present in a feature
    is contained in the set of expected categories.

    Parameters
    ----------
    reference_set : Set[Any] (required)
        Represents the set of reference (expected) values for a feature.
        The provided values can be of any type.
        If at least one of the distinct values of the feature is not in the user specified
        set `reference_set`, then the constraint will fail.
    name : str
        The name of the constraint.
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        SummaryConstraint -  a summary constraint defining a constraint on the distinct values of a feature
        to belong in a user supplied set of values

    """
    if name is None:
        ref_name = _format_set_values_for_display(reference_set)
        name = f"distinct values are in {ref_name}"

    return SummaryConstraint("distinct_column_values", Op.IN_SET, reference_set=reference_set, name=name, verbose=verbose)


def distinctValuesEqualSetConstraint(reference_set: Set[Any], name=None, verbose=False):
    """
    Defines a summary constraint on the distinct values of a feature. The set of the distinct values should
    be equal to the user-provided set or reference values, `reference_set`.
    Useful for categorical features, for checking if the set of values present in a feature
    is the same as the set of expected categories.

    Parameters
    ----------
    reference_set : Set[Any] (required)
        Represents the set of reference (expected) values for a feature.
        The provided values can be of any type.
        If the distinct values of the feature are not equal to the user specified
        set `reference_set`, then the constraint will fail.
    name : str
        The name of the constraint.
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        SummaryConstraint -  a summary constraint defining a constraint on the distinct values of a feature
        to be equal to a user supplied set of values

    """
    if name is None:
        ref_name = _format_set_values_for_display(reference_set)
        name = f"distinct values are equal to the set {ref_name}"

    return SummaryConstraint("distinct_column_values", Op.EQ_SET, reference_set=reference_set, name=name, verbose=verbose)


def distinctValuesContainSetConstraint(reference_set: Set[Any], name=None, verbose=False):
    """
    Defines a summary constraint on the distinct values of a feature. The set of user-supplied reference values,
    `reference_set` should be a subset of the set of distinct values for the current feature.
    Useful for categorical features, for checking if the set of values present in a feature
    is a superset of the set of expected categories.

    Parameters
    ----------
    reference_set : Set[Any] (required)
        Represents the set of reference (expected) values for a feature.
        The provided values can be of any type.
        If at least one of the values of the reference set, specified in `reference_set`,
        is not contained in the set of distinct values of the feature, then the constraint will fail.
    name : str
        The name of the constraint.
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        SummaryConstraint -  a summary constraint defining a constraint on the distinct values of a feature
        to be a super set of the user supplied set of values

    """
    if name is None:
        ref_name = _format_set_values_for_display(reference_set)
        name = f"distinct values contain the set {ref_name}"

    return SummaryConstraint("distinct_column_values", Op.CONTAIN_SET, reference_set=reference_set, name=name, verbose=verbose)


def columnValuesInSetConstraint(value_set: Set[Any], name=None, verbose=False):
    """
    Defines a value constraint with set operations on the values of a single feature.
    The values of the feature should all be in the set of user-supplied values,
    specified in `value_set`.
    Useful for categorical features, for checking if the values in a feature
    belong in a predefined set.

    Parameters
    ----------
    value_set : Set[Any] (required)
        Represents the set of expected values for a feature.
        The provided values can be of any type.
        Each value in the feature is checked against the constraint.
        The total number of failures equals the number of values not in the provided set `value_set`.
    name : str
        The name of the constraint.
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        ValueConstraint -  a value constraint specifying a constraint on the values of a feature
        to be drawn from a predefined set of values.

    """

    try:
        value_set = set(value_set)
    except Exception:
        raise TypeError("The value set should be an iterable data type")

    if name is None:
        val_name = _format_set_values_for_display(value_set)
        name = f"values are in {val_name}"

    return ValueConstraint(Op.IN, value=value_set, name=name, verbose=verbose)


def containsEmailConstraint(regex_pattern: "str" = None, name=None, verbose=False):
    """
    Defines a value constraint with email regex matching operations on the values of a single feature.
    The constraint defines a default email regex pattern, but a user-defined pattern can be supplied to override it.
    Useful for checking the validity of features with values representing email addresses.

    Parameters
    ----------
    regex_pattern : str (optional)
        User-defined email regex pattern.
        If supplied, will override the default email regex pattern provided by whylogs.
    name : str
        Name of the constraint used for reporting
    verbose : bool (optional)
            If true, log every application of this constraint that fails.
            Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        ValueConstraint - a value constraint for email regex matching of the values of a single feature

    """

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

    if name is None:
        name = "column values match the email regex pattern"

    return ValueConstraint(Op.MATCH, regex_pattern=email_pattern, name=name, verbose=verbose)


def containsCreditCardConstraint(regex_pattern: "str" = None, name=None, verbose=False):
    """
    Defines a value constraint with credit card number regex matching operations on the values of a single feature.
    The constraint defines a default credit card number regex pattern,
    but a user-defined pattern can be supplied to override it.
    Useful for checking the validity of features with values representing credit card numbers.

    Parameters
    ----------
    regex_pattern : str (optional)
        User-defined credit card number regex pattern.
        If supplied, will override the default credit card number regex pattern provided by whylogs.
    name : str
        Name of the constraint used for reporting
    verbose : bool (optional)
            If true, log every application of this constraint that fails.
            Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        ValueConstraint - a value constraint for credit card number regex matching of the values of a single feature

    """
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

    if name is None:
        name = "column values match the credit card regex pattern"

    return ValueConstraint(Op.MATCH, regex_pattern=credit_card_pattern, name=name, verbose=verbose)


def dateUtilParseableConstraint(name=None, verbose=False):
    """
    Defines a value constraint which checks if the values of a single feature
    can be parsed by the dateutil parser.
    Useful for checking if the date time values of a feature are compatible with dateutil.

    Parameters
    ----------
    name : str
        Name of the constraint used for reporting
    verbose : bool (optional)
            If true, log every application of this constraint that fails.
            Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        ValueConstraint - a value constraint for checking if a feature's values are dateutil parseable

    """
    if name is None:
        name = "column values are dateutil parseable"

    return ValueConstraint(Op.APPLY_FUNC, apply_function=_try_parse_dateutil, name=name, verbose=verbose)


def jsonParseableConstraint(name=None, verbose=False):
    """
    Defines a value constraint which checks if the values of a single feature
    are JSON parseable.
    Useful for checking if the values of a feature can be serialized to JSON.

    Parameters
    ----------
    name : str
        Name of the constraint used for reporting
    verbose : bool (optional)
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
       ValueConstraint - a value constraint for checking if a feature's values are JSON parseable

    """

    if name is None:
        name = "column values are JSON parseable"
    return ValueConstraint(Op.APPLY_FUNC, apply_function=_try_parse_json, name=name, verbose=verbose)


def matchesJsonSchemaConstraint(json_schema, name=None, verbose=False):
    """
    Defines a value constraint which checks if the values of a single feature
    match a user-provided JSON schema.
    Useful for checking if the values of a feature can be serialized to match a predefined JSON schema.

    Parameters
    ----------
    json_schema: Union[str, dict] (required)
        A string or dictionary of key-value pairs representing the expected JSON schema.
    name : str
        Name of the constraint used for reporting
    verbose : bool (optional)
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        ValueConstraint - a value constraint for checking if a feature's values match a user-provided JSON schema

    """

    if name is None:
        name = f"column values match the provided JSON schema {json_schema}"

    return ValueConstraint(Op.APPLY_FUNC, json_schema, apply_function=_matches_json_schema, name=name, verbose=verbose)


def strftimeFormatConstraint(format, name=None, verbose=False):
    """
    Defines a value constraint which checks if the values of a single feature
    are strftime parsable.

    Parameters
    ----------
    format: str (required)
        A string representing the expected strftime format for parsing the values.
    name : str
        Name of the constraint used for reporting
    verbose : bool (optional)
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        ValueConstraint - a value constraint for checking if a feature's values are strftime parseable

    """

    if name is None:
        name = "column values are strftime parseable"

    return ValueConstraint(Op.APPLY_FUNC, format, apply_function=_try_parse_strftime_format, name=name, verbose=verbose)


def containsSSNConstraint(regex_pattern: "str" = None, name=None, verbose=False):
    """
    Defines a value constraint with social security number (SSN) matching operations
    on the values of a single feature.
    The constraint defines a default SSN regex pattern, but a user-defined pattern can be supplied to override it.
    Useful for checking the validity of features with values representing SNN numbers.

    Parameters
    ----------
    regex_pattern : str (optional)
        User-defined SSN regex pattern.
        If supplied, will override the default SSN regex pattern provided by whylogs.
    name : str
        Name of the constraint used for reporting
    verbose : bool (optional)
            If true, log every application of this constraint that fails.
            Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        ValueConstraint - a value constraint for SSN regex matching of the values of a single feature

    """

    if regex_pattern is not None:
        logger.warning("Warning: supplying your own regex pattern might cause slower evaluation of the containsSSNConstraint, depending on its complexity.")
        ssn_pattern = regex_pattern
    else:
        ssn_pattern = r"^(?!000|666|9[0-9]{2})[0-9]{3}[\s-]?(?!00)[0-9]{2}[\s-]?(?!0000)[0-9]{4}$"

    if name is None:
        name = "column values match the SSN regex pattern"

    return ValueConstraint(Op.MATCH, regex_pattern=ssn_pattern, name=name, verbose=verbose)


def containsURLConstraint(regex_pattern: "str" = None, name=None, verbose=False):
    """
    Defines a value constraint with URL regex matching operations on the values of a single feature.
    The constraint defines a default URL regex pattern, but a user-defined pattern can be supplied to override it.
    Useful for checking the validity of features with values representing URL addresses.

    Parameters
    ----------
    regex_pattern : str (optional)
        User-defined URL regex pattern.
        If supplied, will override the default URL regex pattern provided by whylogs.
    name : str
        Name of the constraint used for reporting
    verbose : bool (optional)
            If true, log every application of this constraint that fails.
            Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        ValueConstraint - a value constraint for URL regex matching of the values of a single feature

    """

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

    if name is None:
        name = "column values match the URL regex pattern"

    return ValueConstraint(Op.MATCH, regex_pattern=url_pattern, name=name, verbose=verbose)


def stringLengthEqualConstraint(length: int, name=None, verbose=False):
    """
    Defines a value constraint which checks if the string values of a single feature
    have a predefined length.

    Parameters
    ----------
    length: int (required)
        A numeric value which represents the expected length of the string values in the specified feature.
    name : str
        Name of the constraint used for reporting
    verbose : bool (optional)
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        ValueConstraint - a value constraint for checking if a feature's string values have a predefined length

    """

    length_pattern = f"^.{{{length}}}$"
    if name is None:
        name = f"length of the string values is equal to {length}"
    return ValueConstraint(Op.MATCH, regex_pattern=length_pattern, name=name, verbose=verbose)


def stringLengthBetweenConstraint(lower_value: int, upper_value: int, name=None, verbose=False):
    """
    Defines a value constraint which checks if the string values' length of a single feature
    is in some predefined interval.

    Parameters
    ----------
    lower_value: int (required)
        A numeric value which represents the expected lower bound of the length
        of the string values in the specified feature.
    upper_value: int (required)
        A numeric value which represents the expected upper bound of the length
        of the string values in the specified feature.
    name : str
        Name of the constraint used for reporting
    verbose : bool (optional)
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        ValueConstraint - a value constraint for checking if a feature's string values'
        length is in a predefined interval

    """

    length_pattern = rf"^.{{{lower_value},{upper_value}}}$"
    if name is None:
        name = f"length of the string values is between {lower_value} and {upper_value}"
    return ValueConstraint(Op.MATCH, regex_pattern=length_pattern, name=name, verbose=verbose)


def quantileBetweenConstraint(
    quantile_value: Union[int, float], lower_value: Union[int, float], upper_value: Union[int, float], name=None, verbose: "bool" = False
):
    """
    Defines a summary constraint on the n-th quantile value of a numeric feature.
    The n-th quantile can be defined to be between two values.
    The defined interval is a closed interval, which includes both of its limit points.

    Parameters
    ----------
    quantile_value: numeric (required)
        The n-the quantile for which the constraint will be executed
    lower_value : numeric (required)
        Represents the lower value limit of the interval for the n-th quantile.
    upper_value : numeric (required)
        Represents the upper value limit of the interval for the n-th quantile.
    name : str
        Name of the constraint used for reporting
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        SummaryConstraint -  a summary constraint defining a closed interval of valid values
        for the n-th quantile value of a specific feature

    """

    if not all([isinstance(v, (int, float)) for v in (quantile_value, upper_value, lower_value)]):
        raise TypeError("The quantile, lower and upper values must be of type int or float")

    if lower_value > upper_value:
        raise ValueError("The lower value must be less than or equal to the upper value")

    if name is None:
        name = f"{quantile_value}-th quantile value is between {lower_value} and {upper_value}"
    return SummaryConstraint("quantile", value=lower_value, upper_value=upper_value, quantile_value=quantile_value, op=Op.BTWN, name=name, verbose=verbose)


def columnUniqueValueCountBetweenConstraint(lower_value: int, upper_value: int, name=None, verbose: bool = False):
    """
    Defines a summary constraint on the cardinality of a specific feature.
    The cardinality can be defined to be between two values.
    The defined interval is a closed interval, which includes both of its limit points.
    Useful for checking the unique count of values for discrete features.

    Parameters
    ----------
    lower_value : numeric (required)
        Represents the lower value limit of the interval for the feature cardinality.
    upper_value : numeric (required)
        Represents the upper value limit of the interval for the feature cardinality.
    name : str
        Name of the constraint used for reporting
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        SummaryConstraint -  a summary constraint defining a closed interval
        for the valid cardinality of a specific feature

    """
    if not all([isinstance(v, int) and v >= 0 for v in (lower_value, upper_value)]):
        raise ValueError("The lower and upper values should be non-negative integers")

    if lower_value > upper_value:
        raise ValueError("The lower value should be less than or equal to the upper value")

    if name is None:
        name = f"number of unique values is between {lower_value} and {upper_value}"
    return SummaryConstraint("unique_count", op=Op.BTWN, value=lower_value, upper_value=upper_value, name=name, verbose=verbose)


def columnUniqueValueProportionBetweenConstraint(lower_fraction: float, upper_fraction: float, name=None, verbose: bool = False):
    """
    Defines a summary constraint on the proportion of unique values of a specific feature.
    The proportion of unique values can be defined to be between two values.
    The defined interval is a closed interval, which includes both of its limit points.
    Useful for checking the frequency of unique values for discrete features.

    Parameters
    ----------
    lower_fraction : fraction between 0 and 1 (required)
        Represents the lower fraction limit of the interval for the feature unique value proportion.
    upper_fraction : fraction between 0 and 1 (required)
        Represents the upper fraction limit of the interval for the feature cardinality.
    name : str
        Name of the constraint used for reporting
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        SummaryConstraint -  a summary constraint defining a closed interval
        for the valid proportion of unique values of a specific feature

    """
    if not all([isinstance(v, float) and 0 <= v <= 1 for v in (lower_fraction, upper_fraction)]):
        raise ValueError("The lower and upper fractions should be between 0 and 1")

    if lower_fraction > upper_fraction:
        raise ValueError("The lower fraction should be decimal values less than or equal to the upper fraction")

    if name is None:
        name = f"proportion of unique values is between {lower_fraction} and {upper_fraction}"
    return SummaryConstraint("unique_proportion", op=Op.BTWN, value=lower_fraction, upper_value=upper_fraction, name=name, verbose=verbose)


def columnExistsConstraint(column: str, name=None, verbose=False):
    """
    Defines a constraint on the data set schema.
    Checks if the user-supplied column, identified by `column`, is present in the data set schema.

    Parameters
    ----------
    column : str (required)
        Represents the name of the column to be checked for existence in the data set.
    name : str
        Name of the constraint used for reporting
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        SummaryConstraint -  a summary constraint which checks the existence of a column
        in the current data set.

    """
    if name is None:
        name = f"The column {column} exists in the table"
    return SummaryConstraint("columns", Op.CONTAIN, value=column, name=name, verbose=verbose)


def numberOfRowsConstraint(n_rows: int, name=None, verbose=False):
    """
    Defines a constraint on the data set schema.
    Checks if the number of rows in the data set equals the user-supplied number of rows.

    Parameters
    ----------
    n_rows : int (required)
        Represents the user-supplied expected number of rows.
    name : str
        Name of the constraint used for reporting
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        SummaryConstraint -  a summary constraint which checks the number of rows in the data set

    """

    if name is None:
        name = f"The number of rows in the table equals {n_rows}"
    return SummaryConstraint("total_row_number", Op.EQ, value=n_rows, name=name, verbose=verbose)


def columnsMatchSetConstraint(reference_set: Set[str], name=None, verbose=False):
    """
    Defines a constraint on the data set schema.
    Checks if the set of columns in the data set is equal to the user-supplied set of expected columns.

    Parameters
    ----------
    reference_set : Set[str] (required)
        Represents the expected columns in the current data set.
    name : str
        Name of the constraint used for reporting
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        SummaryConstraint -  a summary constraint which checks if the column set
        of the current data set matches the expected column set

    """
    if name is None:
        ref_name = _format_set_values_for_display(reference_set)
        name = f"The columns of the table are equal to the set {ref_name}"

    return SummaryConstraint("columns", Op.EQ, reference_set=reference_set, name=name, verbose=verbose)


def columnMostCommonValueInSetConstraint(value_set: Set[Any], name=None, verbose=False):
    """
    Defines a summary constraint on the most common value of a feature.
    The most common value of the feature should be in the set of user-supplied values, `value_set`.
    Useful for categorical features, for checking if the most common value of a feature
    belongs in an expected set of common categories.

    Parameters
    ----------
    value_set : Set[Any] (required)
        Represents the set of expected values for a feature.
        The provided values can be of any type.
        If the most common value of the feature is not in the values of the user-specified `value_set`,
        the constraint will fail.
    name : str
        The name of the constraint.
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        SummaryConstraint -  a summary constraint defining a constraint on the most common value of a feature
        to belong to a set of user-specified expected values

    """
    try:
        value_set = set(value_set)
    except Exception:
        raise TypeError("The value set should be an iterable data type")

    if name is None:
        val_name = _format_set_values_for_display(value_set)
        name = f"most common value is in {val_name}"

    return SummaryConstraint("most_common_value", op=Op.IN, reference_set=value_set, name=name, verbose=verbose)


def columnValuesNotNullConstraint(name=None, verbose=False):
    """
    Defines a non-null summary constraint on the value of a feature.
    Useful for features for which there is no tolerance for missing values.
    The constraint will fail if there is at least one missing value in the specified feature.

    Parameters
    ----------
    name : str
        The name of the constraint.
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        SummaryConstraint -  a summary constraint defining that no missing values
        are allowed for the specified feature

    """

    if name is None:
        name = "does not contain missing values"
    return SummaryConstraint("null_count", value=0, op=Op.EQ, name=name, verbose=verbose)


def missingValuesProportionBetweenConstraint(lower_fraction: float, upper_fraction: float, name: str = None, verbose: bool = False):
    """
    Defines a summary constraint on the proportion of missing values of a specific feature.
    The proportion of missing values can be defined to be between two frequency values.
    The defined interval is a closed interval, which includes both of its limit points.
    Useful for checking features with expected amounts of missing values.

    Parameters
    ----------
    lower_fraction : fraction between 0 and 1 (required)
        Represents the lower fraction limit of the interval for the feature missing value proportion.
    upper_fraction : fraction between 0 and 1 (required)
        Represents the upper fraction limit of the interval for the feature missing value proportion.
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.
    Returns
    -------
        SummaryConstraint -  a summary constraint defining a closed interval
        for the valid proportion of missing values of a specific feature

    """

    if not all([isinstance(v, float) and 0 <= v <= 1 for v in (lower_fraction, upper_fraction)]):
        raise ValueError("The lower and upper fractions should be between 0 and 1")

    if lower_fraction > upper_fraction:
        raise ValueError("The lower fraction should be decimal values less than or equal to the upper fraction")

    if not name:
        name = f"missing values proportion is between {lower_fraction * 100}% and {upper_fraction * 100}%"

    return SummaryConstraint("missing_values_proportion", op=Op.BTWN, value=lower_fraction, upper_value=upper_fraction, name=name, verbose=verbose)


def columnValuesTypeEqualsConstraint(expected_type: Union[InferredType, int], name=None, verbose: bool = False):
    """
    Defines a summary constraint on the type of the feature values.
    The type of values should be equal to the user-provided expected type.

    Parameters
    ----------
    expected_type: Union[InferredType, int]
        whylogs.proto.InferredType.Type - Enumeration of allowed inferred data types
        If supplied as integer value, should be one of:
            UNKNOWN = 0
            NULL = 1
            FRACTIONAL = 2
            INTEGRAL = 3
            BOOLEAN = 4
            STRING = 5
    name : str
        Name of the constraint used for reporting
    verbose: bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        SummaryConstraint - a summary constraint defining that the feature values type should be
            equal to a user-provided expected type

    """

    if not isinstance(expected_type, (InferredType, int)):
        raise ValueError("The expected_type parameter should be of type whylogs.proto.InferredType or int")
    if isinstance(expected_type, InferredType):
        expected_type = expected_type.type

    if name is None:
        name = f"type of the column values is {InferredType.Type.Name(expected_type)}"

    return SummaryConstraint("column_values_type", op=Op.EQ, value=expected_type, name=name, verbose=verbose)


def columnValuesTypeInSetConstraint(type_set: Set[int], name=None, verbose: bool = False):
    """
    Defines a summary constraint on the type of the feature values.
    The type of values should be in the set of to the user-provided expected types.

    Parameters
    ----------
    type_set: Set[int]
        whylogs.proto.InferredType.Type - Enumeration of allowed inferred data types
        If supplied as integer value, should be one of:
            UNKNOWN = 0
            NULL = 1
            FRACTIONAL = 2
            INTEGRAL = 3
            BOOLEAN = 4
            STRING = 5
    name : str
        Name of the constraint used for reporting
    verbose: bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
    SummaryConstraint - a summary constraint defining that the feature values type should be
        in the set of user-provided expected types

    """

    try:
        type_set = set(type_set)
    except Exception:
        raise TypeError("The type_set parameter should be an iterable of int values")

    if not all([isinstance(t, int) for t in type_set]):
        raise TypeError("All of the elements of the type_set parameter should be of type int")

    if name is None:
        type_names = {InferredType.Type.Name(t) if isinstance(t, int) else InferredType.Type.Name(t.type) for t in type_set}
        type_names = _format_set_values_for_display(type_names)
        name = f"type of the column values is in {type_names}"

    return SummaryConstraint("column_values_type", op=Op.IN, reference_set=type_set, name=name, verbose=verbose)


def approximateEntropyBetweenConstraint(lower_value: Union[int, float], upper_value: float, name=None, verbose=False):
    """
    Defines a summary constraint specifying the expected interval of the features estimated entropy.
    The defined interval is a closed interval, which includes both of its limit points.

    Parameters
    ----------
    lower_value : numeric (required)
        Represents the lower value limit of the interval for the feature's estimated entropy.
    upper_value : numeric (required)
        Represents the upper value limit of the interval for the feature's estimated entropy.
    name : str
        Name of the constraint used for reporting
    verbose: bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        SummaryConstraint - a summary constraint defining the interval of valid values
        of the feature's estimated entropy

    """

    if not all([isinstance(v, (int, float)) for v in (lower_value, upper_value)]):
        raise TypeError("The lower and upper values should be of type int or float")
    if not all([v >= 0 for v in (lower_value, upper_value)]):
        raise ValueError("The value of the entropy cannot be a negative number")
    if lower_value > upper_value:
        raise ValueError("The supplied lower bound should be less than or equal to the supplied upper bound")

    if name is None:
        name = f"approximate entropy is between {lower_value} and {upper_value}"

    return SummaryConstraint("entropy", op=Op.BTWN, value=lower_value, upper_value=upper_value, name=name, verbose=verbose)


def parametrizedKSTestPValueGreaterThanConstraint(reference_distribution: Union[List[float], np.ndarray], p_value=0.05, name=None, verbose=False):
    """
    Defines a summary constraint specifying the expected
    upper limit of the p-value for rejecting the null hypothesis of the KS test.
    Can be used only for continuous data.

    Parameters
    ----------
    reference_distribution: Array-like
        Represents the reference distribution for calculating the KS Test p_value of the column,
        should be an array-like object with floating point numbers,
        Only numeric distributions are accepted
    p_value: float
        Represents the reference p_value value to compare with the p_value of the test
        Should be between 0 and 1, inclusive
    name : str
        Name of the constraint used for reporting
    verbose: bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        SummaryConstraint - a summary constraint specifying the upper limit of the
        KS test p-value for rejecting the null hypothesis

    """

    if not isinstance(p_value, float):
        raise TypeError("The p_value should be a of type float")

    if not 0 <= p_value <= 1:
        raise ValueError("The p_value should be a float value between 0 and 1 inclusive")

    if not isinstance(reference_distribution, (list, np.ndarray)):
        raise TypeError("The reference distribution must be a list or numpy array with float values")

    kll_floats = datasketches.kll_floats_sketch(DEFAULT_HIST_K)
    for value in reference_distribution:
        if TypedDataConverter.get_type(value) != TYPES.FRACTIONAL:
            raise ValueError("The reference distribution should be a continuous distribution")
        kll_floats.update(value)

    if name is None:
        name = f"parametrized KS test p-value is greater than {p_value}"

    return SummaryConstraint("ks_test", op=Op.GT, reference_set=kll_floats, value=p_value, name=name, verbose=verbose)


def columnKLDivergenceLessThanConstraint(reference_distribution: Union[List[Any], np.ndarray], threshold: float = 0.5, name=None, verbose: bool = False):
    """
    Defines a summary constraint specifying the expected
    upper limit of the threshold for the KL divergence of the specified feature.

    Parameters
    ----------
    reference_distribution: Array-like
        Represents the reference distribution for calculating the KL Divergence of the column,
        should be an array-like object with floating point numbers, or integers, strings and booleans, but not both
        Both numeric and categorical distributions are accepted
    threshold: float
        Represents the threshold value which if exceeded from the KL Divergence, the constraint would fail
    name : str
        Name of the constraint used for reporting
    verbose: bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        SummaryConstraint - a summary constraint specifying the upper threshold of the
        feature's KL divergence

    """

    if not isinstance(reference_distribution, (list, np.ndarray)):
        raise TypeError("The reference distribution should be an array-like instance of values")
    if not isinstance(threshold, float):
        raise TypeError("The threshold value should be of type float")

    type_error_message = (
        "The provided reference distribution should have only categorical (int, string, bool) or only numeric types (float, double) of values, but not both"
    )

    cardinality_sketch = HllSketch()
    frequent_items_sketch = FrequentItemsSketch()
    quantiles_sketch = datasketches.kll_floats_sketch(DEFAULT_HIST_K)
    data_type = TYPES.UNKNOWN
    categorical_types = (TYPES.INTEGRAL, TYPES.STRING, TYPES.BOOLEAN)
    numeric_types = (TYPES.FRACTIONAL,)
    total_count = 0

    for value in reference_distribution:
        value_type = TypedDataConverter.get_type(value)
        if value_type in numeric_types:
            if data_type not in (TYPES.UNKNOWN,) + numeric_types:
                raise TypeError(type_error_message)
            quantiles_sketch.update(value)
            data_type = value_type
        elif value_type in categorical_types:
            if data_type not in (TYPES.UNKNOWN,) + categorical_types:
                raise TypeError(type_error_message)
            cardinality_sketch.update(value)
            frequent_items_sketch.update(value)
            total_count += 1
            data_type = value_type
        else:
            raise TypeError(type_error_message)

    if data_type in numeric_types:
        ref_summary = quantiles_sketch
    else:
        ref_summary = ReferenceDistributionDiscreteMessage(
            frequent_items=frequent_items_sketch.to_summary(), unique_count=cardinality_sketch.to_summary(), total_count=total_count
        )

    if name is None:
        name = f"KL Divergence is less than {threshold}"

    return SummaryConstraint("kl_divergence", op=Op.LT, reference_set=ref_summary, value=threshold, name=name, verbose=verbose)


def columnChiSquaredTestPValueGreaterThanConstraint(
    reference_distribution: Union[List[Any], np.ndarray, Mapping[str, int]], p_value: float = 0.05, name=None, verbose: bool = False
):
    """
    Defines a summary constraint specifying the expected
    upper limit of the p-value for rejecting the null hypothesis of the Chi-Squared test.
    Can be used only for discrete data.

    Parameters
    ----------
    reference_distribution: Array-like
        Represents the reference distribution for calculating the Chi-Squared test,
        should be an array-like object with integer, string or boolean values
        or a mapping of type key: value where the keys are the items and the values are the per-item counts
        Only categorical distributions are accepted
    p_value: float
        Represents the reference p_value value to compare with the p_value of the test
        Should be between 0 and 1, inclusive
    name : str
        Name of the constraint used for reporting
    verbose: bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        SummaryConstraint - a summary constraint specifying the upper limit of the
        Chi-Squared test p-value for rejecting the null hypothesis
    """

    if not isinstance(reference_distribution, (list, np.ndarray, dict)):
        raise TypeError("The reference distribution should be an array-like instance of float values, or a mapping of the counts of the expected items")
    if not isinstance(p_value, float) or not 0 <= p_value <= 1:
        raise TypeError("The p-value should be a float value between 0 and 1 inclusive")

    categorical_types = (TYPES.INTEGRAL, TYPES.STRING, TYPES.BOOLEAN)
    frequent_items_sketch = FrequentItemsSketch()

    if isinstance(reference_distribution, dict):
        frequency_sum = 0
        for item, frequency in reference_distribution.items():
            if TypedDataConverter.get_type(item) not in categorical_types or not isinstance(frequency, int):
                raise ValueError("The provided frequent items mapping should contain only str, int or bool values as items and int values as counts per item")
            frequent_items_sketch.update(item, frequency)
            frequency_sum += frequency
    else:
        frequency_sum = len(reference_distribution)
        for value in reference_distribution:
            if TypedDataConverter.get_type(value) not in categorical_types:
                raise ValueError("The provided values in the reference distribution should all be of categorical type (str, int or bool)")
            frequent_items_sketch.update(value)

    ref_dist = ReferenceDistributionDiscreteMessage(frequent_items=frequent_items_sketch.to_summary(), total_count=frequency_sum)

    if name is None:
        name = f"Chi-Squared test p-value is greater than {p_value}"

    return SummaryConstraint("chi_squared_test", op=Op.GT, reference_set=ref_dist, value=p_value, name=name, verbose=verbose)


def columnValuesAGreaterThanBConstraint(column_A: str, column_B: str, name=None, verbose: bool = False):
    """
    Defines a multi-column value constraint which specifies that each value in column A,
    specified in `column_A`, is greater than the corresponding value of column B, specified in `column_B`
    in the same row.

    Parameters
    ----------
    column_A : str
        The name of column A
    column_B : str
        The name of column B
    name : str
        Name of the constraint used for reporting
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        MultiColumnValueConstraint - multi-column value constraint specifying that values from column A
        should always be greater than the corresponding values of column B

    """

    if not all([isinstance(col, str)] for col in (column_A, column_B)):
        raise TypeError("The provided dependent_column and reference_column should be of type str, indicating the name of the columns to be compared")

    if name is None:
        name = f"The values of the column {column_A} are greater than the corresponding values of the column {column_B}"

    return MultiColumnValueConstraint(column_A, op=Op.GT, reference_columns=column_B, name=name, verbose=verbose)


def columnValuesAGreaterThanEqualBConstraint(column_A: str, column_B: str, name=None, verbose: bool = False):
    """
    Defines a multi-column value constraint which specifies that each value in column A,
    specified in `column_A`, is greater than or equal to the corresponding value of column B,
    specified in `column_B` in the same row.

    Parameters
    ----------
    column_A : str
        The name of column A
    column_B : str
        The name of column B
    name : str
        Name of the constraint used for reporting
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        MultiColumnValueConstraint - multi-column value constraint specifying that values from column A
        should always be greater than or equal to the corresponding values of column B

    """

    if not all([isinstance(col, str)] for col in (column_A, column_B)):
        raise TypeError("The provided dependent_column and reference_column should be of type str, indicating the name of the columns to be compared")

    if name is None:
        name = f"The values of the column {column_A} are greater than or equal to the corresponding values of the column {column_B}"

    return MultiColumnValueConstraint(column_A, op=Op.GE, reference_columns=column_B, name=name, verbose=verbose)


def columnValuesALessThanBConstraint(column_A: str, column_B: str, name=None, verbose: bool = False):
    """
    Defines a multi-column value constraint which specifies that each value in column A,
    specified in `column_A`, is less than the corresponding value of column B, specified in `column_B`
    in the same row.

    Parameters
    ----------
    column_A : str
        The name of column A
    column_B : str
        The name of column B
    name : str
        Name of the constraint used for reporting
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        MultiColumnValueConstraint - multi-column value constraint specifying that values from column A
        should always be less the corresponding values of column B

    """

    if not all([isinstance(col, str)] for col in (column_A, column_B)):
        raise TypeError("The provided dependent_column and reference_column should be of type str, indicating the name of the columns to be compared")

    if name is None:
        name = f"The values of the column {column_A} are less than the corresponding values of the column {column_B}"

    return MultiColumnValueConstraint(column_A, op=Op.LT, reference_columns=column_B, name=name, verbose=verbose)


def columnValuesALessThanEqualBConstraint(column_A: str, column_B: str, name=None, verbose: bool = False):
    """
    Defines a multi-column value constraint which specifies that each value in column A,
    specified in `column_A`, is less than or equal to the corresponding value of column B, specified in `column_B`
    in the same row.

    Parameters
    ----------
    column_A : str
        The name of column A
    column_B : str
        The name of column B
    name : str
        Name of the constraint used for reporting
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        MultiColumnValueConstraint - multi-column value constraint specifying that values from column A
        should always be less than or equal to the corresponding values of column B

    """

    if not all([isinstance(col, str)] for col in (column_A, column_B)):
        raise TypeError("The provided dependent_column and reference_column should be of type str, indicating the name of the columns to be compared")

    if name is None:
        name = f"The values of the column {column_A} are less than or equal to the corresponding values of the column {column_B}"

    return MultiColumnValueConstraint(column_A, op=Op.LE, reference_columns=column_B, name=name, verbose=verbose)


def columnValuesAEqualBConstraint(column_A: str, column_B: str, name=None, verbose: bool = False):
    """
    Defines a multi-column value constraint which specifies that each value in column A,
    specified in `column_A`, is equal to the corresponding value of column B, specified in `column_B`
    in the same row.

    Parameters
    ----------
    column_A : str
        The name of column A
    column_B : str
        The name of column B
    name : str
        Name of the constraint used for reporting
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        MultiColumnValueConstraint - multi-column value constraint specifying that values from column A
        should always be equal to the corresponding values of column B

    """

    if not all([isinstance(col, str)] for col in (column_A, column_B)):
        raise TypeError("The provided dependent_column and reference_column should be of type str, indicating the name of the columns to be compared")

    if name is None:
        name = f"The values of the column {column_A} are equal to the corresponding values of the column {column_B}"

    return MultiColumnValueConstraint(column_A, op=Op.EQ, reference_columns=column_B, name=name, verbose=verbose)


def columnValuesANotEqualBConstraint(column_A: str, column_B: str, name=None, verbose: bool = False):
    """
    Defines a multi-column value constraint which specifies that each value in column A,
    specified in `column_A`, is different from the corresponding value of column B, specified in `column_B`
    in the same row.

    Parameters
    ----------
    column_A : str
        The name of column A
    column_B : str
        The name of column B
    name : str
        Name of the constraint used for reporting
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        MultiColumnValueConstraint - multi-column value constraint specifying that values from column A
        should always be different from the corresponding values of column B

    """

    if not all([isinstance(col, str)] for col in (column_A, column_B)):
        raise TypeError("The provided dependent_column and reference_column should be of type str, indicating the name of the columns to be compared")

    if name is None:
        name = f"The values of the column {column_A} are not equal to the corresponding values of the column {column_B}"

    return MultiColumnValueConstraint(column_A, op=Op.NE, reference_columns=column_B, name=name, verbose=verbose)


def sumOfRowValuesOfMultipleColumnsEqualsConstraint(
    columns: Union[List[str], Set[str], np.array], value: Union[float, int, str], name=None, verbose: bool = False
):
    """
    Defines a multi-column value constraint which specifies that the sum of the values in each row
    of the provided columns, specified in `columns`, should be equal to the user-predefined value, specified in `value`,
    or to the corresponding value of another column, which will be specified with a name in the `value` parameter.

    Parameters
    ----------
    columns : List[str]
        List of columns for which the sum of row values should equal the provided-value
    value : Union[float, int, str]
        Numeric value to compare with the sum of the column row values,
        or a string indicating a column name for which the row value will be compared with the sum
    name : str
        Name of the constraint used for reporting
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        MultiColumnValueConstraint - specifying the expected value of the sum of the values in multiple columns

    """

    if not isinstance(columns, (list, set, np.array)) or not all(isinstance(col, str) for col in columns):
        raise TypeError(
            "The column list should be an array-like data type of only string values, indicating the column names, for which the values are going to be summed"
        )

    if not isinstance(value, (float, int, str)):
        raise TypeError(
            "The value should be a numeric value equal to the expected sum,"
            " or a string indicating the column name for which the row value will be taken as the reference sum value"
        )

    reference_cols = None
    if isinstance(value, str):
        reference_cols = [value]
        value = None

    if name is None:
        coumn_names = ""
        for i in range(len(columns) - 1):
            if i == len(columns) - 2:
                coumn_names += columns[i] + " "
            else:
                coumn_names += columns[i] + ", "
        coumn_names += "and " + columns[-1]
        value_or_column_name = f"the corresponding value of the column {reference_cols[0]}" if reference_cols else value
        name = f"The sum of the values of {coumn_names} is equal to {value_or_column_name}"

    return MultiColumnValueConstraint(
        dependent_columns=columns, op=Op.EQ, value=value, reference_columns=reference_cols, internal_dependent_cols_op=Op.SUM, name=name, verbose=verbose
    )


def columnPairValuesInSetConstraint(column_A: str, column_B: str, value_set: Set[Tuple[Any, Any]], name=None, verbose: bool = False):
    """
    Defines a multi-column value constraint which specifies that the pair of values of columns A and B,
    should be in a user-predefined set of expected pairs of values.

    Parameters
    ----------
    column_A : str
        The name of the first column
    column_B : str
        The name of the second column
    value_set : Set[Tuple[Any, Any]]
        A set of expected pairs of values for the columns A and B, in that order
    name : str
        Name of the constraint used for reporting
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        MultiColumnValueConstraint - specifying the expected set of value pairs of two columns in the data set

    """

    if not all([isinstance(col, str) for col in (column_A, column_B)]):
        raise TypeError("The provided column_A and column_B should be of type str, indicating the name of the columns to be compared")
    if isinstance(value_set, str):
        raise TypeError("The value_set should be an array-like data type of tuple values")

    try:
        value_set = set(value_set)
    except Exception:
        raise TypeError("The value_set should be an array-like data type of tuple values")

    if name is None:
        val_name = _format_set_values_for_display(value_set)
        name = f"The pair of values of the columns {column_A} and {column_B} are in {val_name}"
    return MultiColumnValueConstraint(dependent_columns=[column_A, column_B], op=Op.IN, value=value_set, name=name, verbose=verbose)


def columnValuesUniqueWithinRow(column_A: str, name=None, verbose: bool = False):
    """
    Defines a multi-column value constraint which specifies that the values of column A
    should be unique within each row of the data set.

    Parameters
    ----------
    column_A : str
        The name of the column for which it is expected that the values are unique within each row
    name : str
        Name of the constraint used for reporting
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        MultiColumnValueConstraint - specifying that the provided column's values are unique within each row

    """

    if not isinstance(column_A, str):
        raise TypeError("The provided column_A should be of type str, indicating the name of the column to be checked")

    if name is None:
        name = f"The values of the column {column_A} are unique within each row"
    return MultiColumnValueConstraint(dependent_columns=column_A, op=Op.NOT_IN, reference_columns="all", name=name, verbose=verbose)
