import json
from logging import getLogger

import numpy as np
import pandas as pd
import pytest

from whylogs.app.config import load_config
from whylogs.app.session import session_from_config
from whylogs.core.statistics.constraints import (
    MAX_SET_DISPLAY_MESSAGE_LENGTH,
    DatasetConstraints,
    MultiColumnValueConstraint,
    Op,
    SummaryConstraint,
    SummaryConstraints,
    ValueConstraint,
    ValueConstraints,
    _matches_json_schema,
    _summary_funcs1,
    _try_parse_dateutil,
    _try_parse_json,
    _try_parse_strftime_format,
    _value_funcs,
    approximateEntropyBetweenConstraint,
    columnChiSquaredTestPValueGreaterThanConstraint,
    columnExistsConstraint,
    columnKLDivergenceLessThanConstraint,
    columnMostCommonValueInSetConstraint,
    columnPairValuesInSetConstraint,
    columnsMatchSetConstraint,
    columnUniqueValueCountBetweenConstraint,
    columnUniqueValueProportionBetweenConstraint,
    columnValuesAGreaterThanBConstraint,
    columnValuesInSetConstraint,
    columnValuesNotNullConstraint,
    columnValuesTypeEqualsConstraint,
    columnValuesTypeInSetConstraint,
    columnValuesUniqueWithinRow,
    containsCreditCardConstraint,
    containsEmailConstraint,
    containsSSNConstraint,
    containsURLConstraint,
    dateUtilParseableConstraint,
    distinctValuesContainSetConstraint,
    distinctValuesEqualSetConstraint,
    distinctValuesInSetConstraint,
    jsonParseableConstraint,
    matchesJsonSchemaConstraint,
    maxBetweenConstraint,
    meanBetweenConstraint,
    minBetweenConstraint,
    numberOfRowsConstraint,
    parametrizedKSTestPValueGreaterThanConstraint,
    quantileBetweenConstraint,
    stddevBetweenConstraint,
    strftimeFormatConstraint,
    stringLengthBetweenConstraint,
    stringLengthEqualConstraint,
    sumOfRowValuesOfMultipleColumnsEqualsConstraint,
)
from whylogs.proto import InferredType, Op
from whylogs.util.protobuf import message_to_json

TEST_LOGGER = getLogger(__name__)


def test_value_summary_serialization():
    for each_op, _ in _value_funcs.items():
        if each_op == Op.APPLY_FUNC:
            continue
        if each_op == Op.IN:
            value = ValueConstraint(each_op, {3.6})
        else:
            value = ValueConstraint(each_op, 3.6)
        msg_value = value.to_protobuf()
        json_value = json.loads(message_to_json(msg_value))
        if each_op == Op.IN:
            assert json_value["name"] == "value " + Op.Name(each_op) + " {3.6}"
            assert json_value["valueSet"][0] == [3.6]
        else:
            assert json_value["name"] == "value " + Op.Name(each_op) + " 3.6"
            assert pytest.approx(json_value["value"], 0.001) == 3.6
        assert json_value["op"] == Op.Name(each_op)
        assert json_value["verbose"] is False

    for each_op, _ in _summary_funcs1.items():
        if each_op in (Op.BTWN, Op.IN_SET, Op.CONTAIN_SET, Op.EQ_SET, Op.CONTAIN, Op.IN):
            continue
        # constraints may have an optional name
        sum_constraint = SummaryConstraint("min", each_op, 300000, name="< 30K")
        msg_sum_const = sum_constraint.to_protobuf()
        json_summary = json.loads(message_to_json(msg_sum_const))

        assert json_summary["name"] == "< 30K"
        assert pytest.approx(json_summary["value"], 0.1) == 300000
        assert json_summary["firstField"] == "min"
        assert json_summary["op"] == str(Op.Name(each_op))
        assert json_summary["verbose"] is False


def test_value_constraints(df_lending_club, local_config_path):
    conforming_loan = ValueConstraint(Op.LT, 548250)
    smallest_loan = ValueConstraint(Op.GT, 2500.0, verbose=True)

    high_fico = ValueConstraint(Op.GT, 4000)

    dc = DatasetConstraints(None, value_constraints={"loan_amnt": [conforming_loan, smallest_loan], "fico_range_high": [high_fico]})

    config = load_config(local_config_path)
    session = session_from_config(config)

    profile = session.log_dataframe(df_lending_club, "test.data", constraints=dc)
    session.close()
    report = dc.report()

    assert len(report) == 2
    # make sure it checked every value
    for each_feat in report:
        for each_constraint in each_feat[1]:
            assert each_constraint[1] == 50

    assert report[1][1][0][2] == 50


def test_value_constraints_pattern_match(df_lending_club, local_config_path):
    regex_state_abbreviation = r"^[a-zA-Z]{2}$"
    contains_state = ValueConstraint(Op.MATCH, regex_pattern=regex_state_abbreviation)

    regex_date = r"^[a-zA-Z]{3}-[0-9]{4}$"
    not_contains_date = ValueConstraint(Op.NOMATCH, regex_pattern=regex_date)

    # just to test applying regex patterns on non-string values
    contains_state_loan_amnt = ValueConstraint(Op.MATCH, regex_pattern=regex_state_abbreviation)

    dc = DatasetConstraints(
        None, value_constraints={"addr_state": [contains_state], "earliest_cr_line": [not_contains_date], "loan_amnt": [contains_state_loan_amnt]}
    )

    config = load_config(local_config_path)
    session = session_from_config(config)

    profile = session.log_dataframe(df_lending_club, "test.data", constraints=dc)
    session.close()
    report = dc.report()
    # checks there are constraints for 3 features
    assert len(report) == 3
    # make sure it checked every value
    for each_feat in report:
        for each_constraint in each_feat[1]:
            assert each_constraint[1] == 50

    # Every row should match a state abbreviation
    assert report[0][1][0][2] == 0

    # At least 1 should be a match w/ the given pattern (# of failures of NOMATCH = # Matches)
    assert report[1][1][0][2] > 0

    # Every row should be a failure, because "loan_amnt" is not a string type
    assert report[2][1][0][2] == 50


def test_summary_constraints(df_lending_club, local_config_path):
    non_negative = SummaryConstraint("min", Op.GE, 0)

    dc = DatasetConstraints(None, summary_constraints={"annual_inc": [non_negative]})
    config = load_config(local_config_path)
    session = session_from_config(config)
    profile = session.log_dataframe(df_lending_club, "test.data", constraints=dc)
    session.close()
    report = r = profile.apply_summary_constraints()

    assert len(report) == 1
    # make sure it checked every value
    for each_feat in report:
        for each_constraint in each_feat[1]:
            assert each_constraint[1] == 1


def test_value_constraints_no_merge_different_names():
    constraint1 = ValueConstraint(Op.LT, 1, name="c1")
    constraint2 = ValueConstraint(Op.LT, 1, name="c2")
    with pytest.raises(AssertionError):
        constraint1.merge(constraint2)


def test_value_constraints_no_merge_different_values():
    constraint1 = ValueConstraint(Op.LT, 1)
    constraint2 = ValueConstraint(Op.LT, 2)
    with pytest.raises(AssertionError):
        constraint1.merge(constraint2)


def test_summary_constraints_no_merge_different_names():
    constraint1 = SummaryConstraint("min", Op.GE, 0, name="non-negative")
    constraint2 = SummaryConstraint("min", Op.GE, 0, name="positive-number")
    with pytest.raises(AssertionError):
        constraint1.merge(constraint2)


def test_summary_constraints_no_merge_different_values():
    constraint1 = SummaryConstraint("min", Op.GE, 1, name="GreaterThanThreshold")
    constraint2 = SummaryConstraint("min", Op.GE, 2, name="GreaterThanThreshold")
    with pytest.raises(AssertionError):
        constraint1.merge(constraint2)


def test_value_constraints_merge():
    constraint1 = ValueConstraint(Op.LT, 1)
    constraint2 = ValueConstraint(Op.LT, 1)
    merged = constraint1.merge(constraint2)
    assert merged.report() == ("value LT 1", 0, 0), "merging unlogged constraints should not change them from initiat state"


def test_value_constraints_merge_empty():
    constraint1 = ValueConstraint(Op.LT, 1)
    constraint2 = None
    merged = constraint1.merge(constraint2)
    assert merged == constraint1, "merging empty constraints should preserve left hand side"


def test_value_constraints_with_zero_as_value():
    c1 = ValueConstraint(Op.LT, 0)
    json_value = json.loads(message_to_json(c1.to_protobuf()))
    assert json_value["name"] == f"value {Op.Name(Op.LT)} 0"
    assert pytest.approx(json_value["value"], 0.01) == 0.0
    assert json_value["op"] == Op.Name(Op.LT)
    assert json_value["verbose"] is False


def test_value_constraints_raw_and_coerced_types_serialize_deserialize():
    pattern = r"\S+@\S+"
    c1 = ValueConstraint(Op.GE, 0)
    c2 = ValueConstraint(Op.MATCH, regex_pattern=pattern)
    constraints = ValueConstraints([c1, c2])
    constraints.update("abc")
    constraints.update_typed(1)

    constraints.from_protobuf(constraints.to_protobuf())
    msg_const = constraints.to_protobuf()
    json_val = json.loads(message_to_json(msg_const))

    first_val_constraint = json_val["constraints"][0]
    second_val_constraint = json_val["constraints"][1]

    assert first_val_constraint["name"] == f"value {Op.Name(Op.MATCH)} {pattern}"
    assert first_val_constraint["op"] == Op.Name(Op.MATCH)
    assert first_val_constraint["regexPattern"] == pattern
    assert first_val_constraint["verbose"] is False

    assert second_val_constraint["name"] == f"value {Op.Name(Op.GE)} 0"
    assert second_val_constraint["op"] == Op.Name(Op.GE)
    assert pytest.approx(second_val_constraint["value"], 0.01) == 0
    assert second_val_constraint["verbose"] is False


def test_value_constraints_raw_and_coerced_types_merge():
    pattern = r"\S+@\S+"
    c1 = ValueConstraint(Op.GE, 0)
    c2 = ValueConstraint(Op.MATCH, regex_pattern=pattern)
    constraints = ValueConstraints([c1, c2])
    c3 = ValueConstraint(Op.GE, 0)
    c4 = ValueConstraint(Op.MATCH, regex_pattern=pattern)
    constraints2 = ValueConstraints([c3, c4])

    merged = constraints.merge(constraints2)
    json_val = json.loads(message_to_json(merged.to_protobuf()))

    first_val_constraint = json_val["constraints"][0]
    second_val_constraint = json_val["constraints"][1]

    assert first_val_constraint["name"] == f"value {Op.Name(Op.MATCH)} {pattern}"
    assert first_val_constraint["op"] == Op.Name(Op.MATCH)
    assert first_val_constraint["regexPattern"] == pattern
    assert first_val_constraint["verbose"] is False

    assert second_val_constraint["name"] == f"value {Op.Name(Op.GE)} 0"
    assert second_val_constraint["op"] == Op.Name(Op.GE)
    assert pytest.approx(second_val_constraint["value"], 0.01) == 0
    assert second_val_constraint["verbose"] is False


def test_value_constraints_raw_and_coerced_types_report():
    pattern = r"\S+@\S+"
    c1 = ValueConstraint(Op.GE, 0)
    c2 = ValueConstraint(Op.MATCH, regex_pattern=pattern)
    constraints = ValueConstraints({c1.name: c1, c2.name: c2})
    report = constraints.report()

    assert report[0][0] == f"value {Op.Name(Op.MATCH)} {pattern}"
    assert report[0][1] == 0
    assert report[0][2] == 0

    assert report[1][0] == f"value {Op.Name(Op.GE)} 0"
    assert report[1][1] == 0
    assert report[1][2] == 0


def test_summary_between_serialization_deserialization():
    # constraints may have an optional name
    sum_constraint = SummaryConstraint("min", Op.BTWN, 0.1, 2.4)
    msg_sum_const = sum_constraint.to_protobuf()
    json_summary = json.loads(message_to_json(msg_sum_const))

    assert json_summary["name"] == "summary min BTWN 0.1 and 2.4"
    assert pytest.approx(json_summary["between"]["lowerValue"], 0.1) == 0.1
    assert pytest.approx(json_summary["between"]["upperValue"], 0.1) == 2.4
    assert json_summary["firstField"] == "min"
    assert json_summary["op"] == str(Op.Name(Op.BTWN))
    assert json_summary["verbose"] == False

    sum_deser_constraint = SummaryConstraint.from_protobuf(sum_constraint.to_protobuf())

    json_deser_summary = json.loads(message_to_json(sum_deser_constraint.to_protobuf()))

    assert json_summary["name"] == json_deser_summary["name"]
    assert pytest.approx(json_summary["between"]["lowerValue"], 0.001) == pytest.approx(json_deser_summary["between"]["lowerValue"], 0.001)
    assert pytest.approx(json_summary["between"]["upperValue"], 0.001) == pytest.approx(json_deser_summary["between"]["upperValue"], 0.001)
    assert json_summary["firstField"] == json_deser_summary["firstField"]
    assert json_summary["op"] == json_deser_summary["op"]
    assert json_summary["verbose"] == json_deser_summary["verbose"]


def test_summary_between_constraint_incompatible_parameters():
    with pytest.raises(TypeError):
        SummaryConstraint("min", Op.BTWN, 0.1, "stddev")

    with pytest.raises(ValueError):
        SummaryConstraint("min", Op.BTWN, 0.1, second_field="stddev")

    with pytest.raises(ValueError):
        SummaryConstraint("min", Op.BTWN, 0.1, 2.4, "stddev")

    with pytest.raises(ValueError):
        SummaryConstraint("min", Op.BTWN, 0.1, 2.4, third_field="stddev")

    with pytest.raises(TypeError):
        SummaryConstraint("stddev", Op.BTWN, second_field=2, third_field="max")

    with pytest.raises(TypeError):
        SummaryConstraint("stddev", Op.BTWN, 2, "max")


def _apply_between_summary_constraint_on_dataset(df_lending_club, local_config_path, between_constraint):
    min_gt_constraint = SummaryConstraint("min", Op.GT, value=100)
    max_le_constraint = SummaryConstraint("max", Op.LE, value=5)

    dc = DatasetConstraints(None, summary_constraints={"annual_inc": [between_constraint, max_le_constraint], "loan_amnt": [min_gt_constraint]})
    config = load_config(local_config_path)
    session = session_from_config(config)
    profile = session.log_dataframe(df_lending_club, "test.data", constraints=dc)
    session.close()
    report = profile.apply_summary_constraints()

    assert len(report) == 2

    # make sure it checked every value
    for each_feat in report:
        for each_constraint in each_feat[1]:
            assert each_constraint[1] == 1


def test_summary_between_constraints_values(df_lending_club, local_config_path):
    std_dev_between = SummaryConstraint("stddev", Op.BTWN, value=100, upper_value=200)
    _apply_between_summary_constraint_on_dataset(df_lending_club, local_config_path, std_dev_between)


def test_summary_between_constraints_fields(df_lending_club, local_config_path):
    std_dev_between = SummaryConstraint("stddev", Op.BTWN, second_field="mean", third_field="max")
    _apply_between_summary_constraint_on_dataset(df_lending_club, local_config_path, std_dev_between)


def test_summary_between_constraints_no_merge_different_values_fields():
    std_dev_between1 = SummaryConstraint("stddev", Op.BTWN, value=0.1, upper_value=200)
    std_dev_between2 = SummaryConstraint("stddev", Op.BTWN, value=0.2, upper_value=200)

    with pytest.raises(AssertionError):
        std_dev_between1.merge(std_dev_between2)

    std_dev_between1 = SummaryConstraint("stddev", Op.BTWN, value=0.1, upper_value=200)
    std_dev_between2 = SummaryConstraint("stddev", Op.BTWN, value=0.1, upper_value=300)

    with pytest.raises(AssertionError):
        std_dev_between1.merge(std_dev_between2)

    std_dev_between1 = SummaryConstraint("stddev", Op.BTWN, second_field="min", third_field="max")
    std_dev_between2 = SummaryConstraint("stddev", Op.BTWN, second_field="mean", third_field="max")

    with pytest.raises(AssertionError):
        std_dev_between1.merge(std_dev_between2)

    std_dev_between1 = SummaryConstraint("stddev", Op.BTWN, second_field="min", third_field="mean")
    std_dev_between2 = SummaryConstraint("stddev", Op.BTWN, second_field="min", third_field="max")

    with pytest.raises(AssertionError):
        std_dev_between1.merge(std_dev_between2)


def test_summary_between_constraints_no_merge_different_names():
    std_dev_between1 = SummaryConstraint("stddev", Op.BTWN, value=0.1, upper_value=200, name="std dev between 1")
    std_dev_between2 = SummaryConstraint("stddev", Op.BTWN, value=0.1, upper_value=200, name="std dev between 2")

    with pytest.raises(AssertionError):
        std_dev_between1.merge(std_dev_between2)


def test_summary_between_constraints_merge():
    std_dev_between1 = SummaryConstraint("stddev", Op.BTWN, value=0.1, upper_value=200)
    std_dev_between2 = SummaryConstraint("stddev", Op.BTWN, value=0.1, upper_value=200)

    merged = std_dev_between1.merge(std_dev_between2)

    pre_merge_json = json.loads(message_to_json(std_dev_between1.to_protobuf()))
    merge_json = json.loads(message_to_json(merged.to_protobuf()))

    assert pre_merge_json["name"] == merge_json["name"]
    assert pytest.approx(pre_merge_json["between"]["lowerValue"], 0.001) == pytest.approx(merge_json["between"]["lowerValue"], 0.001)
    assert pytest.approx(pre_merge_json["between"]["upperValue"], 0.001) == pytest.approx(merge_json["between"]["upperValue"], 0.001)
    assert pre_merge_json["firstField"] == merge_json["firstField"]
    assert pre_merge_json["op"] == merge_json["op"]
    assert pre_merge_json["verbose"] == merge_json["verbose"]


def test_stddev_between_constraint_value(df_lending_club, local_config_path):
    lower = 2.3
    upper = 5.4
    stddev_between_values = stddevBetweenConstraint(lower_value=lower, upper_value=upper)
    # check if all constraints are applied
    _apply_between_summary_constraint_on_dataset(df_lending_club, local_config_path, stddev_between_values)


def test_stddev_between_constraint_field(df_lending_club, local_config_path):
    lower = "min"
    upper = "max"
    stddev_between_fields = stddevBetweenConstraint(lower_field=lower, upper_field=upper)
    _apply_between_summary_constraint_on_dataset(df_lending_club, local_config_path, stddev_between_fields)


def test_stddev_between_constraint_invalid():
    with pytest.raises(ValueError):
        stddevBetweenConstraint(lower_value=2)
    with pytest.raises(ValueError):
        stddevBetweenConstraint(lower_field="min")
    with pytest.raises(TypeError):
        stddevBetweenConstraint(lower_value="2", upper_value=2)
    with pytest.raises(TypeError):
        stddevBetweenConstraint(lower_field="max", upper_field=2)


def test_mean_between_constraint_value(df_lending_club, local_config_path):
    lower = 2.3
    upper = 5.4
    stddev_between_values = meanBetweenConstraint(lower_value=lower, upper_value=upper)
    # check if all constraints are applied
    _apply_between_summary_constraint_on_dataset(df_lending_club, local_config_path, stddev_between_values)


def test_mean_between_constraint_field(df_lending_club, local_config_path):
    lower = "min"
    upper = "max"
    stddev_between_fields = meanBetweenConstraint(lower_field=lower, upper_field=upper)
    _apply_between_summary_constraint_on_dataset(df_lending_club, local_config_path, stddev_between_fields)


def test_mean_between_constraint_invalid():
    with pytest.raises(ValueError):
        meanBetweenConstraint(lower_value=2)
    with pytest.raises(ValueError):
        meanBetweenConstraint(lower_field="min")
    with pytest.raises(TypeError):
        meanBetweenConstraint(lower_value="2", upper_value=2)
    with pytest.raises(TypeError):
        meanBetweenConstraint(lower_field="max", upper_field=2)


def test_min_between_constraint_value(df_lending_club, local_config_path):
    lower = 2.3
    upper = 5.4
    stddev_between_values = minBetweenConstraint(lower_value=lower, upper_value=upper)
    # check if all constraints are applied
    _apply_between_summary_constraint_on_dataset(df_lending_club, local_config_path, stddev_between_values)


def test_min_between_constraint_field(df_lending_club, local_config_path):
    lower = "stddev"
    upper = "max"
    stddev_between_fields = minBetweenConstraint(lower_field=lower, upper_field=upper)
    _apply_between_summary_constraint_on_dataset(df_lending_club, local_config_path, stddev_between_fields)


def test_min_between_constraint_invalid():
    with pytest.raises(ValueError):
        minBetweenConstraint(lower_value=2)
    with pytest.raises(ValueError):
        minBetweenConstraint(lower_field="min")
    with pytest.raises(TypeError):
        minBetweenConstraint(lower_value="2", upper_value=2)
    with pytest.raises(TypeError):
        minBetweenConstraint(lower_field="max", upper_field=2)


def test_max_between_constraint_value(df_lending_club, local_config_path):
    lower = 2.3
    upper = 5.4
    stddev_between_values = maxBetweenConstraint(lower_value=lower, upper_value=upper)
    # check if all constraints are applied
    _apply_between_summary_constraint_on_dataset(df_lending_club, local_config_path, stddev_between_values)


def test_max_between_constraint_field(df_lending_club, local_config_path):
    lower = "stddev"
    upper = "mean"
    stddev_between_fields = maxBetweenConstraint(lower_field=lower, upper_field=upper)
    _apply_between_summary_constraint_on_dataset(df_lending_club, local_config_path, stddev_between_fields)


def test_max_between_constraint_invalid():
    with pytest.raises(ValueError):
        maxBetweenConstraint(lower_value=2)
    with pytest.raises(ValueError):
        maxBetweenConstraint(lower_field="min")
    with pytest.raises(TypeError):
        maxBetweenConstraint(lower_value="2", upper_value=2)
    with pytest.raises(TypeError):
        maxBetweenConstraint(lower_field="max", upper_field=2)


def _apply_summary_constraints_on_dataset(df_lending_club, local_config_path, summary_constraints):
    dc = DatasetConstraints(None, summary_constraints=summary_constraints)
    config = load_config(local_config_path)
    session = session_from_config(config)
    profile = session.log_dataframe(df_lending_club, "test.data", constraints=dc)
    session.close()
    report = profile.apply_summary_constraints()
    return report


def test_set_summary_constraints(df_lending_club, local_config_path):
    org_list = list(df_lending_club["annual_inc"])

    org_list2 = list(df_lending_club["annual_inc"])
    org_list2.extend([1, 4, 5555, "gfsdgs", 0.00333, 245.32])

    in_set = distinctValuesInSetConstraint(reference_set=org_list2, name="True")
    in_set2 = distinctValuesInSetConstraint(reference_set=org_list, name="True2")
    in_set3 = distinctValuesInSetConstraint(reference_set=org_list[:-1], name="False")

    eq_set = distinctValuesEqualSetConstraint(reference_set=org_list, name="True3")
    eq_set2 = distinctValuesEqualSetConstraint(reference_set=org_list2, name="False2")
    eq_set3 = distinctValuesEqualSetConstraint(reference_set=org_list[:-1], name="False3")

    contains_set = distinctValuesContainSetConstraint(reference_set=[org_list[2]], name="True4")
    contains_set2 = distinctValuesContainSetConstraint(reference_set=org_list, name="True5")
    contains_set3 = distinctValuesContainSetConstraint(reference_set=org_list[:-1], name="True6")
    contains_set4 = distinctValuesContainSetConstraint(reference_set=[str(org_list[2])], name="False4")
    contains_set5 = distinctValuesContainSetConstraint(reference_set=[2.3456], name="False5")
    contains_set6 = distinctValuesContainSetConstraint(reference_set=org_list2, name="False6")

    list(df_lending_club["annual_inc"])
    constraints = [in_set, in_set2, in_set3, eq_set, eq_set2, eq_set3, contains_set, contains_set2, contains_set3, contains_set4, contains_set5, contains_set6]
    report = _apply_summary_constraints_on_dataset(df_lending_club, local_config_path, {"annual_inc": constraints})
    for r in report[0][1]:
        if "True" in r[0]:
            assert r[2] == 0
        else:
            assert r[2] == 1


def test_set_summary_constraint_invalid_init():
    with pytest.raises(TypeError):
        SummaryConstraint("distinct_column_values", Op.CONTAIN_SET, reference_set=1)
    with pytest.raises(ValueError):
        SummaryConstraint("distinct_column_values", Op.CONTAIN_SET, 1)
    with pytest.raises(ValueError):
        SummaryConstraint("distinct_column_values", Op.CONTAIN_SET, second_field="aaa")
    with pytest.raises(ValueError):
        SummaryConstraint("distinct_column_values", Op.CONTAIN_SET, third_field="aaa")
    with pytest.raises(ValueError):
        SummaryConstraint("distinct_column_values", Op.CONTAIN_SET, upper_value=2)


def test_set_summary_no_merge_different_set():
    set_c_1 = SummaryConstraint("distinct_column_values", Op.CONTAIN_SET, reference_set=[1, 2, 3])
    set_c_2 = SummaryConstraint("distinct_column_values", Op.CONTAIN_SET, reference_set=[2, 3, 4, 5])
    with pytest.raises(AssertionError):
        set_c_1.merge(set_c_2)


def test_set_summary_merge():
    set_c_1 = SummaryConstraint("distinct_column_values", Op.CONTAIN_SET, reference_set=[1, 2, 3])
    set_c_2 = SummaryConstraint("distinct_column_values", Op.CONTAIN_SET, reference_set=[1, 2, 3])

    merged = set_c_1.merge(set_c_2)

    pre_merge_json = json.loads(message_to_json(set_c_1.to_protobuf()))
    merge_json = json.loads(message_to_json(merged.to_protobuf()))

    assert pre_merge_json["name"] == merge_json["name"]
    assert pre_merge_json["referenceSet"] == merge_json["referenceSet"]
    assert pre_merge_json["firstField"] == merge_json["firstField"]
    assert pre_merge_json["op"] == merge_json["op"]
    assert pre_merge_json["verbose"] == merge_json["verbose"]


def test_set_summary_serialization():
    set1 = SummaryConstraint("distinct_column_values", Op.CONTAIN_SET, reference_set=[1, 2, 3])
    set2 = SummaryConstraint.from_protobuf(set1.to_protobuf())

    set1_json = json.loads(message_to_json(set1.to_protobuf()))
    set2_json = json.loads(message_to_json(set2.to_protobuf()))

    assert set1_json["name"] == set2_json["name"]
    assert set1_json["referenceSet"] == set2_json["referenceSet"]
    assert set1_json["firstField"] == set2_json["firstField"]
    assert set1_json["op"] == set2_json["op"]
    assert set1_json["verbose"] == set2_json["verbose"]


def test_column_values_in_set_constraint(df_lending_club, local_config_path):
    cvisc = columnValuesInSetConstraint(value_set={2, 5, 8, 90671227})
    ltc = ValueConstraint(Op.LT, 1)
    dc = DatasetConstraints(None, value_constraints={"id": [cvisc, ltc]})
    config = load_config(local_config_path)
    session = session_from_config(config)
    profile = session.log_dataframe(df_lending_club, "test.data", constraints=dc)
    session.close()
    report = dc.report()

    # check if all of the rows have been reported
    assert report[0][1][0][1] == len(df_lending_club)
    # the number of fails should equal the number of rows - 1 since the column id only has the value 90671227 in set
    assert report[0][1][0][2] == len(df_lending_club) - 1


def test_merge_values_in_set_constraint_different_value_set():
    cvisc1 = columnValuesInSetConstraint(value_set={1, 2, 3})
    cvisc2 = columnValuesInSetConstraint(value_set={3, 4, 5})
    with pytest.raises(AssertionError):
        cvisc1.merge(cvisc2)


def test_merge_values_in_set_constraint_same_value_set():
    val_set = {"abc", "b", "c"}
    cvisc1 = columnValuesInSetConstraint(value_set=val_set)
    cvisc2 = columnValuesInSetConstraint(value_set=val_set)
    merged = cvisc1.merge(cvisc2)

    TEST_LOGGER.info(f"Serialize the merged columnValuesInSetConstraint:\n {merged.to_protobuf()}")

    json_value = json.loads(message_to_json(merged.to_protobuf()))

    assert json_value["name"] == f"value {Op.Name(Op.IN)} " + str(val_set)
    assert json_value["op"] == Op.Name(Op.IN)
    assert json_value["valueSet"][0] == list(val_set)


def test_serialization_deserialization_values_in_set_constraint():
    val_set = {"abc", 1, 2}
    cvisc = columnValuesInSetConstraint(value_set=val_set)

    cvisc.from_protobuf(cvisc.to_protobuf())
    json_value = json.loads(message_to_json(cvisc.to_protobuf()))

    TEST_LOGGER.info(f"Serialize columnValuesInSetConstraint from deserialized representation:\n {cvisc.to_protobuf()}")

    assert json_value["name"] == f"value {Op.Name(Op.IN)} " + str(val_set)
    assert json_value["op"] == Op.Name(Op.IN)
    assert json_value["valueSet"][0] == list(val_set)


def test_column_values_in_set_wrong_datatype():
    with pytest.raises(TypeError):
        cvisc = columnValuesInSetConstraint(value_set=1)


def _report_email_value_constraint_on_data_set(local_config_path, pattern=None):
    df = pd.DataFrame(
        [
            {"email": r"abc's@gmail.com"},  # valid
            {"email": r'"aVrrR Test \@"@gmail.com'},  # valid
            {"email": r"abc..q12@example.us"},  # invalid
            {"email": r'"sdsss\d"@gmail.com'},  # valid
            {"email": r"customer/department=shipping?@example-another.some-other.us"},  # valid
            {"email": r".should_fail@yahoo.com"},  # invalid
            {"email": r"some.@a.com"},  # invalid
            {"email": r"abs@yahoo."},  # invalid
        ]
    )

    email_constraint = containsEmailConstraint(regex_pattern=pattern)
    dc = DatasetConstraints(None, value_constraints={"email": [email_constraint]})
    config = load_config(local_config_path)
    session = session_from_config(config)
    profile = session.log_dataframe(df, "test.data", constraints=dc)
    session.close()
    report = dc.report()
    return report


def _apply_string_length_constraints(local_config_path, length_constraints):
    df = pd.DataFrame(
        [
            {"str1": "length7"},
            {"str1": "length_8"},
            {"str1": "length__9"},
            {"str1": "a       10"},
            {"str1": "11        b"},
            {"str1": '(*&^%^&*(24!@_+>:|}?><"\\'},
            {"str1": "1b34567"},
        ]
    )

    dc = DatasetConstraints(None, value_constraints={"str1": length_constraints})
    config = load_config(local_config_path)
    session = session_from_config(config)
    profile = session.log_dataframe(df, "test.data", constraints=dc)
    session.close()
    report = dc.report()

    return report


def test_string_length_constraints(local_config_path):
    length_constraint7 = stringLengthEqualConstraint(length=7)
    length_constraint24 = stringLengthEqualConstraint(length=24)
    length_constraint7to10 = stringLengthBetweenConstraint(lower_value=7, upper_value=10)
    length_constraints = [length_constraint7, length_constraint24, length_constraint7to10]

    report = _apply_string_length_constraints(local_config_path, length_constraints)

    # report[column_n][report_list][report][name total or failure]
    assert report[0][1][0][1] == 7 and report[0][1][0][2] == 5 and report[0][1][0][0] == rf"value {Op.Name(Op.MATCH)} ^.{{7}}$"
    assert report[0][1][1][1] == 7 and report[0][1][1][2] == 6 and report[0][1][1][0] == rf"value {Op.Name(Op.MATCH)} ^.{{24}}$"
    assert report[0][1][2][1] == 7 and report[0][1][2][2] == 2 and report[0][1][2][0] == rf"value {Op.Name(Op.MATCH)} ^.{{7,10}}$"


def test_email_constraint(local_config_path):
    report = _report_email_value_constraint_on_data_set(local_config_path)

    assert report[0][1][0][1] == 8
    assert report[0][1][0][2] == 4


def test_email_constraint_supply_regex_pattern(local_config_path):
    report = _report_email_value_constraint_on_data_set(local_config_path, r"\S+@\S+")
    assert report[0][1][0][0] == rf"value {Op.Name(Op.MATCH)} \S+@\S+"
    assert report[0][1][0][1] == 8
    assert report[0][1][0][2] == 1


def test_email_constraint_merge_valid():
    ec1 = containsEmailConstraint(regex_pattern=r"\S+@\S+", verbose=True)
    ec2 = containsEmailConstraint(regex_pattern=r"\S+@\S+")
    merged = ec1.merge(ec2)
    json_value = json.loads(message_to_json(merged.to_protobuf()))

    assert json_value["name"] == rf"value {Op.Name(Op.MATCH)} \S+@\S+"
    assert json_value["op"] == Op.Name(Op.MATCH)
    assert json_value["regexPattern"] == r"\S+@\S+"
    assert json_value["verbose"] is True


def test_email_constraint_merge_invalid():
    ec1 = containsEmailConstraint(regex_pattern=r"\S+@\S+", verbose=True)
    ec2 = containsEmailConstraint(regex_pattern=r"\W+@\W+")
    with pytest.raises(AssertionError):
        ec1.merge(ec2)


def _report_credit_card_value_constraint_on_data_set(local_config_path, regex_pattern=None):
    df = pd.DataFrame(
        [
            {"credit_card": "3714-496353-98431"},  # amex
            {"credit_card": "3787 344936 71000"},  # amex
            {"credit_card": "3056 930902 5904"},  # diners club
            {"credit_card": "3065 133242 2899"},  # invalid
            {"credit_card": "3852-000002-3237"},  # diners club
            {"credit_card": "6011 1111 1111 1117"},  # discover
            {"credit_card": "6011-0009-9013-9424"},  # discover
            {"credit_card": "3530 1113 3330 0000"},  # jcb
            {"credit_card": "3566-0020-2036-0505"},  # jcb
            {"credit_card": "5555 5555 5555 4444"},  # master card
            {"credit_card": "5105 1051 0510 5100"},  # master card
            {"credit_card": "4111 1111 1111 1111"},  # visa
            {"credit_card": "4012 8888 8888 1881"},  # visa
            {"credit_card": "4222-2222-2222-2222"},  # visa
            {"credit_card": "1111-1111-1111-1111"},  # invalid
            {"credit_card": "a4111 1111 1111 1111b"},  # invalid
            {"credit_card": "4111111111111111"},  # visa
            {"credit_card": 12345},  # invalid
            {"credit_card": "absfcvs"},  # invalid
        ]
    )

    credit_card_constraint = containsCreditCardConstraint(regex_pattern=regex_pattern)
    dc = DatasetConstraints(None, value_constraints={"credit_card": [credit_card_constraint]})
    config = load_config(local_config_path)
    session = session_from_config(config)
    profile = session.log_dataframe(df, "test.data", constraints=dc)
    session.close()
    return dc.report()


def test_credit_card_constraint(local_config_path):
    report = _report_credit_card_value_constraint_on_data_set(local_config_path)
    assert report[0][1][0][1] == 19
    assert report[0][1][0][2] == 5


def test_credit_card_constraint_supply_regex_pattern(local_config_path):
    report = _report_credit_card_value_constraint_on_data_set(local_config_path, r"^(?:[0-9]{4}[\s-]?){3,4}$")
    assert report[0][1][0][0] == rf"value {Op.Name(Op.MATCH)} " + r"^(?:[0-9]{4}[\s-]?){3,4}$"
    assert report[0][1][0][1] == 19
    assert report[0][1][0][2] == 8


def test_credit_card_constraint_merge_valid():
    pattern = r"[0-9]{13,16}"
    ccc1 = containsCreditCardConstraint(regex_pattern=pattern, verbose=True)
    ccc2 = containsCreditCardConstraint(regex_pattern=pattern)
    merged = ccc1.merge(ccc2)
    json_value = json.loads(message_to_json(merged.to_protobuf()))

    assert json_value["name"] == f"value {Op.Name(Op.MATCH)} " + pattern
    assert json_value["op"] == Op.Name(Op.MATCH)
    assert json_value["regexPattern"] == pattern
    assert json_value["verbose"] is True


def test_credit_card_constraint_merge_invalid():
    ccc1 = containsCreditCardConstraint()
    ccc2 = containsCreditCardConstraint(regex_pattern=r"[0-9]{13,16}", verbose=False)
    with pytest.raises(AssertionError):
        ccc1.merge(ccc2)


def test_credit_card_invalid_pattern():
    with pytest.raises(TypeError):
        containsCreditCardConstraint(123)


def _apply_apply_func_constraints(local_config_path, apply_func_constraints):
    df = pd.DataFrame(
        [
            {"str1": "1990-12-1"},  # dateutil valid; strftime valid
            {"str1": "1990/12/1"},
            {"str1": "2005/3"},
            {"str1": "2005.3.5"},
            {"str1": "Jan 19, 1990"},
            {"str1": "today is 2019-03-27"},  # dateutil invalid
            {"str1": "Monday at 12:01am"},
            {"str1": "xyz_not_a_date"},  # dateutil invalid
            {"str1": "yesterday"},  # dateutil invalid
            {"str1": {"name": "s", "w2w2": "dgsg", "years": 232, "abc": 1}},  # schema valid
            {"str1": {"name": "s", "w2w2": 12.38, "years": 232, "abc": 1}},  # schema valid
            {"str1": {"name": "s", "years": 232, "abc": 1}},  # schema valid
            {"str1": {"name": "s", "abc": 1}},  # schema valid
            {"str1": {"name": "s", "w2w2": "dgsg", "years": 232}},  # schema invalid
            {"str1": {"name": "s", "w2w2": "dgsg", "years": "232", "abc": 1}},  # schema invalid
            {"str1": {"name": 14, "w2w2": "dgsg", "years": "232", "abc": 1}},  # schema invalid
            {"str1": {"name": "14", "w2w2": "dgsg", "years": 232.44, "abc": 1}},  # schema invalid
            {"str1": {"w2w2": "dgsg", "years": 232, "abc": 1}},  # schema invalid
            {"str1": {"years": 232}},  # schema invalid
            {"str1": json.dumps({"name": "s", "w2w2": "dgsg", "years": 232, "abc": 1})},  # json valid, schema valid
            {"str1": json.dumps({"name": "s", "w2w2": 12.38, "years": 232, "abc": 1})},  # json valid, schema valid
            {"str1": json.dumps({"name": "s", "years": 232, "abc": 1})},  # json valid, schema valid
            {"str1": json.dumps({"name": "s", "abc": 1})},  # json valid, schema valid
            {"str1": json.dumps({"name": "s", "w2w2": "dgsg", "years": "232", "abc": 1})},  # json valid
            {"str1": "random str : fail everything"},
            {"str1": "2003-12-23"},  # strftime valid
            {"str1": "2010-10-18"},  # strftime valid
            {"str1": "2003-15-23"},  # strftime invalid
            {"str1": "2003-12-32"},  # strftime invalid
            {"str1": "10-12-32"},  # strftime invalid, dateutil valid
        ]
    )

    dc = DatasetConstraints(None, value_constraints={"str1": apply_func_constraints})
    config = load_config(local_config_path)
    session = session_from_config(config)
    profile = session.log_dataframe(df, "test.data", constraints=dc)
    session.close()
    report = dc.report()

    return report


def test_apply_func_value_constraints(local_config_path):
    dateutil_parseable = dateUtilParseableConstraint()
    json_parseable = jsonParseableConstraint()

    json_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "years": {"type": "integer"},
        },
        "required": ["name", "abc"],
    }
    matches_json_schema = matchesJsonSchemaConstraint(json_schema=json_schema)

    is_strftime = strftimeFormatConstraint(format="%Y-%m-%d")

    apply_constraints = [dateutil_parseable, json_parseable, matches_json_schema, is_strftime]

    report = _apply_apply_func_constraints(local_config_path, apply_constraints)

    # report[column_n][report_list][report][name total or failure]
    assert report[0][1][0][1] == 30 and report[0][1][0][2] == 21 and report[0][1][0][0] == f"value {Op.Name(Op.APPLY_FUNC)} {_try_parse_dateutil.__name__}"
    assert report[0][1][1][1] == 30 and report[0][1][1][2] == 25 and report[0][1][1][0] == f"value {Op.Name(Op.APPLY_FUNC)} {_try_parse_json.__name__}"
    assert report[0][1][2][1] == 30 and report[0][1][2][2] == 22 and report[0][1][2][0] == f"value {Op.Name(Op.APPLY_FUNC)} {_matches_json_schema.__name__}"
    assert (
        report[0][1][3][1] == 30 and report[0][1][3][2] == 27 and report[0][1][3][0] == f"value {Op.Name(Op.APPLY_FUNC)} {_try_parse_strftime_format.__name__}"
    )


def test_apply_func_merge():
    apply1 = dateUtilParseableConstraint()
    apply2 = ValueConstraint(Op.APPLY_FUNC, apply_function=lambda x: x)

    with pytest.raises(AssertionError):
        apply1.merge(apply2)

    apply3 = dateUtilParseableConstraint()

    merged = apply1.merge(apply3)

    pre_merge_json = json.loads(message_to_json(apply1.to_protobuf()))
    merge_json = json.loads(message_to_json(merged.to_protobuf()))

    assert pre_merge_json["name"] == merge_json["name"]
    assert pre_merge_json["function"] == merge_json["function"]
    assert pre_merge_json["op"] == merge_json["op"]
    assert pre_merge_json["verbose"] == merge_json["verbose"]


def test_apply_func_serialization():
    apply1 = dateUtilParseableConstraint()

    apply2 = ValueConstraint.from_protobuf(apply1.to_protobuf())

    apply1_json = json.loads(message_to_json(apply1.to_protobuf()))
    apply2_json = json.loads(message_to_json(apply2.to_protobuf()))

    apply1.merge(apply2)
    apply2.merge(apply1)

    assert apply1_json["name"] == apply2_json["name"]
    assert apply1_json["function"] == apply2_json["function"]
    assert apply1_json["op"] == apply2_json["op"]
    assert apply1_json["verbose"] == apply2_json["verbose"]

    json_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "years": {"type": "integer"},
        },
        "required": ["name", "abc"],
    }
    apply1 = matchesJsonSchemaConstraint(json_schema)
    apply2 = ValueConstraint.from_protobuf(apply1.to_protobuf())

    apply1_json = json.loads(message_to_json(apply1.to_protobuf()))
    apply2_json = json.loads(message_to_json(apply2.to_protobuf()))

    assert apply1_json["name"] == apply2_json["name"]
    assert apply1_json["function"] == apply2_json["function"]
    assert apply1_json["op"] == apply2_json["op"]
    assert apply1_json["verbose"] == apply2_json["verbose"]


def _report_ssn_value_constraint_on_data_set(local_config_path, regex_pattern=None):
    df = pd.DataFrame(
        [
            {"ssn": "123-01-2335"},  # valid
            {"ssn": "039780012"},  # valid
            {"ssn": "000231324"},  # invalid
            {"ssn": "666781132"},  # invalid
            {"ssn": "926-89-1234"},  # invalid
            {"ssn": "001-01-0001"},  # valid
            {"ssn": "122 23 0001"},  # valid
            {"ssn": "1234-12-123"},  # invalid
        ]
    )

    ssn_constraint = containsSSNConstraint(regex_pattern=regex_pattern)
    dc = DatasetConstraints(None, value_constraints={"ssn": [ssn_constraint]})
    config = load_config(local_config_path)
    session = session_from_config(config)
    profile = session.log_dataframe(df, "test.data", constraints=dc)
    session.close()
    return dc.report()


def test_contains_ssn_constraint(local_config_path):
    report = _report_ssn_value_constraint_on_data_set(local_config_path)
    assert report[0][1][0][1] == 8
    assert report[0][1][0][2] == 4


def test_ssn_constraint_supply_regex_pattern(local_config_path):
    pattern = r"^[0-9]{3}-[0-9]{2}-[0-9]{4}$"
    report = _report_ssn_value_constraint_on_data_set(local_config_path, pattern)
    assert report[0][1][0][0] == rf"value {Op.Name(Op.MATCH)} " + pattern
    assert report[0][1][0][1] == 8
    assert report[0][1][0][2] == 5


def test_ssn_constraint_merge_valid():
    pattern = r"^[0-9]{3}-[0-9]{2}-[0-9]{4}$"
    ccc1 = containsSSNConstraint(regex_pattern=pattern, verbose=True)
    ccc2 = containsSSNConstraint(regex_pattern=pattern)
    merged = ccc1.merge(ccc2)
    json_value = json.loads(message_to_json(merged.to_protobuf()))

    assert json_value["name"] == f"value {Op.Name(Op.MATCH)} " + pattern
    assert json_value["op"] == Op.Name(Op.MATCH)
    assert json_value["regexPattern"] == pattern
    assert json_value["verbose"] is True


def test_ssn_constraint_merge_invalid():
    ccc1 = containsSSNConstraint()
    ccc2 = containsSSNConstraint(regex_pattern=r"[0-9]{13,16}", verbose=False)
    with pytest.raises(AssertionError):
        ccc1.merge(ccc2)


def test_ssn_invalid_pattern():
    with pytest.raises(TypeError):
        containsSSNConstraint(123)


def _report_url_value_constraint_on_data_set(local_config_path, regex_pattern=None):
    df = pd.DataFrame(
        [
            {"url": "http://www.example.com"},  # valid
            {"url": "abc.test.com"},  # valid (without protocol)
            {"url": "abc.w23w.asb#abc?a=2"},  # valid (without protocol)
            {"url": "https://ab.abc.bc"},  # valid
            {"url": "a.b.c"},  # valid
            {"url": "abcd"},  # invalid
            {"url": "123.w23.235"},  # valid
            {"url": "asf://saf.we.12"},  # invalid
            {"url": "12345"},  # invalid
            {"url": "1.2"},  # invalid
        ]
    )

    url_constraint = containsURLConstraint(regex_pattern=regex_pattern)
    dc = DatasetConstraints(None, value_constraints={"url": [url_constraint]})
    config = load_config(local_config_path)
    session = session_from_config(config)
    profile = session.log_dataframe(df, "test.data", constraints=dc)
    session.close()
    return dc.report()


def test_contains_url_constraint(local_config_path):
    report = _report_url_value_constraint_on_data_set(local_config_path)
    assert report[0][1][0][1] == 10
    assert report[0][1][0][2] == 4


def test_url_constraint_supply_regex_pattern(local_config_path):
    pattern = r"^http(s)?:\/\/(www\.)?.+\..+$"
    report = _report_url_value_constraint_on_data_set(local_config_path, pattern)
    assert report[0][1][0][0] == rf"value {Op.Name(Op.MATCH)} " + pattern
    assert report[0][1][0][1] == 10
    assert report[0][1][0][2] == 8


def test_url_constraint_merge_valid():
    pattern = r"^http(s)?://(www)?\..*\..*$"
    ccc1 = containsURLConstraint(regex_pattern=pattern, verbose=False)
    ccc2 = containsURLConstraint(regex_pattern=pattern)
    merged = ccc1.merge(ccc2)
    json_value = json.loads(message_to_json(merged.to_protobuf()))

    assert json_value["name"] == f"value {Op.Name(Op.MATCH)} " + pattern
    assert json_value["op"] == Op.Name(Op.MATCH)
    assert json_value["regexPattern"] == pattern
    assert json_value["verbose"] is False


def test_url_constraint_merge_invalid():
    ccc1 = containsURLConstraint()
    ccc2 = containsURLConstraint(regex_pattern=r"http(s)?://.+", verbose=False)
    with pytest.raises(AssertionError):
        ccc1.merge(ccc2)


def test_url_invalid_pattern():
    with pytest.raises(TypeError):
        containsURLConstraint(2124)


def test_summary_constraint_quantile_invalid():
    with pytest.raises(ValueError):
        SummaryConstraint("stddev", op=Op.LT, value=2, quantile_value=0.2)
    with pytest.raises(ValueError):
        SummaryConstraint("quantile", op=Op.GT, value=2)


def test_quantile_between_constraint_apply(local_config_path, df_lending_club):
    qc = quantileBetweenConstraint(quantile_value=0.25, lower_value=13308, upper_value=241001)
    summary_constraint = {"annual_inc": [qc]}
    report = _apply_summary_constraints_on_dataset(df_lending_club, local_config_path, summary_constraint)

    assert report[0][1][0][0] == f"summary quantile {0.25} {Op.Name(Op.BTWN)} 13308 and 241001"
    assert report[0][1][0][1] == 1
    assert report[0][1][0][2] == 0


def test_merge_quantile_between_constraint_different_values():
    qc1 = quantileBetweenConstraint(quantile_value=0.25, lower_value=0, upper_value=2)
    qc2 = quantileBetweenConstraint(quantile_value=0.25, lower_value=1, upper_value=2)
    with pytest.raises(AssertionError):
        qc1.merge(qc2)


def test_merge_quantile_between_constraint_same_values():
    qc1 = quantileBetweenConstraint(quantile_value=0.5, lower_value=0, upper_value=5)
    qc2 = quantileBetweenConstraint(quantile_value=0.5, lower_value=0, upper_value=5)
    merged = qc1.merge(qc2)
    message = json.loads(message_to_json(merged.to_protobuf()))

    assert message["name"] == f"summary quantile 0.5 {Op.Name(Op.BTWN)} 0 and 5"
    assert message["firstField"] == "quantile"
    assert message["op"] == Op.Name(Op.BTWN)
    assert pytest.approx(message["between"]["lowerValue"], 0.001) == 0.0
    assert pytest.approx(message["between"]["upperValue"], 0.001) == 5.0
    assert message["verbose"] is False


def test_serialization_deserialization_quantile_between_constraint():
    qc1 = quantileBetweenConstraint(quantile_value=0.5, lower_value=1.24, upper_value=6.63, verbose=True)

    qc1.from_protobuf(qc1.to_protobuf())
    json_value = json.loads(message_to_json(qc1.to_protobuf()))

    assert json_value["name"] == f"summary quantile 0.5 {Op.Name(Op.BTWN)} 1.24 and 6.63"
    assert json_value["firstField"] == "quantile"
    assert json_value["op"] == Op.Name(Op.BTWN)
    assert pytest.approx(json_value["between"]["lowerValue"], 0.001) == 1.24
    assert pytest.approx(json_value["between"]["upperValue"], 0.001) == 6.63
    assert json_value["verbose"] is True


def test_quantile_between_wrong_datatype():
    with pytest.raises(TypeError):
        quantileBetweenConstraint(quantile_value=[0.5], lower_value=1.24, upper_value=6.63, verbose=True)
    with pytest.raises(TypeError):
        quantileBetweenConstraint(quantile_value=0.5, lower_value="1.24", upper_value=6.63, verbose=True)
    with pytest.raises(TypeError):
        quantileBetweenConstraint(quantile_value=0.3, lower_value=1.24, upper_value=[6.63], verbose=True)
    with pytest.raises(ValueError):
        quantileBetweenConstraint(quantile_value=0.3, lower_value=2.3, upper_value=1.5, verbose=True)


def test_unique_value_count_between_constraint_apply(local_config_path, df_lending_club):
    uc = columnUniqueValueCountBetweenConstraint(lower_value=5, upper_value=50)
    summary_constraint = {"annual_inc": [uc]}
    report = _apply_summary_constraints_on_dataset(df_lending_club, local_config_path, summary_constraint)
    assert report[0][1][0][0] == f"summary unique_count {Op.Name(Op.BTWN)} 5 and 50"
    assert report[0][1][0][1] == 1
    assert report[0][1][0][2] == 0


def test_merge_unique_value_count_between_constraint_different_values():
    u1 = columnUniqueValueCountBetweenConstraint(lower_value=0, upper_value=2)
    u2 = columnUniqueValueCountBetweenConstraint(lower_value=1, upper_value=2)
    with pytest.raises(AssertionError):
        u1.merge(u2)


def test_merge_unique_value_count_between_constraint_same_values():
    u1 = columnUniqueValueCountBetweenConstraint(lower_value=0, upper_value=5)
    u2 = columnUniqueValueCountBetweenConstraint(lower_value=0, upper_value=5)
    merged = u1.merge(u2)
    message = json.loads(message_to_json(merged.to_protobuf()))

    assert message["name"] == f"summary unique_count {Op.Name(Op.BTWN)} 0 and 5"
    assert message["firstField"] == "unique_count"
    assert message["op"] == Op.Name(Op.BTWN)
    assert pytest.approx(message["between"]["lowerValue"], 0.001) == 0.0
    assert pytest.approx(message["between"]["upperValue"], 0.001) == 5.0
    assert message["verbose"] is False


def test_serialization_deserialization_unique_value_count_between_constraint():
    u1 = columnUniqueValueCountBetweenConstraint(lower_value=15, upper_value=50, verbose=True)

    u1.from_protobuf(u1.to_protobuf())
    json_value = json.loads(message_to_json(u1.to_protobuf()))

    assert json_value["name"] == f"summary unique_count {Op.Name(Op.BTWN)} 15 and 50"
    assert json_value["firstField"] == "unique_count"
    assert json_value["op"] == Op.Name(Op.BTWN)
    assert pytest.approx(json_value["between"]["lowerValue"], 0.001) == 15
    assert pytest.approx(json_value["between"]["upperValue"], 0.001) == 50
    assert json_value["verbose"] is True


def test_unique_count_between_constraint_wrong_datatype():
    with pytest.raises(ValueError):
        columnUniqueValueCountBetweenConstraint(lower_value="0", upper_value=1, verbose=True)
    with pytest.raises(ValueError):
        columnUniqueValueCountBetweenConstraint(lower_value=5, upper_value=6.63, verbose=True)
    with pytest.raises(ValueError):
        columnUniqueValueCountBetweenConstraint(lower_value=1, upper_value=0)


def test_unique_value_proportion_between_constraint_apply(local_config_path, df_lending_club):
    uc = columnUniqueValueProportionBetweenConstraint(lower_fraction=0.6, upper_fraction=0.9)
    summary_constraint = {"annual_inc": [uc]}
    report = _apply_summary_constraints_on_dataset(df_lending_club, local_config_path, summary_constraint)
    assert report[0][1][0][0] == f"summary unique_proportion {Op.Name(Op.BTWN)} 0.6 and 0.9"
    assert report[0][1][0][1] == 1
    assert report[0][1][0][2] == 0


def test_merge_unique_value_proportion_between_constraint_different_values():
    u1 = columnUniqueValueProportionBetweenConstraint(lower_fraction=0.2, upper_fraction=0.3)
    u2 = columnUniqueValueProportionBetweenConstraint(lower_fraction=0.1, upper_fraction=0.3)
    with pytest.raises(AssertionError):
        u1.merge(u2)


def test_merge_unique_value_proportion_between_constraint_same_values():
    u1 = columnUniqueValueProportionBetweenConstraint(lower_fraction=0.1, upper_fraction=0.5)
    u2 = columnUniqueValueProportionBetweenConstraint(lower_fraction=0.1, upper_fraction=0.5)
    merged = u1.merge(u2)
    message = json.loads(message_to_json(merged.to_protobuf()))

    assert message["name"] == f"summary unique_proportion {Op.Name(Op.BTWN)} 0.1 and 0.5"
    assert message["firstField"] == "unique_proportion"
    assert message["op"] == Op.Name(Op.BTWN)
    assert pytest.approx(message["between"]["lowerValue"], 0.001) == 0.1
    assert pytest.approx(message["between"]["upperValue"], 0.001) == 0.5
    assert message["verbose"] is False


def test_serialization_deserialization_unique_value_proportion_between_constraint():
    u1 = columnUniqueValueProportionBetweenConstraint(lower_fraction=0.6, upper_fraction=0.7, verbose=True)

    u1.from_protobuf(u1.to_protobuf())
    json_value = json.loads(message_to_json(u1.to_protobuf()))

    assert json_value["name"] == f"summary unique_proportion {Op.Name(Op.BTWN)} 0.6 and 0.7"
    assert json_value["firstField"] == "unique_proportion"
    assert json_value["op"] == Op.Name(Op.BTWN)
    assert pytest.approx(json_value["between"]["lowerValue"], 0.001) == 0.6
    assert pytest.approx(json_value["between"]["upperValue"], 0.001) == 0.7
    assert json_value["verbose"] is True


def test_unique_proportion_between_constraint_wrong_datatype():
    with pytest.raises(ValueError):
        columnUniqueValueProportionBetweenConstraint(lower_fraction=0, upper_fraction=1.0, verbose=True)
    with pytest.raises(ValueError):
        columnUniqueValueProportionBetweenConstraint(lower_fraction=0.2, upper_fraction=0.1, verbose=True)
    with pytest.raises(ValueError):
        columnUniqueValueProportionBetweenConstraint(lower_fraction=0.4, upper_fraction=2)


def test_table_shape_constraints(df_lending_club, local_config_path):

    rows = numberOfRowsConstraint(n_rows=10)
    rows_2 = numberOfRowsConstraint(n_rows=len(df_lending_club.index))

    column_exist = columnExistsConstraint("no_WAY")
    column_exist2 = columnExistsConstraint("loan_amnt")

    set1 = set(["col1", "col2"])
    set2 = set(df_lending_club.columns)
    columns_match = columnsMatchSetConstraint(set1)
    columns_match2 = columnsMatchSetConstraint(set2)

    table_shape_constraints = [rows, rows_2, column_exist, column_exist2, columns_match, columns_match2]

    dc = DatasetConstraints(None, table_shape_constraints=table_shape_constraints)

    config = load_config(local_config_path)
    session = session_from_config(config)

    report = None
    logger = session.logger(dataset_name="test.data", constraints=dc)
    logger.log_dataframe(df_lending_club)
    report = logger.profile.apply_table_shape_constraints()

    assert len(report) == 6
    # table shape {self.value} {Op.Name(self.op)} {self.first_field}
    assert report[0][0] == f"table total_row_number {Op.Name(Op.EQ)} 10"
    assert report[0][1] == 1
    assert report[0][2] == 1

    assert report[1][0] == f"table total_row_number {Op.Name(Op.EQ)} {len(df_lending_club.index)}"
    assert report[1][1] == 1
    assert report[1][2] == 0

    assert report[2][0] == f"table columns {Op.Name(Op.CONTAIN)} no_WAY"
    assert report[2][1] == 1
    assert report[2][2] == 1

    assert report[3][0] == f"table columns {Op.Name(Op.CONTAIN)} loan_amnt"
    assert report[3][1] == 1
    assert report[3][2] == 0

    assert report[4][0] == f"table columns {Op.Name(Op.EQ)} {set1}"
    assert report[4][1] == 1
    assert report[4][2] == 1

    reference_set_str = ""
    if len(set2) > MAX_SET_DISPLAY_MESSAGE_LENGTH:
        tmp_set = set(list(set2)[:MAX_SET_DISPLAY_MESSAGE_LENGTH])
        reference_set_str = f"{str(tmp_set)[:-1]}, ...}}"
    else:
        reference_set_str = str(set2)
    assert report[5][0] == f"table columns {Op.Name(Op.EQ)} {reference_set_str}"
    assert report[5][1] == 1
    assert report[5][2] == 0

    logger.log({"no_WAY": 1})  # logging a new non existent column

    report2 = logger.profile.apply_table_shape_constraints()

    assert report2[1][0] == f"table total_row_number {Op.Name(Op.EQ)} {len(df_lending_club.index)}"
    assert report2[1][1] == 2
    assert report2[1][2] == 0
    # applying the table shape constraints the second time, after adding a new column the constraint passes since
    # the total row number stays the same

    assert report2[2][0] == f"table columns {Op.Name(Op.CONTAIN)} no_WAY"
    assert report2[2][1] == 2
    assert report2[2][2] == 1
    # after logging the new column 'no_WAY', this constraint DOES NOT fail the second time because the column 'no_way'
    # is now present

    assert report2[5][0] == f"table columns {Op.Name(Op.EQ)} {reference_set_str}"
    assert report2[5][1] == 2
    assert report2[5][2] == 1
    # after logging the new column 'no_WAY', this constraint fails the second time because the reference set
    # does not contain the new column 'no_WAY'

    set3 = set(set2)
    set3.add("no_WAY")

    columns_match3 = columnsMatchSetConstraint(set3)

    report3 = logger.profile.apply_table_shape_constraints(SummaryConstraints([columns_match3]))

    reference_set_str2 = ""
    if len(set3) > MAX_SET_DISPLAY_MESSAGE_LENGTH:
        tmp_set = set(list(set3)[:MAX_SET_DISPLAY_MESSAGE_LENGTH])
        reference_set_str2 = f"{str(tmp_set)[:-1]}, ...}}"
    else:
        reference_set_str2 = str(set3)

    assert report3[0][0] == f"table columns {Op.Name(Op.EQ)} {reference_set_str2}"
    assert report3[0][1] == 1
    assert report3[0][2] == 0
    # adding the new column to 'set3' and creating a constraint with it, now it doesn't fail

    log_dict = dict()
    # logging a new value for every column (one more row)
    for column in df_lending_club.columns:
        value = df_lending_club[column][10]  # sample from the column
        log_dict[column] = value
    logger.log(log_dict)

    report4 = logger.profile.apply_table_shape_constraints()

    assert report4[1][0] == f"table total_row_number {Op.Name(Op.EQ)} {len(df_lending_club.index)}"
    assert report4[1][1] == 3
    assert report4[1][2] == 1
    # applying the table shape constraints the third time, and only this time it fails as the new row was logged

    rows_3 = numberOfRowsConstraint(n_rows=len(df_lending_club.index) + 1)  # 1 new row

    report5 = logger.profile.apply_table_shape_constraints(SummaryConstraints([rows_3]))

    assert report5[0][0] == f"table total_row_number {Op.Name(Op.EQ)} {len(df_lending_club.index)+1}"
    assert report5[0][1] == 1
    assert report5[0][2] == 0
    # the new numberOfRowsConstraint with n_rows=previous_n_rows+1 passes

    profile = logger.close()  # closing the logger and getting the DatasetProfile

    assert profile.total_row_number == rows_3.value


def test_table_shape_constraint_invalid_init():
    with pytest.raises(TypeError):
        SummaryConstraint("columns", Op.EQ, reference_set=1)
    with pytest.raises(ValueError):
        SummaryConstraint("columns", Op.CONTAIN, reference_set=1)
    with pytest.raises(ValueError):
        SummaryConstraint("total_row_number", Op.EQ, reference_set=1)
    with pytest.raises(ValueError):
        SummaryConstraint("total_row_number", Op.CONTAIN, reference_set=1)

    with pytest.raises(ValueError):
        SummaryConstraint("columns", Op.CONTAIN, 1)
    with pytest.raises(ValueError):
        SummaryConstraint("columns", Op.CONTAIN, second_field="aaa")
    with pytest.raises(ValueError):
        SummaryConstraint("columns", Op.EQ, 1)
    with pytest.raises(ValueError):
        SummaryConstraint("columns", Op.EQ, second_field="aaa")

    with pytest.raises(ValueError):
        SummaryConstraint("total_row_number", Op.CONTAIN, 1)
    with pytest.raises(ValueError):
        SummaryConstraint("total_row_number", Op.CONTAIN, second_field="aaa")
    with pytest.raises(ValueError):
        SummaryConstraint("total_row_number", Op.EQ, second_field="aaa")

    with pytest.raises(ValueError):
        SummaryConstraint("columns", Op.CONTAIN, third_field="aaa")
    with pytest.raises(ValueError):
        SummaryConstraint("columns", Op.EQ, third_field="aaa")
    with pytest.raises(ValueError):
        SummaryConstraint("total_row_number", Op.CONTAIN, third_field="aaa")
    with pytest.raises(ValueError):
        SummaryConstraint("total_row_number", Op.EQ, third_field="aaa")

    with pytest.raises(ValueError):
        SummaryConstraint("columns", Op.CONTAIN, upper_value=2)
    with pytest.raises(ValueError):
        SummaryConstraint("columns", Op.EQ, upper_value=2)
    with pytest.raises(ValueError):
        SummaryConstraint("total_row_number", Op.CONTAIN, upper_value=2)
    with pytest.raises(ValueError):
        SummaryConstraint("total_row_number", Op.EQ, upper_value=2)


def test_table_shape_no_merge_different_set():

    set_c_1 = SummaryConstraint("columns", Op.EQ, reference_set=[1, 2, 3])
    set_c_2 = SummaryConstraint("columns", Op.EQ, reference_set=[2, 3, 4, 5])
    with pytest.raises(AssertionError):
        set_c_1.merge(set_c_2)


def test_table_shape_merge():
    set_c_1 = SummaryConstraint("columns", Op.EQ, reference_set=[1, 2, 3])
    set_c_2 = columnsMatchSetConstraint(reference_set=[1, 2, 3])

    merged = set_c_1.merge(set_c_2)

    pre_merge_json = json.loads(message_to_json(set_c_1.to_protobuf()))
    merge_json = json.loads(message_to_json(merged.to_protobuf()))

    assert pre_merge_json["name"] == merge_json["name"]
    assert pre_merge_json["referenceSet"] == merge_json["referenceSet"]
    assert pre_merge_json["firstField"] == merge_json["firstField"]
    assert pre_merge_json["op"] == merge_json["op"]
    assert pre_merge_json["verbose"] == merge_json["verbose"]

    set_c_1 = SummaryConstraint("columns", Op.CONTAIN, "c1")
    set_c_2 = columnExistsConstraint(column="c1")

    merged = set_c_1.merge(set_c_2)

    pre_merge_json = json.loads(message_to_json(set_c_1.to_protobuf()))
    merge_json = json.loads(message_to_json(merged.to_protobuf()))

    assert pre_merge_json["name"] == merge_json["name"]
    assert pre_merge_json["valueStr"] == merge_json["valueStr"]
    assert pre_merge_json["firstField"] == merge_json["firstField"]
    assert pre_merge_json["op"] == merge_json["op"]
    assert pre_merge_json["verbose"] == merge_json["verbose"]

    set_c_1 = SummaryConstraint("total_row_number", Op.EQ, 2)
    set_c_2 = numberOfRowsConstraint(n_rows=2)

    merged = set_c_1.merge(set_c_2)

    pre_merge_json = json.loads(message_to_json(set_c_1.to_protobuf()))
    merge_json = json.loads(message_to_json(merged.to_protobuf()))

    assert pre_merge_json["name"] == merge_json["name"]
    assert pre_merge_json["value"] == merge_json["value"]
    assert pre_merge_json["firstField"] == merge_json["firstField"]
    assert pre_merge_json["op"] == merge_json["op"]
    assert pre_merge_json["verbose"] == merge_json["verbose"]


def test_table_shape_serialization():

    ts1 = columnsMatchSetConstraint([1, 2, 3])

    ts2 = SummaryConstraint.from_protobuf(ts1.to_protobuf())

    ts1_json = json.loads(message_to_json(ts1.to_protobuf()))
    ts2_json = json.loads(message_to_json(ts2.to_protobuf()))

    ts1.merge(ts2)
    ts2.merge(ts1)

    assert ts1_json["name"] == ts2_json["name"]
    assert ts1_json["referenceSet"] == ts2_json["referenceSet"]
    assert ts1_json["firstField"] == ts2_json["firstField"]
    assert ts1_json["op"] == ts2_json["op"]
    assert ts1_json["verbose"] == ts2_json["verbose"]

    ts1 = columnExistsConstraint("c1")

    ts1.to_protobuf()

    ts2 = SummaryConstraint.from_protobuf(ts1.to_protobuf())

    ts1_json = json.loads(message_to_json(ts1.to_protobuf()))
    ts2_json = json.loads(message_to_json(ts2.to_protobuf()))

    ts1.merge(ts2)
    ts2.merge(ts1)

    assert ts1_json["name"] == ts2_json["name"]
    assert ts1_json["valueStr"] == ts2_json["valueStr"]
    assert ts1_json["firstField"] == ts2_json["firstField"]
    assert ts1_json["op"] == ts2_json["op"]
    assert ts1_json["verbose"] == ts2_json["verbose"]

    ts1 = numberOfRowsConstraint(2)

    ts2 = SummaryConstraint.from_protobuf(ts1.to_protobuf())

    ts1_json = json.loads(message_to_json(ts1.to_protobuf()))
    ts2_json = json.loads(message_to_json(ts2.to_protobuf()))

    ts1.merge(ts2)
    ts2.merge(ts1)

    assert ts1_json["name"] == ts2_json["name"]
    assert ts1_json["value"] == ts2_json["value"]
    assert ts1_json["firstField"] == ts2_json["firstField"]
    assert ts1_json["op"] == ts2_json["op"]
    assert ts1_json["verbose"] == ts2_json["verbose"]


def test_dataset_constraints_serialization():

    cvisc = columnValuesInSetConstraint(value_set={2, 5, 8})
    ltc = ValueConstraint(Op.LT, 1)

    min_gt_constraint = SummaryConstraint("min", Op.GT, value=100)
    max_le_constraint = SummaryConstraint("max", Op.LE, value=5)

    set1 = set(["col1", "col2"])
    columns_match_constraint = columnsMatchSetConstraint(set1)

    dc = DatasetConstraints(
        None,
        value_constraints={"annual_inc": [cvisc, ltc]},
        summary_constraints={"annual_inc": [max_le_constraint, min_gt_constraint]},
        table_shape_constraints=[columns_match_constraint],
    )

    dc_deser = DatasetConstraints.from_protobuf(dc.to_protobuf())

    props = dc.dataset_properties
    deser_props = dc_deser.dataset_properties

    if all([props, deser_props]):
        pm_json = json.loads(message_to_json(props))
        deser_pm_json = json.loads(message_to_json(deser_props))

        for (k, v), (k_deser, v_deser) in zip(pm_json.items(), deser_pm_json.items()):
            assert k == k_deser
            if all([v, v_deser]):
                v = v.sort() if isinstance(v, list) else v
                v_deser = v_deser.sort() if isinstance(v_deser, list) else v_deser
            assert v == v_deser

    value_constraints = dc.value_constraint_map
    summary_constraints = dc.summary_constraint_map
    table_shape_constraints = dc.table_shape_constraints

    deser_v_c = dc_deser.value_constraint_map
    deser_s_c = dc_deser.summary_constraint_map
    deser_ts_c = dc_deser.table_shape_constraints

    for (column, constraints), (deser_column, deser_constraints) in zip(value_constraints.items(), deser_v_c.items()):
        assert column == deser_column

        all_constraints = dict()
        all_constraints.update(constraints.raw_value_constraints)
        all_constraints.update(constraints.coerced_type_constraints)

        all_constraints_deser = dict()
        all_constraints_deser.update(deser_constraints.raw_value_constraints)
        all_constraints_deser.update(deser_constraints.coerced_type_constraints)

        for (name, c), (deser_name, deser_c) in zip(all_constraints.items(), all_constraints_deser.items()):
            assert name == deser_name

            a = json.loads(message_to_json(c.to_protobuf()))
            b = json.loads(message_to_json(deser_c.to_protobuf()))

            for (k, v), (k_deser, v_deser) in zip(a.items(), b.items()):
                assert k == k_deser
                if all([v, v_deser]):
                    v = v.sort() if isinstance(v, list) else v
                    v_deser = v_deser.sort() if isinstance(v_deser, list) else v_deser
                assert v == v_deser

    for (column, constraints), (deser_column, deser_constraints) in zip(summary_constraints.items(), deser_s_c.items()):
        assert column == deser_column

        for (name, c), (deser_name, deser_c) in zip(constraints.constraints.items(), deser_constraints.constraints.items()):
            assert name == deser_name

            a = json.loads(message_to_json(c.to_protobuf()))
            b = json.loads(message_to_json(deser_c.to_protobuf()))

            for (k, v), (k_deser, v_deser) in zip(a.items(), b.items()):
                assert k == k_deser
                if all([v, v_deser]):
                    v = v.sort() if isinstance(v, list) else v
                    v_deser = v_deser.sort() if isinstance(v_deser, list) else v_deser
                assert v == v_deser

    for (name, c), (deser_name, deser_c) in zip(table_shape_constraints.constraints.items(), deser_ts_c.constraints.items()):
        assert name == deser_name

        a = json.loads(message_to_json(c.to_protobuf()))
        b = json.loads(message_to_json(deser_c.to_protobuf()))

        for (k, v), (k_deser, v_deser) in zip(a.items(), b.items()):
            assert k == k_deser
            if all([v, v_deser]):
                v = v.sort() if isinstance(v, list) else v
                v_deser = v_deser.sort() if isinstance(v_deser, list) else v_deser
            assert v == v_deser

    report = dc.report()
    report_deser = dc_deser.report()

    assert report == report_deser


def test_most_common_value_in_set_constraint_apply(local_config_path, df_lending_club):
    val_set1 = {2, 3.5, 5000, 52000.0}
    val_set2 = {1, 2.3, "abc"}
    mcvc1 = columnMostCommonValueInSetConstraint(value_set=val_set1)
    mcvc2 = columnMostCommonValueInSetConstraint(value_set=val_set2)
    summary_constraints = {"loan_amnt": [mcvc1], "funded_amnt": [mcvc2]}
    report = _apply_summary_constraints_on_dataset(df_lending_club, local_config_path, summary_constraints)

    assert report[0][1][0][0] == f"summary most_common_value {Op.Name(Op.IN)} {val_set1}"
    assert report[0][1][0][1] == 1
    assert report[0][1][0][2] == 0

    assert report[1][1][0][0] == f"summary most_common_value {Op.Name(Op.IN)} {val_set2}"
    assert report[1][1][0][1] == 1
    assert report[1][1][0][2] == 1


def test_merge_most_common_value_in_set_constraint_different_values():
    c1 = columnMostCommonValueInSetConstraint(value_set={1, 3})
    c2 = columnMostCommonValueInSetConstraint(value_set={1, 5.0})
    with pytest.raises(AssertionError):
        c1.merge(c2)


def test_merge_most_common_value_in_set_constraint_same_values():
    val_set = {1, 2, 3}
    u1 = columnMostCommonValueInSetConstraint(value_set=val_set)
    u2 = columnMostCommonValueInSetConstraint(value_set=val_set)
    merged = u1.merge(u2)
    message = json.loads(message_to_json(merged.to_protobuf()))

    assert message["name"] == f"summary most_common_value {Op.Name(Op.IN)} {val_set}"
    assert message["firstField"] == "most_common_value"
    assert message["op"] == Op.Name(Op.IN)
    assert message["referenceSet"] == list(val_set)
    assert message["verbose"] is False


def test_serialization_deserialization_most_common_value_in_set_constraint():
    val_set = {1, "a", "abc"}
    u1 = columnMostCommonValueInSetConstraint(value_set=val_set, verbose=True)

    u1.from_protobuf(u1.to_protobuf())
    json_value = json.loads(message_to_json(u1.to_protobuf()))

    assert json_value["name"] == f"summary most_common_value {Op.Name(Op.IN)} {val_set}"
    assert json_value["firstField"] == "most_common_value"
    assert json_value["op"] == Op.Name(Op.IN)
    assert json_value["referenceSet"] == list(val_set)
    assert json_value["verbose"] is True


def test_most_common_value_in_set_constraint_wrong_datatype():
    with pytest.raises(TypeError):
        columnMostCommonValueInSetConstraint(value_set=2.3, verbose=True)


def test_column_values_not_null_constraint_apply_pass(local_config_path, df_lending_club):
    nnc1 = columnValuesNotNullConstraint()
    summary_constraints = {"annual_inc": [nnc1]}
    report = _apply_summary_constraints_on_dataset(df_lending_club, local_config_path, summary_constraints)

    TEST_LOGGER.info(f"Apply columnValuesNotNullConstraint report:\n{report}")

    assert report[0][1][0][0] == f"summary null_count {Op.Name(Op.EQ)} 0/None"
    assert report[0][1][0][1] == 1
    assert report[0][1][0][2] == 0


def test_column_values_not_null_constraint_apply_fail(local_config_path):
    nnc2 = columnValuesNotNullConstraint()
    df = pd.DataFrame([{"value": 1}, {"value": 5.2}, {"value": None}, {"value": 2.3}, {"value": None}])
    summary_constraints = {"value": [nnc2]}
    report = _apply_summary_constraints_on_dataset(df, local_config_path, summary_constraints)

    TEST_LOGGER.info(f"Apply columnValuesNotNullConstraint report:\n{report}")

    assert report[0][1][0][0] == f"summary null_count {Op.Name(Op.EQ)} 0/None"
    assert report[0][1][0][1] == 1
    assert report[0][1][0][2] == 1


def test_merge_column_values_not_null_constraint_different_values(local_config_path, df_lending_club):
    nnc1 = columnValuesNotNullConstraint()
    nnc2 = columnValuesNotNullConstraint()
    summary_constraints1 = {"annual_inc": [nnc1]}
    summary_constraints2 = {"annual_inc": [nnc2]}

    report1 = _apply_summary_constraints_on_dataset(df_lending_club, local_config_path, summary_constraints1)
    report2 = _apply_summary_constraints_on_dataset(df_lending_club, local_config_path, summary_constraints2)

    assert report1[0][1][0][0] == f"summary null_count {Op.Name(Op.EQ)} 0/None"
    assert report1[0][1][0][1] == 1
    assert report1[0][1][0][2] == 0

    assert report2[0][1][0][0] == f"summary null_count {Op.Name(Op.EQ)} 0/None"
    assert report2[0][1][0][1] == 1
    assert report2[0][1][0][2] == 0

    merged = nnc1.merge(nnc2)
    report_merged = merged.report()
    print(report_merged)
    TEST_LOGGER.info(f"Merged report of columnValuesNotNullConstraint: {report_merged}")

    assert merged.total == 2
    assert merged.failures == 0


def test_serialization_deserialization_column_values_not_null_constraint():
    nnc = columnValuesNotNullConstraint(verbose=True)

    nnc.from_protobuf(nnc.to_protobuf())
    json_value = json.loads(message_to_json(nnc.to_protobuf()))

    assert json_value["name"] == f"summary null_count {Op.Name(Op.EQ)} 0/None"
    assert json_value["firstField"] == "null_count"
    assert json_value["op"] == Op.Name(Op.EQ)
    assert pytest.approx(json_value["value"], 0.01) == 0
    assert json_value["verbose"] is True


def test_column_values_type_equals_constraint_apply(local_config_path, df_lending_club):
    cvtc = columnValuesTypeEqualsConstraint(expected_type=InferredType.Type.FRACTIONAL)
    summary_constraints = {"annual_inc": [cvtc]}
    report = _apply_summary_constraints_on_dataset(df_lending_club, local_config_path, summary_constraints)

    assert report[0][1][0][0] == f"summary column_values_type {Op.Name(Op.EQ)} {InferredType.Type.Name(InferredType.Type.FRACTIONAL)}"
    assert report[0][1][0][1] == 1
    assert report[0][1][0][2] == 0


def test_merge_column_values_type_equals_constraint_different_values():
    c1 = columnValuesTypeEqualsConstraint(expected_type=InferredType.Type.FRACTIONAL)
    c2 = columnValuesTypeEqualsConstraint(expected_type=InferredType.Type.NULL)
    with pytest.raises(AssertionError):
        c1.merge(c2)


def test_merge_column_values_type_equals_constraint_same_values():
    u1 = columnValuesTypeEqualsConstraint(expected_type=1)
    u2 = columnValuesTypeEqualsConstraint(expected_type=1)
    merged = u1.merge(u2)
    message = json.loads(message_to_json(merged.to_protobuf()))

    assert message["name"] == f"summary column_values_type {Op.Name(Op.EQ)} {InferredType.Type.Name(1)}"
    assert message["firstField"] == "column_values_type"
    assert message["op"] == Op.Name(Op.EQ)
    assert message["value"] == 1
    assert message["verbose"] is False


def test_serialization_deserialization_column_values_type_equals_constraint():
    u1 = columnValuesTypeEqualsConstraint(expected_type=InferredType.Type.STRING, verbose=True)

    u1.from_protobuf(u1.to_protobuf())
    json_value = json.loads(message_to_json(u1.to_protobuf()))

    assert json_value["name"] == f"summary column_values_type {Op.Name(Op.EQ)} {InferredType.Type.Name(InferredType.Type.STRING)}"
    assert json_value["firstField"] == "column_values_type"
    assert json_value["op"] == Op.Name(Op.EQ)
    assert json_value["value"] == InferredType.Type.STRING
    assert json_value["verbose"] is True


def test_column_values_type_equals_constraint_wrong_datatype():
    with pytest.raises(ValueError):
        columnValuesTypeEqualsConstraint(expected_type=2.3, verbose=True)
    with pytest.raises(ValueError):
        columnValuesTypeEqualsConstraint(expected_type="FRACTIONAL", verbose=True)


def test_column_values_type_in_set_constraint_apply(local_config_path, df_lending_club):
    type_set = {InferredType.Type.FRACTIONAL, InferredType.Type.INTEGRAL}
    cvtc = columnValuesTypeInSetConstraint(type_set=type_set)
    summary_constraint = {"annual_inc": [cvtc]}
    report = _apply_summary_constraints_on_dataset(df_lending_club, local_config_path, summary_constraint)

    type_names = {InferredType.Type.Name(t) for t in type_set}
    assert report[0][1][0][0] == f"summary column_values_type {Op.Name(Op.IN)} {type_names}"
    assert report[0][1][0][1] == 1
    assert report[0][1][0][2] == 0


def test_merge_column_values_type_in_set_constraint_different_values():
    c1 = columnValuesTypeInSetConstraint(type_set={InferredType.Type.INTEGRAL, InferredType.Type.STRING})
    c2 = columnValuesTypeInSetConstraint(type_set={InferredType.Type.INTEGRAL, InferredType.Type.NULL})
    with pytest.raises(AssertionError):
        c1.merge(c2)


def test_merge_column_values_type_in_set_constraint_same_values():
    type_set = {InferredType.Type.INTEGRAL, InferredType.Type.STRING}
    c1 = columnValuesTypeInSetConstraint(type_set=type_set)
    c2 = columnValuesTypeInSetConstraint(type_set=type_set)
    merged = c1.merge(c2)
    message = json.loads(message_to_json(merged.to_protobuf()))

    type_names = {InferredType.Type.Name(t) for t in type_set}
    assert message["name"] == f"summary column_values_type {Op.Name(Op.IN)} {type_names}"
    assert message["firstField"] == "column_values_type"
    assert message["op"] == Op.Name(Op.IN)
    assert message["referenceSet"] == list(type_set)
    assert message["verbose"] is False


def test_serialization_deserialization_column_values_type_in_set_constraint():
    type_set = {InferredType.Type.STRING, InferredType.Type.INTEGRAL}
    u1 = columnValuesTypeInSetConstraint(type_set=type_set, verbose=True)

    u1.from_protobuf(u1.to_protobuf())
    json_value = json.loads(message_to_json(u1.to_protobuf()))

    type_names = {InferredType.Type.Name(t) if isinstance(t, int) else InferredType.Type.Name(t.type) for t in type_set}
    assert json_value["name"] == f"summary column_values_type {Op.Name(Op.IN)} {type_names}"
    assert json_value["firstField"] == "column_values_type"
    assert json_value["op"] == Op.Name(Op.IN)
    assert json_value["referenceSet"] == list(type_set)
    assert json_value["verbose"] is True


def test_column_values_type_in_set_constraint_wrong_datatype():
    with pytest.raises(TypeError):
        columnValuesTypeInSetConstraint(type_set={2.3, 1}, verbose=True)
    with pytest.raises(TypeError):
        columnValuesTypeInSetConstraint(type_set={"FRACTIONAL", 2}, verbose=True)
    with pytest.raises(TypeError):
        columnValuesTypeInSetConstraint(type_set="ABCD")


def test_entropy_between_constraint_numeric_apply(local_config_path, df_lending_club):
    ec = approximateEntropyBetweenConstraint(lower_value=0.4, upper_value=0.5)
    summary_constraint = {"annual_inc": [ec]}
    report = _apply_summary_constraints_on_dataset(df_lending_club, local_config_path, summary_constraints=summary_constraint)
    # numeric
    assert report[0][1][0][0] == f"summary entropy {Op.Name(Op.BTWN)} 0.4 and 0.5"
    assert report[0][1][0][1] == 1
    assert report[0][1][0][2] == 1


def test_entropy_between_constraint_categorical_apply(local_config_path, df_lending_club):
    ec = approximateEntropyBetweenConstraint(lower_value=0.6, upper_value=1.5)
    summary_constraint = {"grade": [ec]}
    report = _apply_summary_constraints_on_dataset(df_lending_club, local_config_path, summary_constraints=summary_constraint)

    # categorical
    assert report[0][1][0][0] == f"summary entropy {Op.Name(Op.BTWN)} 0.6 and 1.5"
    assert report[0][1][0][1] == 1
    assert report[0][1][0][2] == 0


def test_entropy_between_constraint_null_apply(local_config_path, df_lending_club):
    ec = approximateEntropyBetweenConstraint(lower_value=0.6, upper_value=1.5)
    summary_constraint = {"member_id": [ec]}
    report = _apply_summary_constraints_on_dataset(df_lending_club, local_config_path, summary_constraints=summary_constraint)

    # categorical
    assert report[0][1][0][0] == f"summary entropy {Op.Name(Op.BTWN)} 0.6 and 1.5"
    assert report[0][1][0][1] == 1
    assert report[0][1][0][2] == 1


def test_merge_entropy_between_constraint_different_values():
    e1 = approximateEntropyBetweenConstraint(lower_value=1, upper_value=2.4)
    e2 = approximateEntropyBetweenConstraint(lower_value=1, upper_value=2.6)
    with pytest.raises(AssertionError):
        e1.merge(e2)


def test_merge_entropy_between_constraint_same_values():
    e1 = approximateEntropyBetweenConstraint(lower_value=1, upper_value=3.2)
    e2 = approximateEntropyBetweenConstraint(lower_value=1, upper_value=3.2)
    merged = e1.merge(e2)
    message = json.loads(message_to_json(merged.to_protobuf()))

    assert message["name"] == f"summary entropy {Op.Name(Op.BTWN)} 1 and 3.2"
    assert message["firstField"] == "entropy"
    assert message["op"] == Op.Name(Op.BTWN)
    assert pytest.approx(message["between"]["lowerValue"], 0.01) == 1
    assert pytest.approx(message["between"]["upperValue"], 0.01) == 3.2
    assert message["verbose"] is False


def test_serialization_deserialization_entropy_between_constraint():
    e1 = approximateEntropyBetweenConstraint(lower_value=0.3, upper_value=1.2, verbose=True)

    e1.from_protobuf(e1.to_protobuf())
    json_value = json.loads(message_to_json(e1.to_protobuf()))

    assert json_value["name"] == f"summary entropy {Op.Name(Op.BTWN)} 0.3 and 1.2"
    assert json_value["firstField"] == "entropy"
    assert json_value["op"] == Op.Name(Op.BTWN)
    assert pytest.approx(json_value["between"]["lowerValue"], 0.01) == 0.3
    assert pytest.approx(json_value["between"]["upperValue"], 0.01) == 1.2
    assert json_value["verbose"] is True


def test_entropy_between_constraint_wrong_datatype():
    with pytest.raises(TypeError):
        approximateEntropyBetweenConstraint(lower_value="2", upper_value=4, verbose=True)
    with pytest.raises(ValueError):
        approximateEntropyBetweenConstraint(lower_value=-2, upper_value=3, verbose=True)
    with pytest.raises(ValueError):
        approximateEntropyBetweenConstraint(lower_value=1, upper_value=0.9)


def test_ks_test_p_value_greater_than_constraint_false(df_lending_club, local_config_path):
    norm_values = np.random.normal(loc=10000, scale=2.0, size=15000)
    kspval = parametrizedKSTestPValueGreaterThanConstraint(norm_values, p_value=0.1)
    dc = DatasetConstraints(None, summary_constraints={"loan_amnt": [kspval]})
    config = load_config(local_config_path)
    session = session_from_config(config)
    profile = session.log_dataframe(df_lending_club, "test.data", constraints=dc)
    session.close()
    report = profile.apply_summary_constraints()

    # check if all of the rows have been reported
    assert report[0][1][0][1] == 1
    # check if the constraint failed once
    assert report[0][1][0][2] == 1


def test_ks_test_p_value_greater_than_constraint_true(df_lending_club, local_config_path):
    kspval = parametrizedKSTestPValueGreaterThanConstraint(df_lending_club["loan_amnt"].values, p_value=0.1)
    dc = DatasetConstraints(None, summary_constraints={"loan_amnt": [kspval]})
    config = load_config(local_config_path)
    session = session_from_config(config)
    profile = session.log_dataframe(df_lending_club, "test.data", constraints=dc)
    session.close()
    report = profile.apply_summary_constraints()

    # check if all of the rows have been reported
    assert report[0][1][0][1] == 1
    # check if the constraint was successfully executed
    assert report[0][1][0][2] == 0


def test_ks_test_p_value_greater_than_constraint_merge_different_values():
    ks1 = parametrizedKSTestPValueGreaterThanConstraint([1.0, 2.0, 3.0])
    ks2 = parametrizedKSTestPValueGreaterThanConstraint([1.0, 2.0, 4.0])
    with pytest.raises(AssertionError):
        ks1.merge(ks2)

    ks1 = parametrizedKSTestPValueGreaterThanConstraint([1.0, 2.0, 3.0], p_value=0.1)
    ks2 = parametrizedKSTestPValueGreaterThanConstraint([1.0, 2.0, 3.0], p_value=0.5)
    with pytest.raises(AssertionError):
        ks1.merge(ks2)


def test_ks_test_p_value_greater_than_constraint_merge_same_values():
    ks1 = parametrizedKSTestPValueGreaterThanConstraint([1.0, 2.0, 3.0])
    ks2 = parametrizedKSTestPValueGreaterThanConstraint([1.0, 2.0, 3.0])
    merged = ks1.merge(ks2)

    TEST_LOGGER.info(f"Serialize the merged parametrizedKSTestPValueGreaterThanConstraint:\n {merged.to_protobuf()}")

    json_value = json.loads(message_to_json(merged.to_protobuf()))
    assert json_value["name"] == f"summary ks_test p-value {Op.Name(Op.GT)} 0.05/None"
    assert json_value["op"] == Op.Name(Op.GT)
    assert json_value["firstField"] == "ks_test"
    assert pytest.approx(json_value["value"], 0.01) == 0.05
    assert len(json_value["continuousDistribution"]["sketch"]["sketch"]) > 0
    assert json_value["verbose"] is False


def test_serialization_deserialization_ks_test_p_value_greater_than_constraint():
    ks1 = parametrizedKSTestPValueGreaterThanConstraint([1.0, 2.0, 3.0], p_value=0.15, verbose=True)
    ks1.from_protobuf(ks1.to_protobuf())
    json_value = json.loads(message_to_json(ks1.to_protobuf()))

    TEST_LOGGER.info(f"Serialize parametrizedKSTestPValueGreaterThanConstraint from deserialized representation:\n {ks1.to_protobuf()}")

    assert json_value["name"] == f"summary ks_test p-value {Op.Name(Op.GT)} 0.15/None"
    assert json_value["op"] == Op.Name(Op.GT)
    assert json_value["firstField"] == "ks_test"
    assert pytest.approx(json_value["value"], 0.01) == 0.15
    assert len(json_value["continuousDistribution"]["sketch"]["sketch"]) > 0
    assert json_value["verbose"] is True


def test_ks_test_p_value_greater_than_constraint_wrong_datatype():
    with pytest.raises(ValueError):
        parametrizedKSTestPValueGreaterThanConstraint([1, 2, 3], p_value=0.15, verbose=True)
    with pytest.raises(TypeError):
        parametrizedKSTestPValueGreaterThanConstraint("abc", p_value=0.15, verbose=True)
    with pytest.raises(ValueError):
        parametrizedKSTestPValueGreaterThanConstraint([1, 2, 3], p_value=1.2, verbose=True)


def test_column_kl_divergence_less_than_constraint_continuous_false(df_lending_club, local_config_path):
    norm_values = np.random.normal(loc=10000, scale=2.0, size=15000)
    kld = columnKLDivergenceLessThanConstraint(norm_values, threshold=2.1)
    dc = DatasetConstraints(None, summary_constraints={"loan_amnt": [kld]})
    config = load_config(local_config_path)
    session = session_from_config(config)
    profile = session.log_dataframe(df_lending_club, "test.data", constraints=dc)
    session.close()
    report = profile.apply_summary_constraints()

    # check if all of the rows have been reported
    assert report[0][1][0][1] == 1
    # check if the constraint failed once
    assert report[0][1][0][2] == 1


def test_column_kl_divergence_less_than_constraint_continuous_true(df_lending_club, local_config_path):
    kld = columnKLDivergenceLessThanConstraint(df_lending_club["loan_amnt"].values, threshold=0.1)
    dc = DatasetConstraints(None, summary_constraints={"loan_amnt": [kld]})
    config = load_config(local_config_path)
    session = session_from_config(config)
    profile = session.log_dataframe(df_lending_club, "test.data", constraints=dc)
    session.close()
    report = profile.apply_summary_constraints()

    # check if all of the rows have been reported
    assert report[0][1][0][1] == 1
    # check if the constraint was successfully executed
    assert report[0][1][0][2] == 0


def test_column_kl_divergence_less_than_constraint_discrete_true(df_lending_club, local_config_path):
    np.random.seed(2)
    dist_data = np.random.choice(list(set(df_lending_club["grade"].values)), 100)
    kld = columnKLDivergenceLessThanConstraint(dist_data, threshold=1.3)
    dc = DatasetConstraints(None, summary_constraints={"grade": [kld]})
    config = load_config(local_config_path)
    session = session_from_config(config)
    profile = session.log_dataframe(df_lending_club, "test.data", constraints=dc)
    session.close()
    report = profile.apply_summary_constraints()

    # check if all of the rows have been reported
    assert report[0][1][0][1] == 1
    # check if the constraint was successfully executed
    assert report[0][1][0][2] == 0


def test_column_kl_divergence_less_than_constraint_discrete_false(df_lending_club, local_config_path):
    np.random.seed(2)
    dist_data = np.random.choice(list(set(df_lending_club["grade"].values)), 1000, p=[0.005, 0.005, 0.005, 0.03, 0.19, 0.765])
    kld = columnKLDivergenceLessThanConstraint(dist_data, threshold=0.4)
    dc = DatasetConstraints(None, summary_constraints={"grade": [kld]})
    config = load_config(local_config_path)
    session = session_from_config(config)
    profile = session.log_dataframe(df_lending_club, "test.data", constraints=dc)
    session.close()
    report = profile.apply_summary_constraints()

    # check if all of the rows have been reported
    assert report[0][1][0][1] == 1
    # check if the constraint was not successfully executed
    assert report[0][1][0][2] == 1


def test_column_kl_divergence_less_than_constraint_merge_different_values():
    ks1 = columnKLDivergenceLessThanConstraint([1.0, 2.0, 3.0])
    ks2 = columnKLDivergenceLessThanConstraint([1.0, 2.0, 4.0])
    with pytest.raises(AssertionError):
        ks1.merge(ks2)

    ks1 = columnKLDivergenceLessThanConstraint([1.0, 2.0, 3.0], threshold=0.1)
    ks2 = columnKLDivergenceLessThanConstraint([1.0, 2.0, 3.0], threshold=0.5)
    with pytest.raises(AssertionError):
        ks1.merge(ks2)


def test_column_kl_divergence_less_than_constraint_merge_same_values():
    ks1 = columnKLDivergenceLessThanConstraint([1.0, 2.0, 3.0])
    ks2 = columnKLDivergenceLessThanConstraint([1.0, 2.0, 3.0])
    merged = ks1.merge(ks2)

    TEST_LOGGER.info(f"Serialize the merged parametrizedKSTestPValueGreaterThanConstraint:\n {merged.to_protobuf()}")

    json_value = json.loads(message_to_json(merged.to_protobuf()))

    print(json_value)
    assert json_value["name"] == f"summary kl_divergence threshold {Op.Name(Op.LT)} 0.5/None"
    assert json_value["op"] == Op.Name(Op.LT)
    assert json_value["firstField"] == "kl_divergence"
    assert pytest.approx(json_value["value"], 0.01) == 0.5
    assert len(json_value["continuousDistribution"]["sketch"]["sketch"]) > 0
    assert json_value["verbose"] is False


def test_serialization_deserialization_column_kl_divergence_less_than_constraint_discrete():
    ks1 = columnKLDivergenceLessThanConstraint([1, 2, 3], threshold=0.15, verbose=True)
    ks1.from_protobuf(ks1.to_protobuf())
    json_value = json.loads(message_to_json(ks1.to_protobuf()))

    TEST_LOGGER.info(f"Serialize columnKLDivergenceLessThanConstraint from deserialized representation:\n {ks1.to_protobuf()}")

    assert json_value["name"] == f"summary kl_divergence threshold {Op.Name(Op.LT)} 0.15/None"
    assert json_value["op"] == Op.Name(Op.LT)
    assert json_value["firstField"] == "kl_divergence"
    assert pytest.approx(json_value["value"], 0.01) == 0.15
    assert len(json_value["discreteDistribution"]["frequentItems"]["items"]) == 3
    assert json_value["discreteDistribution"]["totalCount"] == 3
    assert json_value["verbose"] is True


def test_serialization_deserialization_column_kl_divergence_less_than_constraint_continuous():
    ks1 = columnKLDivergenceLessThanConstraint([2.0, 2.0, 3.0], threshold=0.15, verbose=True)
    ks1.from_protobuf(ks1.to_protobuf())
    json_value = json.loads(message_to_json(ks1.to_protobuf()))

    TEST_LOGGER.info(f"Serialize columnKLDivergenceLessThanConstraint from deserialized representation:\n {ks1.to_protobuf()}")

    assert json_value["name"] == f"summary kl_divergence threshold {Op.Name(Op.LT)} 0.15/None"
    assert json_value["op"] == Op.Name(Op.LT)
    assert json_value["firstField"] == "kl_divergence"
    assert pytest.approx(json_value["value"], 0.01) == 0.15
    assert len(json_value["continuousDistribution"]["sketch"]["sketch"]) > 0
    assert json_value["verbose"] is True


def test_column_kl_divergence_less_than_constraint_wrong_datatype():
    with pytest.raises(TypeError):
        columnKLDivergenceLessThanConstraint([1.0, "abc", 3], threshold=0.15, verbose=True)
    with pytest.raises(TypeError):
        columnKLDivergenceLessThanConstraint("abc", threshold=0.5, verbose=True)
    with pytest.raises(TypeError):
        columnKLDivergenceLessThanConstraint([1, 2, 3], threshold="1.2", verbose=True)


def test_chi_squared_test_p_value_greater_than_constraint_true(df_lending_club, local_config_path):
    test_values = ["A", "A", "B", "C", "C", "C", "C", "D", "D", "E", "F"]
    kspval = columnChiSquaredTestPValueGreaterThanConstraint(test_values, p_value=0.1)
    dc = DatasetConstraints(None, summary_constraints={"grade": [kspval]})
    config = load_config(local_config_path)
    session = session_from_config(config)
    profile = session.log_dataframe(df_lending_club, "test.data", constraints=dc)
    session.close()
    report = profile.apply_summary_constraints()

    # check if all of the rows have been reported
    assert report[0][1][0][1] == 1
    # check if the constraint was successful
    assert report[0][1][0][2] == 0


def test_chi_squared_test_p_value_greater_than_constraint_false(df_lending_club, local_config_path):
    test_values = {"C": 1, "B": 5, "A": 2, "D": 18, "E": 32, "F": 1}
    chi = columnChiSquaredTestPValueGreaterThanConstraint(test_values, p_value=0.05)
    dc = DatasetConstraints(None, summary_constraints={"grade": [chi]})
    config = load_config(local_config_path)
    session = session_from_config(config)
    profile = session.log_dataframe(df_lending_club, "test.data", constraints=dc)
    session.close()
    report = profile.apply_summary_constraints()

    # check if all of the rows have been reported
    assert report[0][1][0][1] == 1
    # check if the constraint failed
    assert report[0][1][0][2] == 1


def test_chi_squared_test_p_value_greater_than_constraint_merge_different_values():
    ks1 = columnChiSquaredTestPValueGreaterThanConstraint([1, 2, 3])
    ks2 = columnChiSquaredTestPValueGreaterThanConstraint([1, 2, 4])
    with pytest.raises(AssertionError):
        ks1.merge(ks2)

    ks1 = columnChiSquaredTestPValueGreaterThanConstraint([1, 2, 3], p_value=0.1)
    ks2 = columnChiSquaredTestPValueGreaterThanConstraint([1, 2, 3], p_value=0.5)
    with pytest.raises(AssertionError):
        ks1.merge(ks2)


def test_column_chi_squared_test_p_value_greater_than_constraint_merge_same_values():
    ks1 = columnChiSquaredTestPValueGreaterThanConstraint([1, 3, "A"])
    ks2 = columnChiSquaredTestPValueGreaterThanConstraint([1, "A", 3])
    merged = ks1.merge(ks2)

    TEST_LOGGER.info(f"Serialize the merged columnChiSquaredTestPValueGreaterThanConstraint:\n {merged.to_protobuf()}")

    json_value = json.loads(message_to_json(merged.to_protobuf()))
    assert json_value["name"] == f"summary chi_squared_test p-value {Op.Name(Op.GT)} 0.05/None"
    assert json_value["op"] == Op.Name(Op.GT)
    assert json_value["firstField"] == "chi_squared_test"
    assert pytest.approx(json_value["value"], 0.01) == 0.05
    assert len(json_value["discreteDistribution"]["frequentItems"]["items"]) == 3
    assert json_value["verbose"] is False


def test_serialization_deserialization_chi_squared_test_p_value_greater_than_constraint():
    ks1 = columnChiSquaredTestPValueGreaterThanConstraint([1, 2, "A", "B"], p_value=0.15, verbose=True)
    ks1.from_protobuf(ks1.to_protobuf())
    json_value = json.loads(message_to_json(ks1.to_protobuf()))

    TEST_LOGGER.info(f"Serialize columnChiSquaredTestPValueGreaterThanConstraint from deserialized representation:\n {ks1.to_protobuf()}")
    assert json_value["name"] == f"summary chi_squared_test p-value {Op.Name(Op.GT)} 0.15/None"
    assert json_value["op"] == Op.Name(Op.GT)
    assert json_value["firstField"] == "chi_squared_test"
    assert pytest.approx(json_value["value"], 0.01) == 0.15
    assert len(json_value["discreteDistribution"]["frequentItems"]["items"]) == 4
    assert json_value["verbose"] is True


def test_chi_squared_test_p_value_greater_than_constraint_wrong_datatype():
    with pytest.raises(ValueError):
        columnChiSquaredTestPValueGreaterThanConstraint([1.0, 2, 3], p_value=0.15, verbose=True)
    with pytest.raises(TypeError):
        columnChiSquaredTestPValueGreaterThanConstraint("abc", p_value=0.15, verbose=True)
    with pytest.raises(ValueError):
        columnChiSquaredTestPValueGreaterThanConstraint({"A": 0.3, "B": 1, "C": 12}, p_value=0.2, verbose=True)
    with pytest.raises(TypeError):
        columnChiSquaredTestPValueGreaterThanConstraint(["a", "b", "c"], p_value=1.2, verbose=True)


def test_generate_default_constraints_categorical(local_config_path):
    usernames = ["jd123", "jane.doe@example.com", "bobsmith", "_anna_"]
    emails = ["john.doe@example.com", "jane.doe@example.com", "bob.smith@example.com", "anna_jones@example.com"]
    data = pd.DataFrame(
        {
            "username": usernames,
            "email": emails,
        }
    )
    config = load_config(local_config_path)
    session = session_from_config(config)
    profile = session.log_dataframe(data, "test.data")
    generated_constraints = profile.generate_constraints()

    json_summ = json.loads(message_to_json(generated_constraints.to_protobuf()))
    constraints_username = json_summ["summaryConstraints"]["username"]["constraints"]
    constraints_email = json_summ["summaryConstraints"]["email"]["constraints"]

    # username constraints
    assert len(constraints_username) == 3  # column value type equals, unique count between and most common value in set
    assert constraints_username[0]["name"] == "summary column_values_type EQ STRING"
    assert constraints_username[0]["firstField"] == "column_values_type"
    assert constraints_username[0]["value"] == InferredType.STRING
    assert constraints_username[0]["op"] == Op.Name(Op.EQ)
    assert constraints_username[0]["verbose"] is False

    # there are 4 unique values in the df for username, so the unique count between is in the range 4-1 and 4+1
    assert constraints_username[1]["name"] == "summary unique_count BTWN 3 and 5"
    assert constraints_username[1]["firstField"] == "unique_count"
    assert constraints_username[1]["op"] == Op.Name(Op.BTWN)
    assert pytest.approx(constraints_username[1]["between"]["lowerValue"], 0.001) == 3
    assert pytest.approx(constraints_username[1]["between"]["upperValue"], 0.001) == 5
    assert constraints_username[1]["verbose"] is False

    assert f"summary most_common_value IN" in constraints_username[2]["name"]  # set has different order
    assert constraints_username[2]["firstField"] == "most_common_value"
    assert constraints_username[2]["op"] == Op.Name(Op.IN)
    assert set(constraints_username[2]["referenceSet"]) == set(usernames)
    assert constraints_username[2]["verbose"] is False

    # email constraints
    assert len(constraints_email) == 3  # column value type equals, unique count between and most common value in set
    assert constraints_email[0]["name"] == "summary column_values_type EQ STRING"
    assert constraints_email[0]["firstField"] == "column_values_type"
    assert constraints_email[0]["value"] == InferredType.STRING
    assert constraints_email[0]["op"] == Op.Name(Op.EQ)
    assert constraints_email[0]["verbose"] is False

    # there are 4 unique values in the df for username, so the unique count between is in the range 4-1 and 4+1
    assert constraints_email[1]["name"] == "summary unique_count BTWN 3 and 5"
    assert constraints_email[1]["firstField"] == "unique_count"
    assert constraints_email[1]["op"] == Op.Name(Op.BTWN)
    assert pytest.approx(constraints_email[1]["between"]["lowerValue"], 0.001) == 3
    assert pytest.approx(constraints_email[1]["between"]["upperValue"], 0.001) == 5
    assert constraints_email[1]["verbose"] is False

    assert f"summary most_common_value IN" in constraints_email[2]["name"]  # set has different order
    assert constraints_email[2]["firstField"] == "most_common_value"
    assert constraints_email[2]["op"] == Op.Name(Op.IN)
    assert set(constraints_email[2]["referenceSet"]) == set(emails)
    assert constraints_email[2]["verbose"] is False


def test_generate_default_constraints_numeric(local_config_path):
    data = pd.DataFrame(
        {
            "followers": [1525, 12268, 51343, 867, 567, 100265, 22113, 3412],
            "points": [23.4, 123.2, 432.22, 32.1, 44.1, 42.2, 344.2, 42.1],
        }
    )

    config = load_config(local_config_path)
    session = session_from_config(config)
    profile = session.log_dataframe(data, "test.data")
    generated_constraints = profile.generate_constraints()

    json_summ = json.loads(message_to_json(generated_constraints.to_protobuf()))
    followers_constraints = json_summ["summaryConstraints"]["followers"]["constraints"]
    points_constraints = json_summ["summaryConstraints"]["points"]["constraints"]

    assert len(followers_constraints) == 5
    # min greater than 0, mean between mean-stddev and mean+stddev,
    # column values type, most common value in set, unique count between

    followers_mean = data["followers"].mean()
    followers_stddev = data["followers"].std()
    lower_followers = followers_mean - followers_stddev
    upper_followers = followers_mean + followers_stddev

    assert followers_constraints[0]["name"] == "summary min GE 0/None"
    assert followers_constraints[1]["name"] == f"summary mean BTWN {lower_followers} and {upper_followers}"
    assert followers_constraints[2]["name"] == "summary column_values_type EQ INTEGRAL"
    assert followers_constraints[3]["name"] == "summary unique_count BTWN 7 and 9"  # we have 8 unique values in the df
    assert "summary most_common_value IN" in followers_constraints[4]["name"]

    assert len(points_constraints) == 4
    # min greater than 0, mean between mean-stddev and mean+stddev,
    # column values type, most common value in set
    points_mean = data["points"].mean()
    points_stddev = data["points"].std()
    lower_points = points_mean - points_stddev
    upper_points = points_mean + points_stddev

    assert points_constraints[0]["name"] == "summary min GE 0/None"
    assert points_constraints[1]["name"] == f"summary mean BTWN {lower_points} and {upper_points}"
    assert points_constraints[2]["name"] == "summary column_values_type EQ FRACTIONAL"
    assert "summary most_common_value IN" in points_constraints[3]["name"]


def test_generate_default_constraints_mixed(local_config_path):
    data = pd.DataFrame(
        {"username": ["jd123", "jane.doe@example.com", "bobsmith", "_anna_"], "followers": [1525, 12268, 51343, 867], "null": [None, None, None, None]}
    )

    config = load_config(local_config_path)
    session = session_from_config(config)
    profile = session.log_dataframe(data, "test.data")
    generated_constraints = profile.generate_constraints()

    json_summ = json.loads(message_to_json(generated_constraints.to_protobuf()))
    username_constraints = json_summ["summaryConstraints"]["username"]["constraints"]
    followers_constraints = json_summ["summaryConstraints"]["followers"]["constraints"]

    # no constraints should be generated for the null column since all values are None
    assert "null" not in json_summ["summaryConstraints"]

    assert len(username_constraints) == 3  # column value type equals, unique count between and most common value in set
    assert username_constraints[0]["name"] == "summary column_values_type EQ STRING"
    assert username_constraints[1]["name"] == "summary unique_count BTWN 3 and 5"  # we have 4 unique values in df
    assert f"summary most_common_value IN" in username_constraints[2]["name"]

    assert len(followers_constraints) == 5
    # min greater than 0, mean between mean-stddev and mean+stddev,
    # column values type, most common value in set, unique count between

    followers_mean = data["followers"].mean()
    followers_stddev = data["followers"].std()
    lower_followers = followers_mean - followers_stddev
    upper_followers = followers_mean + followers_stddev

    assert followers_constraints[0]["name"] == "summary min GE 0/None"
    assert followers_constraints[1]["name"] == f"summary mean BTWN {lower_followers} and {upper_followers}"
    assert followers_constraints[2]["name"] == "summary column_values_type EQ INTEGRAL"
    assert followers_constraints[3]["name"] == "summary unique_count BTWN 3 and 5"  # we have 4 unique values in the df
    assert "summary most_common_value IN" in followers_constraints[4]["name"]


def _apply_value_constraints_on_dataset(df_lending_club, local_config_path, value_constraints=None, multi_column_value_constraints=None):
    dc = DatasetConstraints(None, value_constraints=value_constraints, multi_column_value_constraints=multi_column_value_constraints)
    config = load_config(local_config_path)
    session = session_from_config(config)
    profile = session.log_dataframe(df_lending_club, "test.data", constraints=dc)
    session.close()
    return dc.report()


def test_sum_of_row_values_of_multiple_columns_constraint_apply(local_config_path, df_lending_club):
    col_set = ["loan_amnt", "int_rate"]
    srveq = sumOfRowValuesOfMultipleColumnsEqualsConstraint(columns=col_set, value="total_pymnt", verbose=False)
    multi_column_value_constraints = [srveq]
    report = _apply_value_constraints_on_dataset(df_lending_club, local_config_path, multi_column_value_constraints=multi_column_value_constraints)

    total_payment = ["total_pymnt"]
    assert report[0][0] == f"multi column value SUM {col_set} {Op.Name(Op.EQ)} {total_payment}"
    assert report[0][1] == 50
    assert report[0][2] == 50


def test_sum_of_row_values_of_multiple_columns_constraint_apply_true(local_config_path):
    colset = ["A", "B"]
    srveq = sumOfRowValuesOfMultipleColumnsEqualsConstraint(columns=colset, value=100, verbose=False)

    dc = DatasetConstraints(None, multi_column_value_constraints=[srveq])
    config = load_config(local_config_path)
    session = session_from_config(config)
    df = pd.DataFrame(
        [
            {"A": 1, "B": 2},  # fail
            {"A": 99, "B": 1},  # pass
            {"A": 32, "B": 68},  # pass
            {"A": 100, "B": 2},  # fail
            {"A": 83, "B": 18},  # fail
        ]
    )
    profile = session.log_dataframe(df, "test.data", constraints=dc)
    session.close()
    report = dc.report()

    assert report[0][0] == f"multi column value SUM {colset} {Op.Name(Op.EQ)} 100"
    assert report[0][1] == 5
    assert report[0][2] == 3


def test_merge_sum_of_row_values_different_values():
    cpvis1 = sumOfRowValuesOfMultipleColumnsEqualsConstraint(columns=["annual_inc", "loan_amnt"], value="grade")
    cpvis2 = sumOfRowValuesOfMultipleColumnsEqualsConstraint(columns=["annual_inc", "total_pymnt"], value="grade")
    cpvis3 = sumOfRowValuesOfMultipleColumnsEqualsConstraint(columns=["annual_inc", "total_pymnt"], value="loan_amnt")

    with pytest.raises(AssertionError):
        cpvis1.merge(cpvis2)
    with pytest.raises(AssertionError):
        cpvis2.merge(cpvis3)


def test_merge_sum_of_row_values_constraint_valid():
    col_set = ["loan_amnt", "int_rate"]
    srveq1 = sumOfRowValuesOfMultipleColumnsEqualsConstraint(columns=col_set, value="total_pymnt", verbose=False)
    srveq1.total = 5
    srveq1.failures = 1
    srveq2 = sumOfRowValuesOfMultipleColumnsEqualsConstraint(columns=col_set, value="total_pymnt", verbose=False)
    srveq2.total = 3
    srveq2.failures = 2

    srveq_merged = srveq1.merge(srveq2)
    json_value = json.loads(message_to_json(srveq_merged.to_protobuf()))

    total_payment = ["total_pymnt"]
    assert json_value["name"] == f"multi column value SUM {col_set} {Op.Name(Op.EQ)} {total_payment}"
    assert json_value["dependentColumns"] == col_set
    assert json_value["op"] == Op.Name(Op.EQ)
    assert json_value["referenceColumns"][0] == "total_pymnt"
    assert json_value["verbose"] is False

    report = srveq_merged.report()
    assert report[1] == 8
    assert report[2] == 3


def test_serialization_deserialization_sum_of_row_values_constraint():
    columns = ["A", "B", "C"]
    c = sumOfRowValuesOfMultipleColumnsEqualsConstraint(columns=columns, value=6, verbose=True)

    c.from_protobuf(c.to_protobuf())
    json_value = json.loads(message_to_json(c.to_protobuf()))

    assert json_value["name"] == f"multi column value SUM {columns} {Op.Name(Op.EQ)} 6"
    assert json_value["dependentColumns"] == columns
    assert json_value["op"] == Op.Name(Op.EQ)
    assert pytest.approx(json_value["value"], 0.01) == 6
    assert json_value["internalDependentColumnsOp"] == Op.Name(Op.SUM)
    assert json_value["verbose"] is True


def test_sum_of_row_values_constraint_invalid_params():
    with pytest.raises(TypeError):
        sumOfRowValuesOfMultipleColumnsEqualsConstraint(columns=1, value="B")
    with pytest.raises(TypeError):
        sumOfRowValuesOfMultipleColumnsEqualsConstraint(columns=[1, 2], value="B")
    with pytest.raises(TypeError):
        sumOfRowValuesOfMultipleColumnsEqualsConstraint(columns=1, value=["b"])


def test_multi_column_value_constraints_logical_operation(local_config_path):
    a_gt_b = columnValuesAGreaterThanBConstraint("col1", "col2")

    df = pd.DataFrame({"col1": [4, 5, 6, 7], "col2": [0, 1, 2, 3]})

    dc = DatasetConstraints(None, multi_column_value_constraints=[a_gt_b])

    config = load_config(local_config_path)
    session = session_from_config(config)

    profile = session.log_dataframe(df, "test.data", constraints=dc)
    session.close()
    report = dc.report()

    assert len(report[0]) == 3

    assert report[0][1] == 4 and report[0][2] == 0 and report[0][0] == f"multi column value col1 {Op.Name(Op.GT)} col2"


def test_multi_column_value_constraints_merge_error():
    mcvc1 = columnValuesAGreaterThanBConstraint("col1", "col2")
    mcvc2 = MultiColumnValueConstraint("col1", op=Op.GT, reference_columns="col5")

    with pytest.raises(AssertionError):
        mcvc1.merge(mcvc2)

    mcvc1 = columnValuesAGreaterThanBConstraint("col1", "col2")
    mcvc2 = MultiColumnValueConstraint("col4", op=Op.GT, reference_columns="col2")

    with pytest.raises(AssertionError):
        mcvc1.merge(mcvc2)

    mcvc1 = columnValuesAGreaterThanBConstraint("col1", "col2")
    mcvc2 = MultiColumnValueConstraint("col1", op=Op.EQ, reference_columns="col2")

    with pytest.raises(AssertionError):
        mcvc1.merge(mcvc2)

    mcvc1 = columnValuesAGreaterThanBConstraint("col1", "col2")
    mcvc2 = MultiColumnValueConstraint("col1", op=Op.GT, value=2)

    with pytest.raises(AssertionError):
        mcvc1.merge(mcvc2)


def test_multi_column_value_constraints_merge_valid():
    mcvc1 = columnValuesAGreaterThanBConstraint("col1", "col2")
    mcvc2 = MultiColumnValueConstraint("col1", op=Op.GT, reference_columns="col2")

    merged = mcvc1.merge(mcvc2)

    pre_merge_json = json.loads(message_to_json(mcvc1.to_protobuf()))
    merge_json = json.loads(message_to_json(merged.to_protobuf()))

    assert pre_merge_json["name"] == merge_json["name"]
    assert pre_merge_json["dependentColumn"] == merge_json["dependentColumn"]
    assert pre_merge_json["referenceColumns"] == merge_json["referenceColumns"]
    assert pre_merge_json["op"] == merge_json["op"]
    assert pre_merge_json["verbose"] == merge_json["verbose"]


def test_multi_column_value_constraints_invalid_init():
    with pytest.raises(ValueError):
        mcvc = MultiColumnValueConstraint(None, op=Op.GT, reference_columns="col2")

    with pytest.raises(TypeError):
        mcvc = MultiColumnValueConstraint(1, op=Op.GT, reference_columns="col2")

    with pytest.raises(ValueError):
        mcvc = MultiColumnValueConstraint("a", op=Op.GT, reference_columns="col2", value=2)

    with pytest.raises(ValueError):
        mcvc = MultiColumnValueConstraint("a", op=Op.GT)

    with pytest.raises(ValueError):
        mcvc = MultiColumnValueConstraint("a", op=Op.GT, internal_dependent_cols_op=Op.GT)


def test_multi_column_value_constraints_serialization_reference_columns():

    mcvc1 = MultiColumnValueConstraint(dependent_columns=["col1", "col2"], reference_columns=["col5", "col6"], op=Op.EQ)

    mcvc2 = MultiColumnValueConstraint.from_protobuf(mcvc1.to_protobuf())

    mcvc1_json = json.loads(message_to_json(mcvc1.to_protobuf()))
    mcvc2_json = json.loads(message_to_json(mcvc2.to_protobuf()))

    mcvc1.merge(mcvc2)
    mcvc2.merge(mcvc1)

    assert mcvc1_json["name"] == mcvc2_json["name"]
    assert mcvc1.to_protobuf().dependent_columns == mcvc2.to_protobuf().dependent_columns
    assert mcvc1.to_protobuf().reference_columns == mcvc2.to_protobuf().reference_columns
    assert mcvc1_json["op"] == mcvc2_json["op"]
    assert mcvc1_json["verbose"] == mcvc2_json["verbose"]


def test_multi_column_value_constraints_serialization_value():
    mcvc1 = MultiColumnValueConstraint(["col1", "col2"], op=Op.GT, value=2)

    mcvc2 = MultiColumnValueConstraint.from_protobuf(mcvc1.to_protobuf())

    mcvc1_json = json.loads(message_to_json(mcvc1.to_protobuf()))
    mcvc2_json = json.loads(message_to_json(mcvc2.to_protobuf()))

    mcvc1.merge(mcvc2)
    mcvc2.merge(mcvc1)

    assert mcvc1_json["name"] == mcvc2_json["name"]
    assert mcvc1.to_protobuf().dependent_columns == mcvc2.to_protobuf().dependent_columns
    assert mcvc1_json["value"] == mcvc2_json["value"]
    assert mcvc1_json["op"] == mcvc2_json["op"]
    assert mcvc1_json["verbose"] == mcvc2_json["verbose"]


def test_multi_column_value_constraints_serialization_value_set():
    mcvc1 = MultiColumnValueConstraint(["col1", "col2"], op=Op.GT, value=set([1, 2]))

    mcvc2 = MultiColumnValueConstraint.from_protobuf(mcvc1.to_protobuf())

    mcvc1_json = json.loads(message_to_json(mcvc1.to_protobuf()))
    mcvc2_json = json.loads(message_to_json(mcvc2.to_protobuf()))

    mcvc1.merge(mcvc2)
    mcvc2.merge(mcvc1)

    assert mcvc1_json["name"] == mcvc2_json["name"]
    assert mcvc1.to_protobuf().dependent_columns == mcvc2.to_protobuf().dependent_columns
    assert mcvc1.to_protobuf().value_set == mcvc2.to_protobuf().value_set
    assert mcvc1_json["op"] == mcvc2_json["op"]
    assert mcvc1_json["verbose"] == mcvc2_json["verbose"]


def test_column_pair_values_in_valid_set_constraint_apply(local_config_path, df_lending_club):
    val_set = {("B", "B2"), ("C", "C2"), ("D", "D1")}  # the second pair is found in the data set
    cpvis = columnPairValuesInSetConstraint(column_A="grade", column_B="sub_grade", value_set=val_set)
    multi_column_value_constraints = [cpvis]
    report = _apply_value_constraints_on_dataset(df_lending_club, local_config_path, multi_column_value_constraints=multi_column_value_constraints)

    col_set = ["grade", "sub_grade"]
    assert report[0][0] == f"multi column value {col_set} {Op.Name(Op.IN)} {val_set}"
    assert report[0][1] == 50
    assert report[0][2] == 45  # 5 values are in the set


def test_merge_column_pair_values_in_valid_set_constraint_different_values():
    val_set1 = {(12345, "B"), (41000.0, "C"), (42333, "D")}
    cpvis1 = columnPairValuesInSetConstraint(column_A="annual_inc", column_B="grade", value_set=val_set1)
    val_set2 = {(12345, "B"), (1111, "C"), (42333, "D")}
    cpvis2 = columnPairValuesInSetConstraint(column_A="annual_inc", column_B="grade", value_set=val_set2)
    cpvis3 = columnPairValuesInSetConstraint(column_A="some", column_B="grade", value_set=val_set2)
    cpvis4 = columnPairValuesInSetConstraint(column_A="annual_inc", column_B="b", value_set=val_set1)

    with pytest.raises(AssertionError):
        cpvis1.merge(cpvis2)
    with pytest.raises(AssertionError):
        cpvis2.merge(cpvis3)
    with pytest.raises(AssertionError):
        cpvis1.merge(cpvis4)


def test_merge_column_pair_values_in_valid_set_constraint_valid():
    val_set1 = {(12345, "B"), (41000.0, "C"), (42333, "D")}
    cpvis1 = columnPairValuesInSetConstraint(column_A="annual_inc", column_B="grade", value_set=val_set1)
    cpvis1.total = 5
    cpvis1.failures = 1
    cpvis2 = columnPairValuesInSetConstraint(column_A="annual_inc", column_B="grade", value_set=val_set1)
    cpvis2.total = 3
    cpvis2.failures = 2

    cpvis_merged = cpvis1.merge(cpvis2)
    json_value = json.loads(message_to_json(cpvis_merged.to_protobuf()))

    col_set = ["annual_inc", "grade"]
    assert json_value["name"] == f"multi column value {col_set} {Op.Name(Op.IN)} {val_set1}"
    assert json_value["dependentColumns"] == col_set
    assert json_value["op"] == Op.Name(Op.IN)
    assert json_value["valueSet"] == [list(t) for t in val_set1]
    assert json_value["verbose"] is False

    report = cpvis_merged.report()
    assert report[1] == 8
    assert report[2] == 3


def test_serialization_deserialization_column_pair_values_in_valid_set_constraint():
    val_set1 = {(12345, "B"), (41000.0, "C"), (42333, "D")}
    c = columnPairValuesInSetConstraint(column_A="A", column_B="B", value_set=val_set1, verbose=True)

    c.from_protobuf(c.to_protobuf())
    json_value = json.loads(message_to_json(c.to_protobuf()))

    val_set1 = set(val_set1)
    col_set = ["A", "B"]
    assert json_value["name"] == f"multi column value {col_set} {Op.Name(Op.IN)} {val_set1}"
    assert json_value["dependentColumns"] == col_set
    assert json_value["op"] == Op.Name(Op.IN)
    assert json_value["valueSet"] == [list(t) for t in val_set1]
    assert json_value["verbose"] is True


def test_column_pair_values_in_set_constraint_invalid_params():
    with pytest.raises(TypeError):
        columnPairValuesInSetConstraint(column_A=1, column_B="B", value_set={("A", "B")})
    with pytest.raises(TypeError):
        columnPairValuesInSetConstraint(column_A="A", column_B=["A"], value_set={("A", "B")})
    with pytest.raises(TypeError):
        columnPairValuesInSetConstraint(column_A="A", column_B="B", value_set=1.0)
    with pytest.raises(TypeError):
        columnPairValuesInSetConstraint(column_A="A", column_B="B", value_set="ABC")


def test_column_values_unique_within_row_constraint_apply(local_config_path, df_lending_club):
    cvu = columnValuesUniqueWithinRow(column_A="grade", verbose=True)
    multi_column_value_constraints = [cvu]
    report = _apply_value_constraints_on_dataset(df_lending_club, local_config_path, multi_column_value_constraints=multi_column_value_constraints)

    assert report[0][0] == f"multi column value grade {Op.Name(Op.NOT_IN)} all"
    assert report[0][1] == 50
    assert report[0][2] == 0


def test_merge_column_values_unique_within_row_constraint_different_values():
    cvu1 = columnValuesUniqueWithinRow(column_A="A")
    cvu2 = columnValuesUniqueWithinRow(column_A="A1", verbose=True)

    with pytest.raises(AssertionError):
        cvu1.merge(cvu2)


def test_merge_column_values_unique_within_row_constraint_valid():
    cvu1 = columnValuesUniqueWithinRow(column_A="A")
    cvu1.total = 5
    cvu1.failures = 1
    cvu2 = columnValuesUniqueWithinRow(column_A="A", verbose=True)
    cvu2.total = 3
    cvu2.failures = 2

    merged = cvu1.merge(cvu2)
    json_value = json.loads(message_to_json(merged.to_protobuf()))

    assert json_value["name"] == f"multi column value A {Op.Name(Op.NOT_IN)} all"
    assert json_value["dependentColumn"] == "A"
    assert json_value["op"] == Op.Name(Op.NOT_IN)
    assert json_value["referenceColumns"] == ["all"]
    assert json_value["verbose"] is False

    report = merged.report()
    assert report[1] == 8
    assert report[2] == 3


def test_serialization_deserialization_column_values_unique_within_row_constraint():
    c = columnValuesUniqueWithinRow(column_A="A", verbose=True)

    c.from_protobuf(c.to_protobuf())
    json_value = json.loads(message_to_json(c.to_protobuf()))

    assert json_value["name"] == f"multi column value A {Op.Name(Op.NOT_IN)} all"
    assert json_value["dependentColumn"] == "A"
    assert json_value["op"] == Op.Name(Op.NOT_IN)
    assert json_value["referenceColumns"] == ["all"]
    assert json_value["verbose"] is True


def test_column_values_unique_within_row_constraint_invalid_params():
    with pytest.raises(TypeError):
        columnValuesUniqueWithinRow(column_A=1)
    with pytest.raises(TypeError):
        columnValuesUniqueWithinRow(column_A=["A"])
