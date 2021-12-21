import json
from logging import getLogger

import pandas as pd
import pytest

from whylogs.app.config import load_config
from whylogs.app.session import session_from_config
from whylogs.core.statistics.constraints import (
    DatasetConstraints,
    Op,
    SummaryConstraint,
    ValueConstraint,
    ValueConstraints,
    _summary_funcs1,
    _value_funcs,
    columnValuesInSetConstraint,
    containsCreditCardConstraint,
    containsEmailConstraint,
    containsSSNConstraint,
    containsURLConstraint,
    distinctValuesContainSetConstraint,
    distinctValuesEqualSetConstraint,
    distinctValuesInSetConstraint,
    maxBetweenConstraint,
    meanBetweenConstraint,
    minBetweenConstraint,
    stddevBetweenConstraint,
    stringLengthBetweenConstraint,
    stringLengthEqualConstraint,
)
from whylogs.proto import Op
from whylogs.util.protobuf import message_to_json

TEST_LOGGER = getLogger(__name__)


def test_value_summary_serialization():

    for each_op, _ in _value_funcs.items():
        if each_op == Op.IN_SET:
            value = ValueConstraint(each_op, {3.6})
        else:
            value = ValueConstraint(each_op, 3.6)
        msg_value = value.to_protobuf()
        json_value = json.loads(message_to_json(msg_value))
        if each_op == Op.IN_SET:
            assert json_value["name"] == "value " + Op.Name(each_op) + " {3.6}"
            assert json_value["valueSet"][0] == [3.6]
        else:
            assert json_value["name"] == "value " + Op.Name(each_op) + " 3.6"
            assert pytest.approx(json_value["value"], 0.001) == 3.6
        assert json_value["op"] == Op.Name(each_op)
        assert json_value["verbose"] == False

    for each_op, _ in _summary_funcs1.items():
        if each_op in (Op.BTWN, Op.IN_SET, Op.CONTAIN_SET, Op.EQ_SET):
            continue
        # constraints may have an optional name
        sum_constraint = SummaryConstraint("min", each_op, 300000, name="< 30K")
        msg_sum_const = sum_constraint.to_protobuf()
        json_summary = json.loads(message_to_json(msg_sum_const))

        assert json_summary["name"] == "< 30K"
        assert pytest.approx(json_summary["value"], 0.1) == 300000
        assert json_summary["firstField"] == "min"
        assert json_summary["op"] == str(Op.Name(each_op))
        assert json_summary["verbose"] == False


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
    print(report)
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
    print(report)
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

    print(report)
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


def _apply_set_summary_constraints_on_dataset(df_lending_club, local_config_path, constraints):

    dc = DatasetConstraints(None, summary_constraints={"annual_inc": constraints})
    config = load_config(local_config_path)
    session = session_from_config(config)
    profile = session.log_dataframe(df_lending_club, "test.data", constraints=dc)
    session.close()
    report = profile.apply_summary_constraints()

    print(report)
    assert len(report) == 1

    # make sure it checked every value
    for each_feat in report:
        for each_constraint in each_feat[1]:
            assert each_constraint[1] == 1
            if "True" in each_constraint[0]:
                assert each_constraint[2] == 0
            else:
                assert each_constraint[2] == 1


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
    _apply_set_summary_constraints_on_dataset(df_lending_club, local_config_path, constraints)


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

    assert json_value["name"] == f"value {Op.Name(Op.IN_SET)} " + str(val_set)
    assert json_value["op"] == Op.Name(Op.IN_SET)
    assert json_value["valueSet"][0] == list(val_set)


def test_serialization_deserialization_values_in_set_constraint():
    val_set = {"abc", 1, 2}
    cvisc = columnValuesInSetConstraint(value_set=val_set)

    cvisc.from_protobuf(cvisc.to_protobuf())
    json_value = json.loads(message_to_json(cvisc.to_protobuf()))

    TEST_LOGGER.info(f"Serialize columnValuesInSetConstraint from deserialized representation:\n {cvisc.to_protobuf()}")

    assert json_value["name"] == f"value {Op.Name(Op.IN_SET)} " + str(val_set)
    assert json_value["op"] == Op.Name(Op.IN_SET)
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
            {"credit_card": "3852-000002-3237"},  # invalid
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
    print(report)
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
    print(report)
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
    print(report)
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
