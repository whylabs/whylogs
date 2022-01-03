import json

import pandas as pd
import pytest

from whylogs.app.config import load_config
from whylogs.app.session import session_from_config
from whylogs.core.statistics.constraints import (
    DatasetConstraints,
    Op,
    SummaryConstraint,
    ValueConstraint,
    _matches_json_schema,
    _summary_funcs1,
    _try_parse_dateutil,
    _try_parse_json,
    _try_parse_strftime_format,
    _value_funcs,
    columnExistsConstraint,
    columnPairValuesInSetConstraint,
    columnsMatchSetConstraint,
    columnValuesInSetConstraint,
    containsCreditCardConstraint,
    containsEmailConstraint,
    dateUtilParseableConstraint,
    jsonParseableConstraint,
    matchesJsonSchemaConstraint,
    maxBetweenConstraint,
    meanBetweenConstraint,
    minBetweenConstraint,
    numberOfRowsConstraint,
    stddevBetweenConstraint,
    strftimeFormatConstraint,
    stringLengthBetweenConstraint,
    stringLengthEqualConstraint,
)
from whylogs.proto import Op
from whylogs.util.protobuf import message_to_json


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
        assert json_value["verbose"] == False

    for each_op, _ in _summary_funcs1.items():
        if each_op in (Op.BTWN, Op.IN_SET, Op.CONTAINS_SET, Op.EQ_SET, Op.CONTAIN):
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


def _apply_set_summary_constraints_on_dataset(df_lending_club, local_config_path, constraints):

    dc = DatasetConstraints(None, summary_constraints={"annual_inc": constraints})
    config = load_config(local_config_path)
    session = session_from_config(config)
    profile = session.log_dataframe(df_lending_club, "test.data", constraints=dc)
    session.close()
    report = profile.apply_summary_constraints()

    assert len(report) == 1

    # make sure it checked every value
    for each_feat in report:
        for each_constraint in each_feat[1]:
            assert each_constraint[1] == 1
            if "True" in each_constraint[0]:
                assert each_constraint[2] == 0
            else:
                assert each_constraint[2] == 1


def test_column_values_in_set_constraint(df_lending_club, local_config_path):
    cvisc = columnValuesInSetConstraint(value_set={2, 5, 8})
    ltc = ValueConstraint(Op.LT, 1)
    dc = DatasetConstraints(None, value_constraints={"annual_inc": [cvisc, ltc]})
    config = load_config(local_config_path)
    session = session_from_config(config)
    profile = session.log_dataframe(df_lending_club, "test.data", constraints=dc)
    session.close()
    report = dc.report()

    assert report[0][1][0][1] == len(df_lending_club)
    assert report[0][1][1][1] == len(df_lending_club)


def test_set_summary_constraints(df_lending_club, local_config_path):

    org_list = list(df_lending_club["annual_inc"])

    org_list2 = list(df_lending_club["annual_inc"])
    org_list2.extend([1, 4, 5555, "gfsdgs", 0.00333, 245.32])

    in_set = SummaryConstraint("distinct_column_values", Op.IN_SET, reference_set=org_list2, name="True")
    in_set2 = SummaryConstraint("distinct_column_values", Op.IN_SET, reference_set=org_list, name="True2")
    in_set3 = SummaryConstraint("distinct_column_values", Op.IN_SET, reference_set=org_list[:-1], name="False")

    eq_set = SummaryConstraint("distinct_column_values", Op.EQ_SET, reference_set=org_list, name="True3")
    eq_set2 = SummaryConstraint("distinct_column_values", Op.EQ_SET, reference_set=org_list2, name="False2")
    eq_set3 = SummaryConstraint("distinct_column_values", Op.EQ_SET, reference_set=org_list[:-1], name="False3")

    contains_set = SummaryConstraint("distinct_column_values", Op.CONTAINS_SET, reference_set=[org_list[2]], name="True4")
    contains_set2 = SummaryConstraint("distinct_column_values", Op.CONTAINS_SET, reference_set=org_list, name="True5")
    contains_set3 = SummaryConstraint("distinct_column_values", Op.CONTAINS_SET, reference_set=org_list[:-1], name="True6")
    contains_set4 = SummaryConstraint("distinct_column_values", Op.CONTAINS_SET, reference_set=[str(org_list[2])], name="False4")
    contains_set5 = SummaryConstraint("distinct_column_values", Op.CONTAINS_SET, reference_set=[2.3456], name="False5")
    contains_set6 = SummaryConstraint("distinct_column_values", Op.CONTAINS_SET, reference_set=org_list2, name="False6")

    list(df_lending_club["annual_inc"])
    constraints = [in_set, in_set2, in_set3, eq_set, eq_set2, eq_set3, contains_set, contains_set2, contains_set3, contains_set4, contains_set5, contains_set6]
    _apply_set_summary_constraints_on_dataset(df_lending_club, local_config_path, constraints)


def test_set_summary_constraint_invalid_init():
    with pytest.raises(TypeError):
        SummaryConstraint("distinct_column_values", Op.CONTAINS_SET, reference_set=1)
    with pytest.raises(ValueError):
        SummaryConstraint("distinct_column_values", Op.CONTAINS_SET, 1)
    with pytest.raises(ValueError):
        SummaryConstraint("distinct_column_values", Op.CONTAINS_SET, second_field="aaa")
    with pytest.raises(ValueError):
        SummaryConstraint("distinct_column_values", Op.CONTAINS_SET, third_field="aaa")
    with pytest.raises(ValueError):
        SummaryConstraint("distinct_column_values", Op.CONTAINS_SET, upper_value=2)


def test_set_summary_no_merge_different_set():

    set_c_1 = SummaryConstraint("distinct_column_values", Op.CONTAINS_SET, reference_set=[1, 2, 3])
    set_c_2 = SummaryConstraint("distinct_column_values", Op.CONTAINS_SET, reference_set=[2, 3, 4, 5])
    with pytest.raises(AssertionError):
        set_c_1.merge(set_c_2)


def test_set_summary_merge():
    set_c_1 = SummaryConstraint("distinct_column_values", Op.CONTAINS_SET, reference_set=[1, 2, 3])
    set_c_2 = SummaryConstraint("distinct_column_values", Op.CONTAINS_SET, reference_set=[1, 2, 3])

    merged = set_c_1.merge(set_c_2)

    pre_merge_json = json.loads(message_to_json(set_c_1.to_protobuf()))
    merge_json = json.loads(message_to_json(merged.to_protobuf()))

    assert pre_merge_json["name"] == merge_json["name"]
    assert pre_merge_json["referenceSet"] == merge_json["referenceSet"]
    assert pre_merge_json["firstField"] == merge_json["firstField"]
    assert pre_merge_json["op"] == merge_json["op"]
    assert pre_merge_json["verbose"] == merge_json["verbose"]


def test_merge_values_in_set_constraint_different_value_set():
    cvisc1 = columnValuesInSetConstraint(value_set={1, 2, 3})
    cvisc2 = columnValuesInSetConstraint(value_set={3, 4, 5})
    with pytest.raises(AssertionError):
        cvisc1.merge(cvisc2)


def test_merge_values_in_set_constraint_same_value_set():
    cvisc1 = columnValuesInSetConstraint(value_set={"abc", "b", "c"})
    cvisc2 = columnValuesInSetConstraint(value_set={"abc", "b", "c"})
    merged = cvisc1.merge(cvisc2)
    print(merged.report())


def test_serialization_deserialization_values_in_set_constraint():
    val_set = {"abc", 1, 2}
    cvisc = columnValuesInSetConstraint(value_set=val_set)

    cvisc.from_protobuf(cvisc.to_protobuf())
    json_value = json.loads(message_to_json(cvisc.to_protobuf()))

    assert json_value["name"] == "value IN " + str(val_set)
    assert json_value["op"] == Op.Name(Op.IN)
    assert json_value["valueSet"][0] == list(val_set)


def test_column_values_in_set_wrong_datatype():
    with pytest.raises(TypeError):
        cvisc = columnValuesInSetConstraint(value_set=1)


def _report_email_value_constraint_on_data_set(local_config_path, pattern=None):
    df = pd.DataFrame(
        [
            {"email": r"abc's@gmail.com"},
            {"email": r'"avrrr test \@"@gmail.com'},
            {"email": r"abc..q12@example.us"},
            {"email": r'"sdsss\d"@gmail.com'},
            {"email": r"customer/department=shipping?@example-another.some-other.us"},
            {"email": r".should_fail@yahoo.com"},
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

    assert report[0][1][0][1] == 6
    assert report[0][1][0][2] == 2


def test_email_constraint_supply_regex_pattern(local_config_path):
    report = _report_email_value_constraint_on_data_set(local_config_path, r"\S+@\S+")
    assert report[0][1][0][0] == rf"value {Op.Name(Op.MATCH)} \S+@\S+"
    assert report[0][1][0][1] == 6
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
    assert report[0][1][0][2] == 6


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

    json_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "years": {"type": "integer"},
        },
        "required": ["name", "abc"],
    }
    apply1 = matchesJsonSchemaConstraint(json_schema)
    apply2 = ValueConstraint(Op.APPLY_FUNC, apply_function=lambda x: x)
    apply3 = ValueConstraint(Op.APPLY_FUNC, apply_function=_matches_json_schema)

    with pytest.raises(AssertionError):
        apply1.merge(apply2)
    with pytest.raises(TypeError):
        apply1.merge(apply3)

    apply4 = ValueConstraint(Op.APPLY_FUNC, json_schema, apply_function=_matches_json_schema)

    merged = apply1.merge(apply4)

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

    profile = session.log_dataframe(df_lending_club, "test.data", constraints=dc)

    report = profile.apply_table_shape_constraints()

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
    if len(set2) > 20:
        tmp_set = set(list(set2)[:20])
        reference_set_str = f"{str(tmp_set)[:-1]}, ...}}"
    else:
        reference_set_str = str(set2)
    assert report[5][0] == f"table columns {Op.Name(Op.EQ)} {reference_set_str}"
    assert report[5][1] == 1
    assert report[5][2] == 0


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

        tmp_val_constraints = dict()
        tmp_val_constraints.update(constraints.raw_value_constraints)
        tmp_val_constraints.update(constraints.coerced_type_constraints)

        tmp_val_constraints_deser = dict()
        tmp_val_constraints_deser.update(deser_constraints.raw_value_constraints)
        tmp_val_constraints_deser.update(deser_constraints.coerced_type_constraints)

        for (name, c), (deser_name, deser_c) in zip(tmp_val_constraints.items(), tmp_val_constraints_deser.items()):
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


def test_column_pair_values_in_valid_set_constraint_apply(local_config_path, df_lending_club):
    val_set = {("B", "B2"), ("C", "C2"), ("D", "D1")}  # the second pair is found in the data set
    cpvis = columnPairValuesInSetConstraint(column_A="grade", column_B="sub_grade", value_set=val_set)

    dc = DatasetConstraints(None, multi_column_value_constraints=[cpvis])
    config = load_config(local_config_path)
    session = session_from_config(config)
    profile = session.log_dataframe(df_lending_club, "test.data", constraints=dc)
    session.close()
    report = dc.report()

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
    assert json_value["dependentColumns"][0] == list(col_set)
    assert json_value["op"] == Op.Name(Op.IN)
    assert json_value["valueSet"] == [[list(t)] for t in val_set1]
    assert json_value["verbose"] is False

    report = cpvis_merged.report()
    assert report[1] == 8
    assert report[2] == 3


def test_serialization_deserialization_column_pair_values_in_valid_set_constraint():
    val_set1 = [(12345, "B"), (41000.0, "C"), (42333, "D")]
    c = columnPairValuesInSetConstraint(column_A="A", column_B="B", value_set=val_set1, verbose=True)

    c.from_protobuf(c.to_protobuf())
    json_value = json.loads(message_to_json(c.to_protobuf()))

    val_set1 = set(val_set1)
    col_set = ["A", "B"]
    assert json_value["name"] == f"multi column value {col_set} {Op.Name(Op.IN)} {val_set1}"
    assert json_value["dependentColumns"][0] == list(col_set)
    assert json_value["op"] == Op.Name(Op.IN)
    assert json_value["valueSet"] == [[list(t)] for t in val_set1]
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
