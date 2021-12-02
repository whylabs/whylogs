import json

import pytest

from whylogs.app.config import load_config
from whylogs.app.session import session_from_config
from whylogs.core.statistics.constraints import (
    DatasetConstraints,
    Op,
    StddevBetweenConstraint,
    SummaryConstraint,
    ValueConstraint,
    _summary_funcs1,
    _value_funcs,
)
from whylogs.proto import Op
from whylogs.util.protobuf import message_to_json


def test_value_summary_serialization():

    for each_op, _ in _value_funcs.items():

        value = ValueConstraint(each_op, 3.6)
        msg_value = value.to_protobuf()
        json_value = json.loads(message_to_json(msg_value))
        assert json_value["name"] == "value " + Op.Name(each_op) + " 3.6"
        assert pytest.approx(json_value["value"], 0.001) == 3.6
        assert json_value["op"] == Op.Name(each_op)
        assert json_value["verbose"] == False

    for each_op, _ in _summary_funcs1.items():
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


def test_stddev_between_constraint_serialization():
    stddev_between = StddevBetweenConstraint(2.3, 5.6, False)
    msg_value = stddev_between.to_protobuf()
    json_value = json.loads(message_to_json(msg_value))

    assert json_value["name"] == f"stddev between 2.3 and 5.6"

    constraints = json_value["constraints"]
    assert len(constraints) == 2
    first_constraint = constraints[0]
    second_constraint = constraints[1]

    assert first_constraint["name"] == "stddev>=min_value"
    assert pytest.approx(first_constraint["value"], 0.001) == 2.3
    assert first_constraint["op"] == Op.Name(Op.GE)
    assert first_constraint["verbose"] == False

    assert second_constraint["name"] == "stddev<=max_value"
    assert pytest.approx(second_constraint["value"], 0.001) == 5.6
    assert second_constraint["op"] == Op.Name(Op.LE)
    assert second_constraint["verbose"] == False


def test_stddev_between_constraint_invalid_initialization():
    with pytest.raises(ValueError):
        StddevBetweenConstraint(None, 5.6, False)

    with pytest.raises(ValueError):
        StddevBetweenConstraint(2.3, None, False)


# inheritance would be a better option than a function for implementing specific merging
def test_stddev_between_constraints_merge_different_values():
    stddev_1 = StddevBetweenConstraint(1.3, 6.7, False)
    stddev_2 = StddevBetweenConstraint(2.3, 7.8, True)
    with pytest.raises(AssertionError):
        stddev_1.merge(stddev_2)  # in SummaryConstraints merge, constraints.items() is forgotten


def test_stddev_between_constraints_merge_same_values():
    stddev_1 = StddevBetweenConstraint(1.3, 6.7, False)
    stddev_2 = StddevBetweenConstraint(1.3, 6.7, False)
    merged = stddev_1.merge(stddev_2)
    msg_value = merged.to_protobuf()
    json_value = json.loads(message_to_json(msg_value))

    first_constraint = json_value["constraints"][0]
    second_constraint = json_value["constraints"][1]

    assert first_constraint["name"] == "stddev>=min_value"
    assert first_constraint["firstField"] == "stddev"
    assert first_constraint["op"] == Op.Name(Op.GE)
    assert pytest.approx(first_constraint["value"], 0.001) == 1.3
    assert first_constraint["verbose"] == False

    assert second_constraint["name"] == "stddev<=max_value"
    assert second_constraint["firstField"] == "stddev"
    assert second_constraint["op"] == Op.Name(Op.LE)
    assert pytest.approx(second_constraint["value"], 0.001) == 6.7
    assert second_constraint["verbose"] == False


def test_stddev_report(df_lending_club, local_config_path):
    stddev_between = StddevBetweenConstraint(2.3, 3.4, False)
    non_negative = SummaryConstraint("mean", Op.GE, 0, None, "non negative mean", False)
    dc = DatasetConstraints(
        None, summary_constraints={"addr_state": [stddev_between, non_negative], "earliest_cr_line": [non_negative], "loan_amnt": [non_negative]}
    )

    report = dc.report()

    # checks there are constraints for 3 features
    assert len(report) == 3
    print(report)
    # check reporting for stddev greater or equal
    assert report[0][1][0][0] == (f"stddev between {2.3} and {3.4}", 0, 0)
    # check reporting for non-negative mean outside of container of stddev between constraints
    assert report[0][1][1] == ("non negative mean", 0, 0)


def test_stddev_summary_constraints_to_protobuf_from_dataset_constraints():
    stddev_between = StddevBetweenConstraint(2.3, 3.4, False)
    non_negative = SummaryConstraint("mean", Op.GE, 0, None, "non negative mean", False)

    dc = DatasetConstraints(
        None, summary_constraints={"addr_state": [stddev_between, non_negative], "earliest_cr_line": [non_negative], "loan_amnt": [non_negative]}
    )

    # check serialization and deserialization, should not fail
    dc.from_protobuf(dc.to_protobuf())

    msg_value = dc.to_protobuf()
    json_value = json.loads(message_to_json(msg_value))

    complex_constraints = json_value["summaryConstraints"]["addr_state"]["complexConstraints"][0]["constraints"]
    first_constraint = complex_constraints[0]
    second_constraint = complex_constraints[1]

    assert first_constraint["name"] == "stddev>=min_value"
    assert first_constraint["firstField"] == "stddev"
    assert first_constraint["op"] == Op.Name(Op.GE)
    assert pytest.approx(first_constraint["value"], 0.001) == 2.3
    assert first_constraint["verbose"] == False

    assert second_constraint["name"] == "stddev<=max_value"
    assert second_constraint["firstField"] == "stddev"
    assert second_constraint["op"] == Op.Name(Op.LE)
    assert pytest.approx(second_constraint["value"], 0.001) == 3.4
    assert second_constraint["verbose"] == False

    # issues reading from and writing to protobuf
    # since the implementation expects SummaryConstraintMsg but is provided SummaryConstraintMsgs
    # if we are going to use this implementation we need to check if the we have a SummaryConstraintMsg
    # or SummaryConstraintMsgs


def test_stddev_apply_constraints(df_lending_club, local_config_path):
    stddev_between = StddevBetweenConstraint(2.3, 3.4, False)
    non_negative_addr_state_mean = SummaryConstraint("mean", Op.GE, 0, None, "non negative address state mean", False)
    non_negative_loan_amnt = SummaryConstraint("min", Op.GE, 0, None)
    dc = DatasetConstraints(None, summary_constraints={"addr_state": [stddev_between, non_negative_addr_state_mean], "loan_amnt": [non_negative_loan_amnt]})

    config = load_config(local_config_path)
    session = session_from_config(config)

    profile = session.log_dataframe(df_lending_club, "test.data", constraints=dc)
    session.close()

    # apply constraints
    applied_constraints = profile.apply_summary_constraints()
    print(applied_constraints)

    assert applied_constraints[0][1][0][0][1] > 0  # check if constraints have been applied
    assert applied_constraints[0][1][0][0][2] > 0  # check if constraints have been applied
    assert applied_constraints[0][1][1][1] > 0  # check if constraints have been applied
