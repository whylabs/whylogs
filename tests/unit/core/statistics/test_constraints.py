import json

import pytest

from whylogs.app.config import load_config
from whylogs.app.session import session_from_config
from whylogs.core.statistics.constraints import (
    DatasetConstraints,
    Op,
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
