import json
from logging import getLogger

import numpy as np
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
    columnKLDivergenceLessThanConstraint,
    columnValuesInSetConstraint,
    maxBetweenConstraint,
    meanBetweenConstraint,
    minBetweenConstraint,
    parametrizedKSTestPValueGreaterThanConstraint,
    stddevBetweenConstraint,
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
        if each_op == Op.BTWN:
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
    assert json_value["name"] == f"summary ks_test p-value {Op.Name(Op.GT)} 0.05"
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

    assert json_value["name"] == f"summary ks_test p-value {Op.Name(Op.GT)} 0.15"
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
    kld = columnKLDivergenceLessThanConstraint(dist_data, threshold=0.5)
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
    dist_data = np.random.choice(list(set(df_lending_club["grade"].values)), 100, p=[0.05, 0.05, 0.21, 0.3, 0.19, 0.2])
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
    assert json_value["name"] == f"summary kl_divergence threshold {Op.Name(Op.LT)} 0.5"
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

    assert json_value["name"] == f"summary kl_divergence threshold {Op.Name(Op.LT)} 0.15"
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

    assert json_value["name"] == f"summary kl_divergence threshold {Op.Name(Op.LT)} 0.15"
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
