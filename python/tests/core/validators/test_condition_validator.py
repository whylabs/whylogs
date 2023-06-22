from logging import getLogger
from typing import Any

import numpy as np
import pandas as pd
import pytest

import whylogs as why
from whylogs.core.metrics import MetricConfig
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.relations import Not, Predicate
from whylogs.core.schema import DatasetSchema
from whylogs.core.validators import ConditionValidator

X = Predicate()

TEST_LOGGER = getLogger(__name__)
regex_conditions = {
    "noCreditCard": Not(X.matches(".*4[0-9]{12}(?:[0-9]{3})?"))
}  # or Not(X.matches(".*4[0-9]{12}(?:[0-9]{3})?"))


number_conditions = {
    "equals42": X.equals(42),
    "equals42.2": X.equals(42.2),
    "lessthan5": X.less_than(5),
    "morethan40": X.greater_than(40),
}


def do_something_important(validator_name, condition_name: str, value: Any, row_id: Any = None):
    TEST_LOGGER.info(
        "Validator: {}\n    Condition name {} failed for value {} and row_id {}".format(
            validator_name, condition_name, value, row_id
        )
    )
    return


def do_another_important_thing(validator_name, condition_name: str, value: Any):
    TEST_LOGGER.info(
        "Validator: {}\n    Condition name {} failed for value {}".format(validator_name, condition_name, value)
    )
    return


@pytest.fixture
def credit_card_validator():
    return ConditionValidator(
        name="transcription_doesnt_contain_credit_card",
        conditions=regex_conditions,
        actions=[do_something_important],
    )


def number_validator():
    return ConditionValidator(
        name="number_equalities",
        conditions=number_conditions,
        actions=[do_something_important, do_another_important_thing],
    )


def even(x: Any) -> bool:
    return x % 2 == 0


is_even_conditions = {
    "isEven": X.is_(even),
}


def is_even_validator():
    return ConditionValidator(
        name="is_even",
        conditions=is_even_conditions,
        actions=[do_something_important],
    )


@pytest.fixture
def transcriptions():
    return [
        "Bob's credit card number is 4000000000000",
        "Alice's credit card is XXXXXXXXXXXXX",
        "Hi, my name is Bob",
        "Hi, I'm Alice",
    ]


@pytest.fixture
def numbers():
    return {"ints": [1, 12, 42, 4], "floats": [1.2, 12.3, 42.2, 5.6]}


def test_condition_validator(credit_card_validator, transcriptions) -> None:
    for data in [transcriptions, np.array(transcriptions), pd.Series(transcriptions)]:
        col = PreprocessedColumn.apply(data)
        credit_card_validator = ConditionValidator(
            name="transcription_doesnt_contain_credit_card",
            conditions=regex_conditions,
            actions=[do_something_important],
        )
        credit_card_validator.columnar_validate(col)
        summary = credit_card_validator.to_summary_dict()
        assert summary["total_evaluations"] == 4
        assert summary["noCreditCard"] == 1


def test_row_condition_validator(credit_card_validator, transcriptions) -> None:
    transcription = transcriptions[0]
    col = PreprocessedColumn._process_scalar_value(transcription)
    credit_card_validator = ConditionValidator(
        name="transcription_doesnt_contain_credit_card",
        conditions=regex_conditions,
        actions=[do_something_important],
    )
    credit_card_validator.columnar_validate(col)
    summary = credit_card_validator.to_summary_dict()
    assert summary["total_evaluations"] == 1
    assert summary["noCreditCard"] == 1


@pytest.mark.parametrize("identity,sampling", [(True, True), (False, True), (True, False), (False, False)])
def test_condition_validator_with_row_ids(identity, sampling) -> None:
    if identity:
        condition_count_config = MetricConfig(identity_column="ids")
    else:
        condition_count_config = MetricConfig()
    data = {"int_col": [f"c{x}" for x in range(100)], "ids": [f"i{x}" for x in range(100)]}
    df = pd.DataFrame(data=data)
    X = Predicate()
    iseven_conditions = {"iseven": X.matches(r".*\d*[02468]$")}

    iseven_validator = ConditionValidator(
        name="int_col",
        conditions=iseven_conditions,
        actions=[do_something_important],
        enable_sampling=sampling,
        sample_size=7,
    )

    validators = {"int_col": [iseven_validator]}
    schema = DatasetSchema(validators=validators, default_configs=condition_count_config)
    why.log(df, schema=schema)

    summary = iseven_validator.to_summary_dict()
    assert summary["total_evaluations"] == 100
    assert summary["iseven"] == 50

    if not sampling:
        with pytest.raises(ValueError, match="Sampling is not enabled for this validator"):
            iseven_validator.get_samples()
    else:
        samples = iseven_validator.get_samples()
        assert len(samples) == 7
        assert [x[-1] not in ["0", "2", "4", "6", "8"] for x in samples]
        if identity:
            assert samples[0][0] == "i"
        else:
            assert samples[0][0] == "c"  # if identity is not set, sample the validated column


def test_condition_validator_dataframe(credit_card_validator, transcriptions):
    df = pd.DataFrame(data=transcriptions, columns=["transcriptions"])
    validators = {"transcriptions": [credit_card_validator]}

    schema = DatasetSchema(validators=validators)
    profile = why.log(df, schema=schema).profile()

    assert profile._columns["transcriptions"]._column_validators[0].total == 4
    assert profile._columns["transcriptions"]._column_validators[0].failures["noCreditCard"] == 1
    summary = profile._columns["transcriptions"]._column_validators[0].to_summary_dict()
    assert summary["total_evaluations"] == 4
    assert summary["noCreditCard"] == 1


def test_number_conditions(numbers):
    df = pd.DataFrame(data=numbers)
    validators = {"ints": [number_validator(), is_even_validator()], "floats": [number_validator()]}
    schema = DatasetSchema(validators=validators)
    profile = why.log(df, schema=schema).profile()
    assert profile._columns["ints"]._column_validators[0].name == "number_equalities"
    assert profile._columns["ints"]._column_validators[1].name == "is_even"
    assert profile._columns["ints"]._column_validators[0].total == 4
    assert profile._columns["floats"]._column_validators[0].total == 4
    assert profile._columns["floats"]._column_validators[0].failures["equals42.2"] == 3
    assert profile._columns["ints"]._column_validators[0].failures["equals42.2"] == 4
    assert profile._columns["floats"]._column_validators[0].failures["equals42"] == 4
    assert profile._columns["ints"]._column_validators[0].failures["equals42"] == 3
    assert profile._columns["ints"]._column_validators[1].failures["isEven"] == 1
