from logging import getLogger
from typing import Any

import pandas as pd
import pytest

import whylogs as why
from whylogs.core.relations import Relation as Rel
from whylogs.core.relations import not_relation as not_rel
from whylogs.core.relations import relation as rel
from whylogs.core.relations import udf_relation as udf_rel
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.schema import DatasetSchema
from whylogs.core.validators import ConditionValidator

TEST_LOGGER = getLogger(__name__)
regex_conditions = {"noCreditCard": not_rel(rel(Rel.match, ".*4[0-9]{12}(?:[0-9]{3})?"))}

number_conditions = {
    "equals42": rel(Rel.equal, 42),
    "equals42.2": rel(Rel.equal, 42.2),
    "lessthan5": rel(Rel.less, 5),
    "morethan40": rel(Rel.greater, 40),
}


def do_something_important(validator_name, condition_name: str, value: Any):
    TEST_LOGGER.info(
        "Validator: {}\n    Condition name {} failed for value {}".format(validator_name, condition_name, value)
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


def is_even(x: Any, y: Any) -> bool:
    return x % 2 == 0


is_even_conditions = {
    "isEven": udf_rel(is_even),
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
    p_col = PreprocessedColumn.apply(transcriptions)

    credit_card_validator.columnar_validate(p_col)
    summary = credit_card_validator.to_summary_dict()
    assert summary["total_evaluations"] == 4
    assert summary["noCreditCard"] == 1


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
