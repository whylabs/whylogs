from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from whylogs.core.datatypes import (
    AnyType,
    Fractional,
    Integral,
    StandardTypeMapper,
    String,
)

NUMERICAL_TYPE = [int, bool, np.intc, np.uintc, np.int_, np.uint, np.longlong, np.ulonglong]
DOUBLE_TYPE = [float, np.double, np.longdouble, np.float16, np.float64, Decimal]
DATETIME_TYPE = [np.datetime64, np.timedelta64, pd.Timestamp, pd.Timedelta]
STRING_TYPE = [str, pd.CategoricalDtype()]


@pytest.mark.parametrize("expected_data_type", NUMERICAL_TYPE)
def test_integral_match_expected_data(expected_data_type) -> None:
    tpe = Integral()
    assert tpe.match(expected_data_type)


@pytest.mark.parametrize("unexpected_numerical", DOUBLE_TYPE)
def test_integral_does_not_match_unexpected_numerical(unexpected_numerical):
    tpe = Integral()
    assert not tpe.match(unexpected_numerical)


@pytest.mark.parametrize("unexpected_datetime", [*DATETIME_TYPE, *STRING_TYPE])
def test_integral_does_not_match_unexpected_datetime(unexpected_datetime):
    tpe = Integral()
    assert not tpe.match(unexpected_datetime)


@pytest.mark.parametrize("expected_data_type", DOUBLE_TYPE)
def test_fractional_match(expected_data_type) -> None:
    frac = Fractional()
    assert frac.match(expected_data_type)


@pytest.mark.parametrize("unexpected_numerical", NUMERICAL_TYPE)
def test_fractional_does_not_match_unexpected_numerical(unexpected_numerical):
    frac = Fractional()
    assert not frac.match(unexpected_numerical)


@pytest.mark.parametrize("unexpected_datetime", [*DATETIME_TYPE, *STRING_TYPE])
def test_fractional_does_not_match_unexpected_datetime(unexpected_datetime):
    frac = Fractional()
    assert not frac.match(unexpected_datetime)


@pytest.mark.parametrize("expected_data_type", STRING_TYPE)
def test_string_match(expected_data_type):
    string = String()
    assert string.match(expected_data_type)


@pytest.mark.parametrize("unexpected_numerical", [*NUMERICAL_TYPE, *DOUBLE_TYPE])
def test_string_does_not_match_unexpected_numerical(unexpected_numerical):
    string = String()
    assert not string.match(unexpected_numerical)


@pytest.mark.parametrize("unexpected_datetime", DATETIME_TYPE)
def test_string_does_not_match_unexpected_datetime(unexpected_datetime):
    string = String()
    assert not string.match(unexpected_datetime)


def test_type_mapper() -> None:
    class Foo:
        pass

    mapper = StandardTypeMapper()

    assert mapper(int).return_type == int
    assert mapper(np.int64).return_type == int
    assert mapper(np.int32).return_type == int
    assert mapper(float).return_type == float
    assert mapper(np.float64).return_type == float
    assert mapper(np.float32).return_type == float
    assert mapper(str).return_type == str
    assert isinstance(mapper(np.datetime64), AnyType)
    assert isinstance(mapper(Foo), AnyType)
