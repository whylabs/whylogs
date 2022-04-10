import numpy as np
import pandas as pd

from whylogs_v1.core.datatypes import (
    AnyType,
    Fractional,
    Integral,
    StandardTypeMapper,
    String,
)


def test_integral_match() -> None:
    tpe = Integral()
    assert tpe.match(int)
    assert tpe.match(bool)
    assert tpe.match(int)
    assert tpe.match(np.intc)
    assert tpe.match(np.uintc)
    assert tpe.match(np.int_)
    assert tpe.match(np.uint)
    assert tpe.match(np.longlong)
    assert tpe.match(np.ulonglong)

    assert not tpe.match(float)
    assert not tpe.match(np.double)
    assert not tpe.match(np.longdouble)
    assert not tpe.match(np.float16)
    assert not tpe.match(np.float64)

    # numpy date time
    assert not tpe.match(np.datetime64)
    assert not tpe.match(np.timedelta64)

    # pandas datetime
    assert not tpe.match(pd.Timestamp)
    assert not tpe.match(pd.Timedelta)
    assert not tpe.match(str)


def test_fractional_match() -> None:
    frac = Fractional()
    assert frac.match(float)
    assert frac.match(np.double)
    assert frac.match(np.longdouble)
    assert frac.match(np.float16)
    assert frac.match(np.float64)

    assert not frac.match(int)
    assert not frac.match(bool)
    assert not frac.match(int)
    assert not frac.match(np.intc)
    assert not frac.match(np.uintc)
    assert not frac.match(np.int_)
    assert not frac.match(np.uint)
    assert not frac.match(np.longlong)
    assert not frac.match(np.ulonglong)

    # numpy date time
    assert not frac.match(np.datetime64)
    assert not frac.match(np.timedelta64)

    # pandas datetime
    assert not frac.match(pd.Timestamp)
    assert not frac.match(pd.Timedelta)
    assert not frac.match(str)


def test_string_match() -> None:
    string = String()
    # pandas
    assert string.match(pd.CategoricalDtype())
    assert string.match(str)

    assert not string.match(pd.Timestamp)
    assert not string.match(pd.Timedelta)

    assert not string.match(float)
    assert not string.match(np.double)
    assert not string.match(np.longdouble)
    assert not string.match(np.float16)
    assert not string.match(np.float64)

    assert not string.match(int)
    assert not string.match(bool)
    assert not string.match(int)
    assert not string.match(np.intc)
    assert not string.match(np.uintc)
    assert not string.match(np.int_)
    assert not string.match(np.uint)
    assert not string.match(np.longlong)
    assert not string.match(np.ulonglong)

    # numpy date time
    assert not string.match(np.datetime64)
    assert not string.match(np.timedelta64)


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
