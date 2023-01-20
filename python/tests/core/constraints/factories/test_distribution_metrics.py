import numpy as np
import pandas as pd
import pytest

import whylogs as why
from whylogs.core.constraints import ConstraintsBuilder
from whylogs.core.constraints.factories.distribution_metrics import (
    greater_than_number,
    is_in_range,
    is_non_negative,
    mean_between_range,
    quantile_between_range,
    smaller_than_number,
    stddev_between_range,
)


@pytest.fixture
def empty_builder():
    data = {
        "a": [],
    }
    df = pd.DataFrame(data)
    profile_view = why.log(pandas=df).view()
    empty_builder = ConstraintsBuilder(dataset_profile_view=profile_view)
    return empty_builder


def test_is_in_range(builder, nan_builder):
    builder.add_constraint(is_in_range(column_name="weight", lower=1.1, upper=3.2, skip_missing=False))
    builder.add_constraint(is_in_range(column_name="legs", lower=0, upper=6, skip_missing=False))
    builder.add_constraint(is_in_range(column_name="animal", lower=0, upper=6, skip_missing=False))

    constraint = builder.build()
    report = constraint.generate_constraints_report()
    assert report[0].name == "weight is in range [1.1,3.2]" and report[0].passed == 0
    assert report[1].name == "legs is in range [0,6]" and report[1].passed == 1
    assert report[2].name == "animal is in range [0,6]" and report[2].passed == 0

    nan_builder.add_constraint(is_in_range(column_name="a", lower=1.1, upper=3.2, skip_missing=True))
    constraint = nan_builder.build()
    report = constraint.generate_constraints_report()
    assert report[0].name == "a is in range [1.1,3.2]" and report[0].passed == 1


@pytest.mark.parametrize(
    "column,passed,failed,skip",
    [
        ("weight", 0, 1, True),
        ("legs", 1, 0, True),
        ("animal", 0, 1, False),
        ("null_column", 1, 0, True),
        ("non_existing_column", None, None, False),
    ],
)
def test_is_non_negative(column, passed, failed, skip):
    data = {
        "animal": ["cat", "hawk", "snake", "cat", "mosquito"],
        "legs": [4, 2, 0, 4, 6],
        "weight": [4.3, -1.8, 1.3, 4.1, 5.5e-6],
        "null_column": [np.nan] * 5,
    }

    view = why.log(pd.DataFrame(data)).profile().view()
    builder = ConstraintsBuilder(dataset_profile_view=view)
    if column == "non_existing_column":
        with pytest.raises(ValueError, match="non_existing_column was not found in set of this profile"):
            builder.add_constraint(is_non_negative(column_name=column))
    else:
        builder.add_constraint(is_non_negative(column_name=column, skip_missing=skip))
        constraint = builder.build()
        report = constraint.generate_constraints_report()
        assert (
            report[0].name == f"{column} is non negative" and report[0].passed == passed and report[0].failed == failed
        )


def test_greater_than_number(builder, nan_builder, empty_builder):
    builder.add_constraint(greater_than_number(column_name="weight", number=0.0, skip_missing=False))
    builder.add_constraint(greater_than_number(column_name="legs", number=6, skip_missing=False))
    builder.add_constraint(greater_than_number(column_name="animal", number=0.0, skip_missing=True))
    builder.add_constraint(greater_than_number(column_name="animal", number=0.5, skip_missing=False))
    constraint = builder.build()
    assert constraint.validate() is False
    # ReportResult(name, passed, failed, summary)
    assert constraint.generate_constraints_report() == [
        ("weight greater than number 0.0", 1, 0, None),
        ("legs greater than number 6", 0, 1, None),
        ("animal greater than number 0.0", 1, 0, None),
        ("animal greater than number 0.5", 0, 1, None),
    ]
    for x, y in zip(constraint.report(), constraint.generate_constraints_report()):
        assert (x[0], x[1], x[2]) == (y[0], y[1], y[2])

    nan_builder.add_constraint(greater_than_number(column_name="a", number=2.2, skip_missing=False))
    constraint = nan_builder.build()
    assert not constraint.validate()
    empty_builder.add_constraint(greater_than_number(column_name="a", number=2.2, skip_missing=False))
    constraint = empty_builder.build()
    assert not constraint.validate()


def test_smaller_than_number(builder, nan_builder, empty_builder):
    builder.add_constraint(smaller_than_number(column_name="legs", number=6))
    builder.add_constraint(smaller_than_number(column_name="legs", number=3.5))
    builder.add_constraint(smaller_than_number(column_name="animal", number=0.0, skip_missing=True))
    builder.add_constraint(smaller_than_number(column_name="animal", number=0.5, skip_missing=False))
    constraint = builder.build()
    # ReportResult(name, passed, failed, summary)
    assert constraint.generate_constraints_report() == [
        ("legs smaller than number 6", 1, 0, None),
        ("legs smaller than number 3.5", 0, 1, None),
        ("animal smaller than number 0.0", 1, 0, None),
        ("animal smaller than number 0.5", 0, 1, None),
    ]
    for x, y in zip(constraint.report(), constraint.generate_constraints_report()):
        assert (x[0], x[1], x[2]) == (y[0], y[1], y[2])

    nan_builder.add_constraint(smaller_than_number(column_name="a", number=2.2, skip_missing=False))
    constraint = nan_builder.build()
    assert not constraint.validate()
    empty_builder.add_constraint(smaller_than_number(column_name="a", number=2.2, skip_missing=True))
    constraint = empty_builder.build()
    assert constraint.validate()


def test_mean_between_range(builder, nan_builder):
    builder.add_constraint(mean_between_range(column_name="legs", lower=0, upper=20.0))
    builder.add_constraint(mean_between_range(column_name="legs", lower=0.0, upper=1.0))
    builder.add_constraint(mean_between_range(column_name="animal", lower=0.0, upper=1.0, skip_missing=True))
    builder.add_constraint(mean_between_range(column_name="animal", lower=0.0, upper=1.3, skip_missing=False))
    constraint = builder.build()
    # ReportResult(name, passed, failed, summary)
    assert constraint.generate_constraints_report() == [
        ("legs mean between 0 and 20.0 (inclusive)", 1, 0, None),
        ("legs mean between 0.0 and 1.0 (inclusive)", 0, 1, None),
        ("animal mean between 0.0 and 1.0 (inclusive)", 1, 0, None),
        ("animal mean between 0.0 and 1.3 (inclusive)", 0, 1, None),
    ]
    for x, y in zip(constraint.report(), constraint.generate_constraints_report()):
        assert (x[0], x[1], x[2]) == (y[0], y[1], y[2])

    nan_builder.add_constraint(mean_between_range(column_name="a", lower=0.0, upper=1.3, skip_missing=False))
    constraint = nan_builder.build()
    assert not constraint.validate()


def test_stddev_between_range(builder, nan_builder):
    builder.add_constraint(stddev_between_range(column_name="legs", lower=0.0, upper=10.0))
    builder.add_constraint(stddev_between_range(column_name="legs", lower=0.0, upper=0.5))
    builder.add_constraint(stddev_between_range(column_name="animal", lower=0.0, upper=1.0, skip_missing=True))
    builder.add_constraint(stddev_between_range(column_name="animal", lower=0.0, upper=1.3, skip_missing=False))
    constraint = builder.build()
    assert not constraint.validate()
    # ReportResult(name, passed, failed, summary)
    assert constraint.generate_constraints_report() == [
        ("legs standard deviation between 0.0 and 10.0 (inclusive)", 1, 0, None),
        ("legs standard deviation between 0.0 and 0.5 (inclusive)", 0, 1, None),
        ("animal standard deviation between 0.0 and 1.0 (inclusive)", 1, 0, None),
        ("animal standard deviation between 0.0 and 1.3 (inclusive)", 0, 1, None),
    ]
    for x, y in zip(constraint.report(), constraint.generate_constraints_report()):
        assert (x[0], x[1], x[2]) == (y[0], y[1], y[2])

    nan_builder.add_constraint(stddev_between_range(column_name="a", lower=0.0, upper=1.3, skip_missing=False))
    constraint = nan_builder.build()
    assert not constraint.validate()


def test_quantile_between_range(builder, nan_builder):
    builder.add_constraint(
        quantile_between_range(column_name="legs", quantile=0.9, lower=0.0, upper=4.0, skip_missing=False)
    )
    builder.add_constraint(
        quantile_between_range(column_name="legs", quantile=0.5, lower=0.0, upper=1.2, skip_missing=False)
    )
    builder.add_constraint(
        quantile_between_range(column_name="animal", quantile=0.5, lower=0.0, upper=5.2, skip_missing=True)
    )
    builder.add_constraint(
        quantile_between_range(column_name="animal", quantile=0.5, lower=0.0, upper=5.0, skip_missing=False)
    )

    constraint = builder.build()
    # ReportResult(name, passed, failed, summary)
    assert constraint.generate_constraints_report() == [
        ("legs 0.9-th quantile value between 0.0 and 4.0 (inclusive)", 1, 0, None),
        ("legs 0.5-th quantile value between 0.0 and 1.2 (inclusive)", 0, 1, None),
        ("animal 0.5-th quantile value between 0.0 and 5.2 (inclusive)", 1, 0, None),
        ("animal 0.5-th quantile value between 0.0 and 5.0 (inclusive)", 0, 1, None),
    ]
    for x, y in zip(constraint.report(), constraint.generate_constraints_report()):
        assert (x[0], x[1], x[2]) == (y[0], y[1], y[2])

    nan_builder.add_constraint(
        quantile_between_range(column_name="a", lower=0.0, upper=1.3, quantile=0.5, skip_missing=False)
    )
    constraint = nan_builder.build()
    assert not constraint.validate()
