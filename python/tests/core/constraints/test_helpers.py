import pytest

from whylogs.core.constraints import (
    ConstraintsBuilder,
    count_below_number,
    distinct_number_in_range,
    frequent_strings_in_reference_set,
    greater_than_number,
    mean_between_range,
    n_most_common_items_in_set,
    null_percentage_below_number,
    null_values_below_number,
    quantile_between_range,
    smaller_than_number,
    stddev_between_range,
)


@pytest.fixture
def builder(profile_view):
    builder = ConstraintsBuilder(dataset_profile_view=profile_view)
    return builder


def test_greater_than_number(builder):
    builder.add_constraint(greater_than_number(column_name="legs", number=0.0))
    constraint = builder.build()
    assert constraint.validate()
    assert constraint.report() == [("legs greater than number 0.0", 1, 0)]

    builder.add_constraint(greater_than_number(column_name="legs", number=6))
    constraint = builder.build()
    assert not constraint.validate()
    assert constraint.report() == [("legs greater than number 0.0", 1, 0), ("legs greater than number 6", 0, 1)]


def test_smaller_than_number(builder):
    builder.add_constraint(smaller_than_number(column_name="legs", number=6))
    constraint = builder.build()
    assert constraint.validate()
    assert constraint.report() == [("legs smaller than number 6", 1, 0)]

    builder.add_constraint(smaller_than_number(column_name="legs", number=3.5))
    constraint = builder.build()
    assert not constraint.validate()
    assert constraint.report() == [("legs smaller than number 6", 1, 0), ("legs smaller than number 3.5", 0, 1)]


def test_mean_between_range(builder):
    builder.add_constraint(mean_between_range(column_name="legs", lower=0, upper=20.0))
    constraint = builder.build()
    assert constraint.validate()
    assert constraint.report() == [("legs mean between 0 and 20.0 (inclusive)", 1, 0)]

    builder.add_constraint(mean_between_range(column_name="legs", lower=0.0, upper=1.0))
    constraint = builder.build()
    assert not constraint.validate()
    assert constraint.report() == [
        ("legs mean between 0 and 20.0 (inclusive)", 1, 0),
        ("legs mean between 0.0 and 1.0 (inclusive)", 0, 1),
    ]


def test_stddev_between_range(builder):
    builder.add_constraint(stddev_between_range(column_name="legs", lower=0.0, upper=10.0))
    constraint = builder.build()
    assert constraint.validate()
    assert constraint.report() == [("legs standard deviation between 0.0 and 10.0 (inclusive)", 1, 0)]

    builder.add_constraint(stddev_between_range(column_name="legs", lower=0.0, upper=0.5))
    constraint = builder.build()
    assert not constraint.validate()
    assert constraint.report() == [
        ("legs standard deviation between 0.0 and 10.0 (inclusive)", 1, 0),
        ("legs standard deviation between 0.0 and 0.5 (inclusive)", 0, 1),
    ]


def test_quantile_between_range(builder):
    builder.add_constraint(
        quantile_between_range(column_name="legs", quantile=0.9, lower=0.0, upper=4.0, skip_missing=True)
    )
    constraint = builder.build()
    assert constraint.validate()
    assert constraint.report() == [("legs 0.9-th quantile value between 0.0 and 4.0 (inclusive)", 1, 0)]

    builder.add_constraint(
        quantile_between_range(column_name="legs", quantile=0.5, lower=0.0, upper=1.2, skip_missing=True)
    )
    constraint = builder.build()
    assert not constraint.validate()
    assert constraint.report() == [
        ("legs 0.9-th quantile value between 0.0 and 4.0 (inclusive)", 1, 0),
        ("legs 0.5-th quantile value between 0.0 and 1.2 (inclusive)", 0, 1),
    ]

    builder.add_constraint(
        quantile_between_range(column_name="animal", quantile=0.5, lower=0.0, upper=5.0, skip_missing=False)
    )
    constraint = builder.build()
    assert not constraint.validate()
    assert constraint.report() == [
        ("legs 0.9-th quantile value between 0.0 and 4.0 (inclusive)", 1, 0),
        ("legs 0.5-th quantile value between 0.0 and 1.2 (inclusive)", 0, 1),
        ("animal 0.5-th quantile value between 0.0 and 5.0 (inclusive)", 0, 1),
    ]


def test_quantile_between_range_fails_on_missing_metrics(builder):
    pass


def test_count_below_number(builder):
    builder.add_constraint(count_below_number(column_name="legs", number=10))
    constraint = builder.build()
    assert constraint.validate()
    assert constraint.report() == [("count of legs lower than 10", 1, 0)]

    builder.add_constraint(count_below_number(column_name="legs", number=1))
    constraint = builder.build()
    assert not constraint.validate()
    assert constraint.report() == [("count of legs lower than 10", 1, 0), ("count of legs lower than 1", 0, 1)]


def test_null_values_below_number(builder):
    builder.add_constraint(null_values_below_number(column_name="legs", number=1))
    constraint = builder.build()
    assert constraint.validate()
    assert constraint.report() == [("null values of legs lower than 1", 1, 0)]

    builder.add_constraint(null_values_below_number(column_name="weight", number=1))
    constraint = builder.build()
    assert not constraint.validate()
    assert constraint.report() == [
        ("null values of legs lower than 1", 1, 0),
        ("null values of weight lower than 1", 0, 1),
    ]


def test_null_percentage_below_number(builder):
    builder.add_constraint(null_percentage_below_number(column_name="weight", number=1.0))
    constraint = builder.build()
    assert constraint.validate()
    assert constraint.report() == [("null percentage of weight lower than 1.0", 1, 0)]

    builder.add_constraint(null_percentage_below_number(column_name="weight", number=0.1))
    constraint = builder.build()
    assert not constraint.validate()
    assert constraint.report() == [
        ("null percentage of weight lower than 1.0", 1, 0),
        ("null percentage of weight lower than 0.1", 0, 1),
    ]


def test_distinct_number_in_range(builder):
    builder.add_constraint(distinct_number_in_range(column_name="legs", lower=0.0, upper=4.0))
    constraint = builder.build()
    assert constraint.validate()
    assert constraint.report() == [("legs distinct values estimate between 0.0 and 4.0 (inclusive)", 1, 0)]

    builder.add_constraint(distinct_number_in_range(column_name="legs", lower=0.1, upper=2.0))
    constraint = builder.build()
    assert not constraint.validate()
    assert constraint.report() == [
        ("legs distinct values estimate between 0.0 and 4.0 (inclusive)", 1, 0),
        ("legs distinct values estimate between 0.1 and 2.0 (inclusive)", 0, 1),
    ]


def test_frequent_strings_in_reference_set(builder):
    ref_set = {"cat", "hawk", "snake"}

    builder.add_constraint(frequent_strings_in_reference_set(column_name="animal", reference_set=ref_set))
    constraint = builder.build()
    assert constraint.validate()
    assert constraint.report() == [(f"animal values in set {ref_set}", 1, 0)]

    other_set = {"elephant"}

    builder.add_constraint(frequent_strings_in_reference_set(column_name="animal", reference_set=other_set))
    constraint = builder.build()
    assert not constraint.validate()
    assert constraint.report() == [
        (f"animal values in set {ref_set}", 1, 0),
        (f"animal values in set {other_set}", 0, 1),
    ]


def test_n_most_common_items_in_set(builder):
    ref_set = {"cat"}

    builder.add_constraint(n_most_common_items_in_set(column_name="animal", n=1, reference_set=ref_set))
    constraint = builder.build()
    assert constraint.validate()
    assert constraint.report() == [(f"animal 1-most common items in set {ref_set}", 1, 0)]

    other_set = {"elephant"}

    builder.add_constraint(n_most_common_items_in_set(column_name="animal", n=1, reference_set=other_set))
    constraint = builder.build()
    assert not constraint.validate()
    assert constraint.report() == [
        (f"animal 1-most common items in set {ref_set}", 1, 0),
        (f"animal 1-most common items in set {other_set}", 0, 1),
    ]
