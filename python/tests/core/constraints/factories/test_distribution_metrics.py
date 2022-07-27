from whylogs.core.constraints.factories.distribution_metrics import (
    greater_than_number,
    mean_between_range,
    quantile_between_range,
    smaller_than_number,
    stddev_between_range,
)


def test_greater_than_number(builder):
    builder.add_constraint(greater_than_number(column_name="weight", number=0.0))
    constraint = builder.build()
    assert constraint.validate()
    assert constraint.report() == [("weight greater than number 0.0", 1, 0)]

    builder.add_constraint(greater_than_number(column_name="legs", number=6))
    constraint = builder.build()
    assert not constraint.validate()
    assert constraint.report() == [("weight greater than number 0.0", 1, 0), ("legs greater than number 6", 0, 1)]


def test_greater_than_number_with_nan(nan_builder):
    nan_builder.add_constraint(greater_than_number(column_name="a", number=2.2))
    constraint = nan_builder.build()

    assert constraint.validate() is False


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
