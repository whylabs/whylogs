from whylogs.core.constraints.factories import (
    count_below_number,
    null_percentage_below_number,
    null_values_below_number,
)


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
