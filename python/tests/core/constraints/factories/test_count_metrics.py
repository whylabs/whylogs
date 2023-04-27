from whylogs.core.constraints.factories import (
    count_below_number,
    no_missing_values,
    null_percentage_below_number,
    null_values_below_number,
)


def test_no_missing_values(builder):
    builder.add_constraint(no_missing_values(column_name="animal"))
    builder.add_constraint(no_missing_values(column_name="weight"))
    constraints = builder.build()
    assert constraints.validate() is False
    report = constraints.generate_constraints_report()
    assert report[0].name == "animal has no missing values" and report[0].passed == 1
    assert report[1].name == "weight has no missing values" and report[1].passed == 0


def test_count_below_number(builder, nan_builder):
    builder.add_constraint(count_below_number(column_name="weight", number=10))
    builder.add_constraint(count_below_number(column_name="weight", number=1))
    constraint = builder.build()
    assert not constraint.validate()
    # ReportResult(name, passed, failed, summary)
    assert constraint.generate_constraints_report() == [
        ("weight", "count of weight lower than 10", 1, 0, None),
        ("weight", "count of weight lower than 1", 0, 1, None),
    ]

    nan_builder.add_constraint(count_below_number(column_name="a", number=10))
    nan_builder.add_constraint(count_below_number(column_name="a", number=1))
    constraint = nan_builder.build()
    assert not constraint.validate()
    assert constraint.generate_constraints_report() == [
        ("a", "count of a lower than 10", 1, 0, None),
        ("a", "count of a lower than 1", 0, 1, None),
    ]
    for x, y in zip(constraint.report(), constraint.generate_constraints_report()):
        assert (x[0], x[1], x[2]) == (y[0], y[1], y[2])


def test_null_values_below_number(builder, nan_builder):
    builder.add_constraint(null_values_below_number(column_name="legs", number=1))
    builder.add_constraint(null_values_below_number(column_name="weight", number=1))
    constraint = builder.build()
    assert not constraint.validate()
    # ReportResult(name, passed, failed, summary)
    assert constraint.generate_constraints_report() == [
        ("legs", "null values of legs lower than 1", 1, 0, None),
        ("weight", "null values of weight lower than 1", 0, 1, None),
    ]
    for x, y in zip(constraint.report(), constraint.generate_constraints_report()):
        assert (x[0], x[1], x[2]) == (y[0], y[1], y[2])

    nan_builder.add_constraint(null_values_below_number(column_name="a", number=10))
    nan_builder.add_constraint(null_values_below_number(column_name="a", number=3))
    constraint = nan_builder.build()
    assert not constraint.validate()
    assert constraint.generate_constraints_report() == [
        ("a", "null values of a lower than 10", 1, 0, None),
        ("a", "null values of a lower than 3", 0, 1, None),
    ]
    for x, y in zip(constraint.report(), constraint.generate_constraints_report()):
        assert (x[0], x[1], x[2]) == (y[0], y[1], y[2])


def test_null_percentage_below_number(builder, nan_builder):
    builder.add_constraint(null_percentage_below_number(column_name="weight", number=1.0))
    builder.add_constraint(null_percentage_below_number(column_name="weight", number=0.1))
    constraint = builder.build()
    assert not constraint.validate()
    # ReportResult(name, passed, failed, summary)
    assert constraint.generate_constraints_report() == [
        ("weight", "null percentage of weight lower than 1.0", 1, 0, None),
        ("weight", "null percentage of weight lower than 0.1", 0, 1, None),
    ]
    for x, y in zip(constraint.report(), constraint.generate_constraints_report()):
        assert (x[0], x[1], x[2]) == (y[0], y[1], y[2])

    nan_builder.add_constraint(null_percentage_below_number(column_name="a", number=1.0))
    nan_builder.add_constraint(null_percentage_below_number(column_name="a", number=0.1))
    constraint = nan_builder.build()
    assert not constraint.validate()
    assert constraint.generate_constraints_report() == [
        ("a", "null percentage of a lower than 1.0", 1, 0, None),
        ("a", "null percentage of a lower than 0.1", 0, 1, None),
    ]
    for x, y in zip(constraint.report(), constraint.generate_constraints_report()):
        assert (x[0], x[1], x[2]) == (y[0], y[1], y[2])
