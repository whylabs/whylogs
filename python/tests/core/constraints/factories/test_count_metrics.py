from whylogs.core.constraints.factories import (
    count_below_number,
    no_missing_values,
    null_percentage_below_number,
    null_values_below_number,
    is_probably_unique,
)
import pandas as pd
import whylogs as why
from whylogs.core.constraints import ConstraintsBuilder


def test_no_missing_values(builder):
    builder.add_constraint(no_missing_values(column_name="animal"))
    builder.add_constraint(no_missing_values(column_name="weight"))
    constraint = builder.build()
    assert constraint.validate() is False
    report = constraint.generate_constraints_report()
    assert report[0].name == "animal has no missing values" and report[0].passed == 1
    assert report[1].name == "weight has no missing values" and report[1].passed == 0


def test_count_below_number(builder, nan_builder):
    builder.add_constraint(count_below_number(column_name="weight", number=10))
    builder.add_constraint(count_below_number(column_name="weight", number=1))
    constraint = builder.build()
    assert not constraint.validate()
    # ReportResult(name, passed, failed, summary)
    assert constraint.generate_constraints_report() == [
        ("count of weight lower than 10", 1, 0, None),
        ("count of weight lower than 1", 0, 1, None),
    ]

    nan_builder.add_constraint(count_below_number(column_name="a", number=10))
    nan_builder.add_constraint(count_below_number(column_name="a", number=1))
    constraint = nan_builder.build()
    assert not constraint.validate()
    assert constraint.generate_constraints_report() == [
        ("count of a lower than 10", 1, 0, None),
        ("count of a lower than 1", 0, 1, None),
    ]
    for (x, y) in zip(constraint.report(), constraint.generate_constraints_report()):
        assert (x[0], x[1], x[2]) == (y[0], y[1], y[2])


def test_null_values_below_number(builder, nan_builder):
    builder.add_constraint(null_values_below_number(column_name="legs", number=1))
    builder.add_constraint(null_values_below_number(column_name="weight", number=1))
    constraint = builder.build()
    assert not constraint.validate()
    # ReportResult(name, passed, failed, summary)
    assert constraint.generate_constraints_report() == [
        ("null values of legs lower than 1", 1, 0, None),
        ("null values of weight lower than 1", 0, 1, None),
    ]
    for (x, y) in zip(constraint.report(), constraint.generate_constraints_report()):
        assert (x[0], x[1], x[2]) == (y[0], y[1], y[2])

    nan_builder.add_constraint(null_values_below_number(column_name="a", number=10))
    nan_builder.add_constraint(null_values_below_number(column_name="a", number=3))
    constraint = nan_builder.build()
    assert not constraint.validate()
    assert constraint.generate_constraints_report() == [
        ("null values of a lower than 10", 1, 0, None),
        ("null values of a lower than 3", 0, 1, None),
    ]
    for (x, y) in zip(constraint.report(), constraint.generate_constraints_report()):
        assert (x[0], x[1], x[2]) == (y[0], y[1], y[2])


def test_null_percentage_below_number(builder, nan_builder):
    builder.add_constraint(null_percentage_below_number(column_name="weight", number=1.0))
    builder.add_constraint(null_percentage_below_number(column_name="weight", number=0.1))
    constraint = builder.build()
    assert not constraint.validate()
    # ReportResult(name, passed, failed, summary)
    assert constraint.generate_constraints_report() == [
        ("null percentage of weight lower than 1.0", 1, 0, None),
        ("null percentage of weight lower than 0.1", 0, 1, None),
    ]
    for (x, y) in zip(constraint.report(), constraint.generate_constraints_report()):
        assert (x[0], x[1], x[2]) == (y[0], y[1], y[2])

    nan_builder.add_constraint(null_percentage_below_number(column_name="a", number=1.0))
    nan_builder.add_constraint(null_percentage_below_number(column_name="a", number=0.1))
    constraint = nan_builder.build()
    assert not constraint.validate()
    assert constraint.generate_constraints_report() == [
        ("null percentage of a lower than 1.0", 1, 0, None),
        ("null percentage of a lower than 0.1", 0, 1, None),
    ]
    for (x, y) in zip(constraint.report(), constraint.generate_constraints_report()):
        assert (x[0], x[1], x[2]) == (y[0], y[1], y[2])


def test_is_probably_unique():
    data = {
        "animal": ["cat", "hawk", "snake", "cat"],
        "animal_null": ["cat", None, "snake", "cat"],
        "legs": [4, 2, 0, 1],
        "legs_null": [4, 2, None, 1],
        "weight": [4.1, 1.8, None, 4.1],
    }

    df = pd.DataFrame(data)

    prof_view = why.log(df).profile().view()
    constraints_builder = ConstraintsBuilder(dataset_profile_view=prof_view)

    print(prof_view.to_pandas())

    constraints_builder.add_constraint(
        is_probably_unique("animal", profile_view=constraints_builder._dataset_profile_view)
    )
    constraints_builder.add_constraint(
        is_probably_unique("animal_null", profile_view=constraints_builder._dataset_profile_view)
    )
    constraints_builder.add_constraint(
        is_probably_unique("legs", profile_view=constraints_builder._dataset_profile_view)
    )
    constraints_builder.add_constraint(
        is_probably_unique("legs_null", profile_view=constraints_builder._dataset_profile_view)
    )
    constraints_builder.add_constraint(
        is_probably_unique("weight", profile_view=constraints_builder._dataset_profile_view)
    )

    constraints = constraints_builder.build()
    report = constraints.generate_constraints_report()
    assert report[0].name == "animal is probably unique" and report[0].passed == 0
    assert report[1].name == "animal_null is probably unique" and report[1].passed == 0
    assert report[2].name == "legs is probably unique" and report[2].passed == 1
    assert report[3].name == "legs_null is probably unique" and report[3].passed == 1
    assert report[4].name == "weight is probably unique" and report[4].passed == 0
