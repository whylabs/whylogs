import pandas as pd

import whylogs as why
from whylogs.core.constraints import ConstraintsBuilder
from whylogs.core.constraints.factories import (
    frequent_strings_in_reference_set,
    is_unique,
    n_most_common_items_in_set,
)


def test_frequent_strings_in_reference_set(builder):
    ref_set = {"cat", "hawk", "snake"}

    builder.add_constraint(frequent_strings_in_reference_set(column_name="animal", reference_set=ref_set))
    constraint = builder.build()
    assert constraint.validate()
    # ReportResult(name, passed, failed, summary)
    assert constraint.generate_constraints_report() == [(f"animal values in set {ref_set}", 1, 0, None)]

    other_set = {"elephant"}

    builder.add_constraint(frequent_strings_in_reference_set(column_name="animal", reference_set=other_set))
    constraint = builder.build()
    assert not constraint.validate()
    # ReportResult(name, passed, failed, summary)
    assert constraint.generate_constraints_report() == [
        (f"animal values in set {ref_set}", 1, 0, None),
        (f"animal values in set {other_set}", 0, 1, None),
    ]
    for (x, y) in zip(constraint.report(), constraint.generate_constraints_report()):
        assert (x[0], x[1], x[2]) == (y[0], y[1], y[2])


def test_n_most_common_items_in_set(builder):
    ref_set = {"cat"}

    builder.add_constraint(n_most_common_items_in_set(column_name="animal", n=1, reference_set=ref_set))
    constraint = builder.build()
    assert constraint.validate()
    # ReportResult(name, passed, failed, summary)
    assert constraint.generate_constraints_report() == [(f"animal 1-most common items in set {ref_set}", 1, 0, None)]

    other_set = {"elephant"}

    builder.add_constraint(n_most_common_items_in_set(column_name="animal", n=1, reference_set=other_set))
    constraint = builder.build()
    assert not constraint.validate()
    assert constraint.generate_constraints_report() == [
        (f"animal 1-most common items in set {ref_set}", 1, 0, None),
        (f"animal 1-most common items in set {other_set}", 0, 1, None),
    ]
    for (x, y) in zip(constraint.report(), constraint.generate_constraints_report()):
        assert (x[0], x[1], x[2]) == (y[0], y[1], y[2])


def test_is_unique():
    data = {
        "animal": ["cat", "hawk", "snake", "cat", "mosquito"],
        "legs": [0, 1, 2, 3, 4],
        "weight": [4.3, -1.8, 1.3, 4.1, 5.5e-6],
    }

    view = why.log(pd.DataFrame(data)).profile().view()
    builder = ConstraintsBuilder(dataset_profile_view=view)

    builder.add_constraint(is_unique(column_name="animal"))
    builder.add_constraint(is_unique(column_name="legs"))
    constraint = builder.build()
    report = constraint.generate_constraints_report()
    assert report[0].name == "animal is unique" and report[0].passed == 0 and report[0].failed == 1
    assert report[1].name == "legs is unique" and report[1].passed == 1 and report[1].failed == 0
