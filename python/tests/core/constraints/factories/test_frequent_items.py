from whylogs.core.constraints.factories import (
    frequent_strings_in_reference_set,
    n_most_common_items_in_set,
)


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
