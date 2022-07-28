from whylogs.core.constraints.factories import distinct_number_in_range


def test_distinct_number_in_range(builder):
    builder.add_constraint(distinct_number_in_range(column_name="legs", lower=0.0, upper=4.0))
    builder.add_constraint(distinct_number_in_range(column_name="legs", lower=0.1, upper=2.0))
    constraint = builder.build()
    assert not constraint.validate()
    assert constraint.report() == [
        ("legs distinct values estimate between 0.0 and 4.0 (inclusive)", 1, 0),
        ("legs distinct values estimate between 0.1 and 2.0 (inclusive)", 0, 1),
    ]


def test_distinct_number_in_range_with_nans(nan_builder):
    nan_builder.add_constraint(distinct_number_in_range(column_name="a", lower=0, upper=5.0))
    nan_builder.add_constraint(distinct_number_in_range(column_name="a", lower=2.2, upper=5.0))
    constraint = nan_builder.build()
    assert not constraint.validate()
    assert constraint.report() == [
        ("a distinct values estimate between 0 and 5.0 (inclusive)", 1, 0),
        ("a distinct values estimate between 2.2 and 5.0 (inclusive)", 0, 1),
    ]
