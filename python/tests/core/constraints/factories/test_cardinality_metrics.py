from whylogs.core.constraints.factories import distinct_number_in_range


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
