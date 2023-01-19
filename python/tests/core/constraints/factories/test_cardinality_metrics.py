from whylogs.core.constraints.factories import distinct_number_in_range


def test_distinct_number_in_range(builder):
    builder.add_constraint(distinct_number_in_range(column_name="legs", lower=0.0, upper=4.0))
    builder.add_constraint(distinct_number_in_range(column_name="legs", lower=0.1, upper=2.0))
    constraint = builder.build()
    assert not constraint.validate()
    assert constraint.generate_constraints_report() == [
        # ReportResult(name, passed, failed, summary)
        ("legs distinct values estimate between 0.0 and 4.0 (inclusive)", 1, 0, None),
        ("legs distinct values estimate between 0.1 and 2.0 (inclusive)", 0, 1, None),
    ]
    for x, y in zip(constraint.report(), constraint.generate_constraints_report()):
        assert (x[0], x[1], x[2]) == (y[0], y[1], y[2])


def test_distinct_number_in_range_with_nans(nan_builder):
    nan_builder.add_constraint(distinct_number_in_range(column_name="a", lower=0, upper=5.0))
    nan_builder.add_constraint(distinct_number_in_range(column_name="a", lower=2.2, upper=5.0))
    constraint = nan_builder.build()
    assert not constraint.validate()
    assert constraint.generate_constraints_report() == [
        # ReportResult(name, passed, failed, summary)
        ("a distinct values estimate between 0 and 5.0 (inclusive)", 1, 0, None),
        ("a distinct values estimate between 2.2 and 5.0 (inclusive)", 0, 1, None),
    ]
    for x, y in zip(constraint.report(), constraint.generate_constraints_report()):
        assert (x[0], x[1], x[2]) == (y[0], y[1], y[2])
