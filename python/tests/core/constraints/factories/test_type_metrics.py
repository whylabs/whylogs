from whylogs.core.constraints.factories import (
    column_is_nullable_boolean,
    column_is_nullable_fractional,
    column_is_nullable_integral,
    column_is_nullable_object,
    column_is_nullable_string,
)


def test_is_nullable_type(builder):
    builder.add_constraint(column_is_nullable_string(column_name="animal"))
    builder.add_constraint(column_is_nullable_integral(column_name="legs"))
    builder.add_constraint(column_is_nullable_fractional(column_name="weight"))
    builder.add_constraint(column_is_nullable_boolean(column_name="weight"))
    builder.add_constraint(column_is_nullable_object(column_name="weight"))

    constraint = builder.build()
    assert not constraint.validate()
    # ReportResult(name, passed, failed, summary)
    assert constraint.generate_constraints_report() == [
        ("animal", "animal is nullable string", 1, 0, None),
        ("legs", "legs is nullable integral", 1, 0, None),
        ("weight", "weight is nullable fractional", 1, 0, None),
        ("weight", "weight is nullable boolean", 0, 1, None),
        ("weight", "weight is nullable object", 0, 1, None),
    ]
    for x, y in zip(constraint.report(), constraint.generate_constraints_report()):
        assert (x[0], x[1], x[2]) == (y[0], y[1], y[2])
