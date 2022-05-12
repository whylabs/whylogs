from whylogs.core import DatasetProfileView
from whylogs.core.constraints import DatasetConstraints


def test_max_less_than_equal_constraint(
    profile_view: DatasetProfileView, max_less_than_equal_constraints: DatasetConstraints
) -> None:
    constraints_report = max_less_than_equal_constraints(profile_view=profile_view)
    assert len(constraints_report) == 1
    assert len(constraints_report[0][1]) == 1
    assert "max" in constraints_report[0][1][0][0]
    assert constraints_report[0][1][0][1] == 1
    assert constraints_report[0][1][0][2] == 0
