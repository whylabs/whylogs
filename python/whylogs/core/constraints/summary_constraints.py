from whylogs.core.constraints import SummaryConstraint
from whylogs.core.proto.v0 import Op

MAX_SET_DISPLAY_MESSAGE_LENGTH = 20


def maxLessThanEqualConstraint(
    value: float = None, field: str = None, name: str = None, verbose: bool = False
) -> SummaryConstraint:
    """
    Defines a summary constraint on the maximum value of a feature. The maximum can be defined to be
    less than or equal to some value,
    or less than or equal to the values of another summary field of the same feature, such as the mean (average).

    Parameters
    ----------
    value : numeric (one-of)
        Represents the value which should be compared to the maximum value of the specified feature,
        for checking the less than or equal to constraint.
        Only one of `value` and `field` should be supplied.
    field : str (one-of)
        The field is a string representing a summary field
        e.g. `min`, `mean`, `max`, `stddev`, etc., for which the value will be used for
        checking the less than or equal to constraint.
        Only one of `field` and `value` should be supplied.
    name : str
        Name of the constraint used for reporting
    verbose : bool
        If true, log every application of this constraint that fails.
        Useful to identify specific streaming values that fail the constraint.

    Returns
    -------
        SummaryConstraint -  a summary constraint defining a constraint on the maximum value to be less than
        or equal to some value / summary field

    """
    if name is None:
        name = f"maximum is less than or equal to {value}"

    return SummaryConstraint("max", Op.LE, value=value, second_field=field, name=name, verbose=verbose)
