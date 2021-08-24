"""
Statistical functions used by whylogs
"""
CARDINALITY_SLOP = 1


def is_discrete(num_records: int, cardinality: int, p=0.15):
    """
    Estimate whether a feature is discrete given the number of records
    observed and the cardinality (number of unique values)

    The default assumption is that features are not discrete.

    Parameters
    ----------
    num_records : int
        The number of observed records
    cardinality : int
        Number of unique observed values

    Returns
    -------
    discrete : bool
        Whether the feature is discrete
    """
    if cardinality >= num_records:
        return False
    if num_records < 1:
        return False
    if cardinality < 1:
        raise ValueError("Cardinality must be >= 1 for num records >= 1")
    discrete = False
    density = num_records / (cardinality + CARDINALITY_SLOP)
    if 1 / density <= p:
        discrete = True

    return discrete
