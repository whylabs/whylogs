"""Centralized module for core math logic.

These utility functions can be hard to reason about so it's important to keep them as
centralized as possible and close to the original pseudo code.
"""
from collections import namedtuple

VarianceM2Result = namedtuple("VarianceM2Result", "n mean m2")


def parallel_variance_m2(*, first: VarianceM2Result, second: VarianceM2Result) -> VarianceM2Result:
    """
    Verbatim copy of the parallel variance algorithm from Wikipedia.

    Source: https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
    Args:
        first: the :class:VarianceM2Result object for the first stream
        second: the :class:VarianceM2Result object for the second stream

    Returns:
        The variance result of the overall stream.

    """
    (n_a, avg_a, m2_a) = first
    (n_b, avg_b, m2_b) = second
    n = n_a + n_b
    delta = avg_b - avg_a
    mean = (avg_a * n_a + avg_b * n_b) / (n_a + n_b)
    m2 = m2_a + m2_b + delta**2 * n_a * n_b / n
    return VarianceM2Result(n=n, mean=mean, m2=m2)


def welford_online_variance_m2(*, existing: VarianceM2Result, new_value: float) -> VarianceM2Result:
    (n, mean, m2) = existing

    n += 1
    delta = new_value - mean
    mean += delta / n
    delta2 = new_value - mean
    m2 += delta * delta2
    return VarianceM2Result(n=n, mean=mean, m2=m2)
