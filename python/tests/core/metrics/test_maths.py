from whylogs.core.metrics.maths import (
    VarianceM2Result,
    parallel_variance_m2,
    welford_online_variance_m2,
)


def test_parallel_variance_m2() -> None:
    n_a = 1
    mean_a = 2.4
    m2_a = 2

    n_b = 2
    mean_b = 3.4
    m2_b = 4

    first = VarianceM2Result(n_a, mean_a, m2_a)
    second = VarianceM2Result(n_b, mean_b, m2_b)

    actual_result = parallel_variance_m2(first=first, second=second)

    assert isinstance(actual_result, VarianceM2Result)
    assert actual_result.mean == (mean_a * n_a + mean_b * n_b) / (n_a + n_b)
    assert actual_result.n == n_a + n_b
    assert actual_result.m2 == m2_a + m2_b + (mean_a - mean_b) ** 2 * n_a * n_b / (n_a + n_b)


def test_welford_online_variance_m2() -> None:
    n = 1
    mean = 2.4
    m2 = 2
    existing_aggregate = VarianceM2Result(n, mean, m2)
    new_value = 2

    actual_result = welford_online_variance_m2(existing=existing_aggregate, new_value=2)

    n += 1
    delta = new_value - mean
    mean += delta / n
    delta2 = new_value - mean
    m2 += delta * delta2  # type: ignore

    assert isinstance(actual_result, VarianceM2Result)
    assert actual_result.mean == mean
    assert actual_result.n == n
    assert actual_result.m2 == m2
