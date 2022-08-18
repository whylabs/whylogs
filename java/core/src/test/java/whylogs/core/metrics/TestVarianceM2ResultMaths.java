package whylogs.core.metrics;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TestVarianceM2ResultMaths {

  @Test
  public void test_parallel_variance_m2() {
    double n_a = 1;
    double mean_a = 2.4;
    double m2_a = 2;

    double n_b = 2;
    double mean_b = 3.4;
    double m2_b = 4;

    VarianceM2Result first = new VarianceM2Result(n_a, mean_a, m2_a);
    VarianceM2Result second = new VarianceM2Result(n_b, mean_b, m2_b);

    VarianceM2Result result = VarianceM2Result.parallelVarianceM2(first, second);

    Assert.assertEquals(result.getN(), n_a + n_b);
    Assert.assertEquals(result.getMean(), (mean_a * n_a + mean_b * n_b) / (n_a + n_b));
    Assert.assertEquals(
        result.getM2(), m2_a + m2_b + Math.pow((mean_a - mean_b), 2) * n_a * n_b / (n_a + n_b));
  }

  @Test
  public void test_welford_online_variance_m2() {
    double delta, delta2;
    double n = 1;
    double mean = 2.4;
    double m2 = 2;
    VarianceM2Result existing_aggregate = new VarianceM2Result(n, mean, m2);

    double new_value = 2;
    VarianceM2Result actual_result =
        VarianceM2Result.welfordOnlineVarianceM2(existing_aggregate, new_value);

    n++;
    delta = new_value - mean;
    mean += delta / n;
    delta2 = new_value - mean;
    m2 += delta * delta2;

    Assert.assertEquals(actual_result.getN(), n);
    Assert.assertEquals(actual_result.getMean(), mean);
    Assert.assertEquals(actual_result.getM2(), m2);
  }
}
