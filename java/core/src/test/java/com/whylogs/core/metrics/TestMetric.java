package com.whylogs.core.metrics;

import java.util.ArrayList;
import org.junit.Assert;
import org.testng.annotations.Test;

@Test
public class TestMetric {

  @Test
  public void testMetrics() {
    ArrayList<Metric> metrics = new ArrayList<>();
    metrics.add(IntegralMetric.zero(new MetricConfig()));
    metrics.add(IntegralMetric.zero(new MetricConfig()));

    for (Metric metric : metrics) {
      metric.merge(new IntegralMetric());
      Assert.assertTrue(metric instanceof IntegralMetric);
    }

    Assert.assertEquals(metrics.size(), 2);
  }
}
