package com.whylogs.core.metrics;

import java.util.ArrayList;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TestStandardMetricFactory {

  @Test
  public void test_standardMetric() {
    IntegralMetric ints = StandardMetric.ints.zero(new MetricConfig());
    Assert.assertEquals((int) ints.getMaxComponent().getValue(), Integer.MIN_VALUE);

    ArrayList<Metric> list = new ArrayList<>();
    list.add(ints);
    list.add(StandardMetric.ints.zero(new MetricConfig()));
  }
}
