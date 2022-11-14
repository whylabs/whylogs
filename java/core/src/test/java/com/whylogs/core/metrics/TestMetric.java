package com.whylogs.core.metrics;

import java.util.ArrayList;

import com.whylogs.core.message.MetricComponentMessage;
import com.whylogs.core.message.MetricMessage;
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

  @Test
  public void testRoundTripProtobuf(){
    IntegralMetric metric = IntegralMetric.zero(new MetricConfig());
    Metric<?> metric2 = Metric.fromProtobuf(metric.toProtobuf(), metric.getNamespace());
    Assert.assertTrue(metric2 instanceof IntegralMetric);
    Assert.assertEquals(metric2.getNamespace(), "ints");
    Assert.assertEquals(metric2.getComponents().size(), 2);
    Assert.assertEquals(metric2.getComponents().get("MinIntegralComponent"), metric.getComponents().get("MinIntegralComponent"));
    Assert.assertEquals(metric2.getComponents().get("MaxIntegralComponent"), metric.getComponents().get("MaxIntegralComponent"));
  }

  @Test
  public void testToProtobuf(){
    IntegralMetric metric = IntegralMetric.zero(new MetricConfig());
    MetricMessage message = metric.toProtobuf();

    Assert.assertEquals(message.getMetricComponentsCount(), 2);
    Assert.assertEquals(message.getMetricComponentsMap().get("MinIntegralComponent").getN(), Integer.MAX_VALUE);
    Assert.assertEquals(message.getMetricComponentsMap().get("MaxIntegralComponent").getN(), Integer.MIN_VALUE);
  }
}
