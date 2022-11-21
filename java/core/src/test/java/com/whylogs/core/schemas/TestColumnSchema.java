package com.whylogs.core.schemas;

import com.whylogs.core.metrics.IntegralMetric;
import com.whylogs.core.metrics.Metric;
import com.whylogs.core.metrics.MetricConfig;
import com.whylogs.core.resolvers.StandardResolver;
import java.util.HashMap;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TestColumnSchema {

  @Test
  public void test_column_schema() {
    ColumnSchema columnSchema =
        new ColumnSchema(Integer.class, new MetricConfig(), new StandardResolver());
    HashMap<String, ? extends Metric> metrics = columnSchema.getMetrics();

    Assert.assertEquals(metrics.get("ints").getClass(), IntegralMetric.class);
    Assert.assertEquals(columnSchema.getType(), Integer.class);
    IntegralMetric ints = (IntegralMetric) metrics.get("ints");
    Assert.assertEquals((int) ints.getMaxComponent().getValue(), Integer.MIN_VALUE);
  }
}
