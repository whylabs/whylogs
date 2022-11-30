package com.whylogs.core.resolvers;

import com.whylogs.core.metrics.IntegralMetric;
import com.whylogs.core.metrics.Metric;
import com.whylogs.core.metrics.MetricConfig;
import com.whylogs.core.schemas.ColumnSchema;
import java.util.HashMap;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TestStandardResolver {

  @Test
  public void test_integralInput() {
    StandardResolver resolver = new StandardResolver();
    ColumnSchema columnSchema = new ColumnSchema(Integer.class, new MetricConfig(), resolver);
    HashMap<String, Metric<?>> metrics = resolver.resolve(columnSchema);
    Assert.assertEquals(metrics.get("ints").getClass(), IntegralMetric.class);
  }

  // TODO: add tests when other metrics get added
}
