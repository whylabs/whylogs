package com.whylogs.core.resolvers;

import com.whylogs.core.metrics.IntegralMetric;
import com.whylogs.core.metrics.Metric;
import com.whylogs.core.metrics.MetricConfig;
import com.whylogs.core.schemas.ColumnSchema;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;

@Test
public class TestStandardResolver {

    @Test
    public void test_integralInput() {
        StandardResolver resolver = new StandardResolver();
        ColumnSchema columnSchema = new ColumnSchema(Integer.class, new MetricConfig(), resolver);
        HashMap<String, ? extends Metric> metrics = resolver.resolve(columnSchema);
        Assert.assertEquals(metrics.get("ints").getClass(), IntegralMetric.class);
    }

    // TODO: add tests when other metrics get added
}
