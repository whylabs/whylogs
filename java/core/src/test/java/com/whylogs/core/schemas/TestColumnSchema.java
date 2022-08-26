package com.whylogs.core.schemas;

import com.whylogs.core.DataTypes;
import com.whylogs.core.metrics.IntegralMetric;
import com.whylogs.core.metrics.Metric;
import com.whylogs.core.metrics.MetricConfig;
import com.whylogs.core.metrics.StandardMetric;
import com.whylogs.core.resolvers.StandardResolver;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;

@Test
public class TestColumnSchema {

    @Test
    public void test_column_schema() {
        ColumnSchema columnSchema = new ColumnSchema(Integer.class, new MetricConfig(),  new StandardResolver());
        HashMap<String, ? extends Metric>  metrics = columnSchema.getMetrics();

        // TODO: I'm not sure I like this. Might want to rethink the Metric just a little
        Assert.assertEquals(metrics.get("ints").getClass(), IntegralMetric.class);
        IntegralMetric ints = (IntegralMetric) metrics.get("ints");
        Assert.assertEquals((int) ints.getMaxComponent().getValue(), Integer.MIN_VALUE);
    }
}
