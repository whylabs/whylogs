package com.whylogs.core.metrics;

import com.whylogs.core.metrics.components.MaxIntegralComponent;
import org.junit.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;

@Test
public class TestMetric {

    @Test
    public void testMetrics(){
        ArrayList<? extends AbstractMetric> metrics = new ArrayList<>();
        metrics.add(IntegralMetric.zero(new MetricConfig()));
        metrics.add(IntegralMetric.zero(new MetricConfig()));

        for(Metric metric : metrics){
            Assert.assertTrue(metric instanceof IntegralMetric);
            metric.merge(new IntegralMetric());

            ((IntegralMetric) metric).getMaxComponent()
        }
    }
}
